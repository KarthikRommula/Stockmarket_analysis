import os
import json
import time
import fitz  
import boto3
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Configure AWS Bedrock client
bedrock_runtime = boto3.client(
    service_name='bedrock-runtime',
    region_name=os.getenv('AWS_REGION'),
    aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
    aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY')
)

def extract_pdf(pdf_path):
    """
    Extract text from PDF with page numbers.
    
    Args:
        pdf_path (str): Path to the PDF file
        
    Returns:
        list: List of dictionaries containing text and page number
    """
    import fitz  # PyMuPDF
    
    extracted_pages = []
    
    try:
        # Open the PDF document
        doc = fitz.open(pdf_path)
        
        # Process each page
        for page_idx in range(len(doc)):
            page = doc[page_idx]
            text = page.get_text()
            
            # Store page content with page number (1-based indexing)
            extracted_pages.append({
                "text": text,
                "page_number": page_idx + 1  # 1-based page numbering
            })
        
        doc.close()
        print(f"Extracted {len(extracted_pages)} pages from {pdf_path}")
        
    except Exception as e:
        print(f"Error extracting PDF {pdf_path}: {e}")
    
    return extracted_pages


def chunk_text(extracted_pages, chunk_size=1000, chunk_overlap=200):
    """
    Break extracted text into chunks with proper page metadata.
    
    Args:
        extracted_pages (list): List of dictionaries with text and page_number
        chunk_size (int): Target size for each chunk
        chunk_overlap (int): Overlap between chunks
        
    Returns:
        list: List of chunk dictionaries with text and metadata
    """
    chunks = []
    
    for i, page_data in enumerate(extracted_pages):
        text = page_data.get("text", "")
        page_num = page_data.get("page_number", i + 1)
        
        # Skip empty pages
        if not text.strip():
            continue
            
        # If text is shorter than chunk_size, keep it as one chunk
        if len(text) <= chunk_size:
            chunks.append({
                "text": text,
                "metadata": {
                    "pages": [page_num],
                    "source_page": page_num
                }
            })
            continue
            
        # Split longer text into chunks
        start = 0
        while start < len(text):
            # Get chunk_size characters, try to break at paragraph or sentence boundary
            end = min(start + chunk_size, len(text))
            
            # Try to find a paragraph break within the last 20% of the chunk
            search_start = end - int(chunk_size * 0.2)
            paragraph_break = text.rfind("\n\n", search_start, end)
            
            if paragraph_break != -1 and paragraph_break > start:
                end = paragraph_break + 2  # Include the paragraph break
            else:
                # Try to find sentence break
                sentence_break = -1
                for sep in [". ", "! ", "? ", ".\n", "!\n", "?\n"]:
                    pos = text.rfind(sep, search_start, end)
                    if pos != -1 and (sentence_break == -1 or pos > sentence_break):
                        sentence_break = pos + len(sep)
                
                if sentence_break != -1 and sentence_break > start:
                    end = sentence_break
            
            # Create chunk with metadata
            chunk_text = text[start:end].strip()
            if chunk_text:  # Skip empty chunks
                chunks.append({
                    "text": chunk_text,
                    "metadata": {
                        "pages": [page_num],
                        "source_page": page_num
                    }
                })
            
            # Move to next chunk with overlap
            start = max(start + 1, end - chunk_overlap)
    
    print(f"Created {len(chunks)} chunks from {len(extracted_pages)} pages")
    
    # Debug output
    for i, chunk in enumerate(chunks[:3]):
        print(f"Chunk {i} pages: {chunk['metadata']['pages']}")
    
    return chunks

def summarize_with_claude(chunk_text, max_retries=5):
    """Summarize text using Claude with retry mechanism"""
    prompt = f"""Below is a section from a stock report. Please provide a concise summary that captures the key financial information, metrics, and insights:

{chunk_text}

Summary:"""

    for attempt in range(max_retries):
        try:
            response = bedrock_runtime.invoke_model(
                modelId='anthropic.claude-3-sonnet-20240229-v1:0',
                body=json.dumps({
                    "anthropic_version": "bedrock-2023-05-31",
                    "max_tokens": 500,
                    "messages": [
                        {
                            "role": "user",
                            "content": prompt
                        }
                    ]
                })
            )
            
            response_body = json.loads(response['body'].read())
            summary = response_body['content'][0]['text']
            
            return summary
        
        except Exception as e:
            if "ThrottlingException" in str(e) and attempt < max_retries - 1:
                wait_time = 2 ** (attempt + 2)  # Exponential backoff starting with 4 seconds
                print(f"Rate limited. Waiting {wait_time} seconds before retry (attempt {attempt+1}/{max_retries})...")
                time.sleep(wait_time)
            else:
                if attempt == max_retries - 1:
                    print(f"Failed after {max_retries} attempts. Error: {e}")
                raise

def save_partial_results(data, output_filename):
    """Save partial results to avoid losing progress"""
    with open(output_filename, 'w') as f:
        json.dump(data, f, indent=2)
    print(f"Saved partial results to {output_filename}")

def process_stock_report(pdf_path, output_dir="extracted_data"):
    """Process a stock report PDF and save extracted data"""
    # Create output directory if it doesn't exist
    os.makedirs(output_dir, exist_ok=True)
    
    # Extract text with layout information
    print(f"Extracting text from {pdf_path}...")
    extracted_pages = extract_pdf(pdf_path)
    
    # Chunk the extracted text
    print("Chunking text...")
    text_chunks = chunk_text(extracted_pages)
    
    # Prepare output file name
    output_filename = os.path.join(output_dir, os.path.basename(pdf_path).replace('.pdf', '.json'))
    result_data = {
        "source": pdf_path,
        "chunks": text_chunks
    }
    
    # Process each chunk and summarize
    print("Summarizing chunks with Claude...")
    successful_chunks = 0
    for i, chunk in enumerate(text_chunks):
        text_content = chunk["text"]
        
        try:
            # Summarize with Claude
            summary = summarize_with_claude(text_content)
            chunk["summary"] = summary
            successful_chunks += 1
            print(f"Processed chunk {i+1}/{len(text_chunks)}")
            
            # Save partial results after each successful chunk processing
            save_partial_results(result_data, output_filename)
            
        except Exception as e:
            print(f"Error processing chunk {i+1}: {e}")
            # Mark the chunk as failed but continue processing
            chunk["summary"] = f"Error: {str(e)}"
        
        # Add delay between requests to avoid throttling
        if i < len(text_chunks) - 1:  # Don't delay after the last chunk
            delay = 5  # 5 seconds between requests
            print(f"Waiting {delay} seconds before processing next chunk...")
            time.sleep(delay)
    
    # Verify final output
    with open(output_filename, 'r') as f:
        data = json.load(f)
        print("\nVerifying output data structure:")
        print(f"Total chunks: {len(data.get('chunks', []))}")
        for i, chunk in enumerate(data.get('chunks', [])[:3]):  # Show first 3 chunks
            pages = chunk.get('metadata', {}).get('pages', [])
            print(f"Chunk {i} pages: {pages}")
    
    print(f"Completed processing. {successful_chunks} of {len(text_chunks)} chunks processed successfully.")
    print(f"Results saved to {output_filename}")
    return output_filename

if __name__ == "__main__":
    import sys
    if len(sys.argv) < 2:
        print("Usage: python extract_pdf.py <path_to_pdf>")
        sys.exit(1)
    
    pdf_path = sys.argv[1]
    process_stock_report(pdf_path)