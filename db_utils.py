# db_utils.py
import lancedb
import os

def get_db_stats(db_path="stockreports_db"):
    """Get statistics on the LanceDB database"""
    db = lancedb.connect(db_path)
    
    if "stock_chunks" not in db.table_names():
        print("Stock chunks table does not exist")
        return
    
    table = db.open_table("stock_chunks")
    
    # Get row count
    row_count = len(table)
    
    # Get unique source files
    df = table.to_pandas()
    unique_sources = df['source_file'].unique()
    
    print(f"Database Statistics:")
    print(f"- Total chunks: {row_count}")
    print(f"- Unique source files: {len(unique_sources)}")
    print("\nSource files:")
    for source in unique_sources:
        source_chunks = df[df['source_file'] == source]
        print(f"- {source}: {len(source_chunks)} chunks")

def clear_database(db_path="stockreports_db", confirm=True):
    """Clear the database (for testing/reset)"""
    if confirm:
        confirmation = input("Are you sure you want to clear the database? (y/n): ")
        if confirmation.lower() != 'y':
            print("Operation cancelled")
            return
    
    db = lancedb.connect(db_path)
    
    if "stock_chunks" in db.table_names():
        db.drop_table("stock_chunks")
        print("Database cleared")
    else:
        print("No database to clear")

if __name__ == "__main__":
    import sys
    if len(sys.argv) < 2:
        print("Usage: python db_utils.py [stats|clear]")
        sys.exit(1)
    
    action = sys.argv[1].lower()
    
    if action == "stats":
        get_db_stats()
    elif action == "clear":
        clear_database()
    else:
        print(f"Unknown action: {action}")
        print("Available actions: stats, clear")