import os
import time
import argparse
import multiprocessing
import tempfile
import glob
import shutil
from functools import partial
from typing import List, Tuple
import duckdb
from tqdm import tqdm

def parse_args():
    parser = argparse.ArgumentParser(description='Ultra-fast loading of large domain dataset')
    parser.add_argument('--file', type=str, required=True, help='Path to the data file')
    parser.add_argument('--db-path', type=str, default='domains.duckdb', help='Path to DuckDB file')
    parser.add_argument('--workers', type=int, default=4, 
                        help='Number of worker processes (default: 4)')
    parser.add_argument('--chunk-size', type=int, default=250000, 
                        help='Rows per chunk for multiprocess loading')
    parser.add_argument('--direct', action='store_true', 
                        help='Use direct COPY statement instead of chunking')
    parser.add_argument('--memory-limit', type=str, default='8GB', 
                        help='Memory limit for DuckDB')
    parser.add_argument('--compression', choices=['none', 'uncompressed', 'gzip', 'zstd'], 
                        default='none', help='Input file compression')
    parser.add_argument('--temp-dir', type=str, default='./temp_dbs', 
                        help='Directory for temporary databases')
    return parser.parse_args()

def setup_database(db_path: str, memory_limit: str) -> duckdb.DuckDBPyConnection:
    """Initialize the database with optimal settings."""
    conn = duckdb.connect(db_path)
    
    # Configure DuckDB for performance
    conn.execute(f"PRAGMA memory_limit='{memory_limit}'")
    
    # Set threads to CPU count
    thread_count = max(1, multiprocessing.cpu_count() // 2)  # Use half the cores for each connection
    conn.execute(f"PRAGMA threads={thread_count}")
    
    # Enable optimizations if available
    try:
        conn.execute("PRAGMA enable_object_cache")
    except:
        pass
        
    # Create the table with optimized schema
    conn.execute("""
    CREATE TABLE IF NOT EXISTS domains (
        domain VARCHAR,
        nameservers VARCHAR,
        ip VARCHAR,
        country VARCHAR,
        server VARCHAR,
        field5 VARCHAR,
        field6 VARCHAR,
        field7 VARCHAR,
        field8 VARCHAR
    );
    """)
    return conn

def get_file_chunks(file_path: str, total_rows: int, chunk_size: int) -> List[Tuple[int, int]]:
    """Split the file into processing chunks based on byte positions."""
    file_size = os.path.getsize(file_path)
    approx_bytes_per_row = file_size / total_rows
    chunk_bytes = int(approx_bytes_per_row * chunk_size)
    
    chunks = []
    with open(file_path, 'rb') as f:
        start_pos = 0
        while start_pos < file_size:
            end_pos = min(start_pos + chunk_bytes, file_size)
            
            # Move to end of line
            if end_pos < file_size:
                f.seek(end_pos)
                while end_pos < file_size:
                    char = f.read(1)
                    end_pos += 1
                    if char == b'\n':
                        break
            
            chunks.append((start_pos, end_pos))
            start_pos = end_pos
    
    return chunks

def process_chunk(chunk_info: Tuple[int, Tuple[int, int]], file_path: str, temp_dir: str,
                  memory_limit: str, compression: str) -> Tuple[int, str]:
    """Process a specific chunk of the file into a temporary database."""
    chunk_id, (start_pos, end_pos) = chunk_info
    
    # Create a unique temp database for this chunk
    temp_db_path = os.path.join(temp_dir, f"chunk_{chunk_id}.duckdb")
    
    # Connect to temporary database
    conn = duckdb.connect(temp_db_path)
    conn.execute(f"PRAGMA memory_limit='{memory_limit}'")
    
    # Create the domains table in the temp database
    conn.execute("""
    CREATE TABLE domains (
        domain VARCHAR,
        nameservers VARCHAR,
        ip VARCHAR,
        country VARCHAR,
        server VARCHAR,
        field5 VARCHAR,
        field6 VARCHAR,
        field7 VARCHAR,
        field8 VARCHAR
    );
    """)
    
    # Prepare the COPY statement
    copy_options = f"(DELIMITER ';', HEADER 0, QUOTE '\"'"
    if compression != 'none' and compression != 'uncompressed':
        copy_options += f", COMPRESSION '{compression}'"
    copy_options += ")"
    
    # Extract chunk to temporary file
    rows_in_chunk = 0
    temp_file = os.path.join(temp_dir, f"chunk_{chunk_id}.csv")
    try:
        # Read the chunk from the file
        with open(file_path, 'rb') as f:
            f.seek(start_pos)
            data = f.read(end_pos - start_pos)
        
        # Count rows
        rows_in_chunk = data.count(b'\n')
        if data[-1:] != b'\n':
            rows_in_chunk += 1
        
        # Write to temporary file
        with open(temp_file, 'wb') as f:
            f.write(data)
        
        # Copy from temp file to temp database
        conn.execute(f"COPY domains FROM '{temp_file}' {copy_options}")
        
    except Exception as e:
        print(f"Error processing chunk {chunk_id}: {e}")
        rows_in_chunk = 0
    finally:
        conn.close()
        # Clean up temp file
        try:
            os.remove(temp_file)
        except:
            pass
    
    return (rows_in_chunk, temp_db_path)

def merge_temp_databases(db_path: str, temp_dbs: List[str], memory_limit: str):
    """Merge all temporary databases into the final database."""
    # Create or connect to the main database
    conn = duckdb.connect(db_path)
    conn.execute(f"PRAGMA memory_limit='{memory_limit}'")
    
    # Ensure the domains table exists
    conn.execute("""
    CREATE TABLE IF NOT EXISTS domains (
        domain VARCHAR,
        nameservers VARCHAR,
        ip VARCHAR,
        country VARCHAR,
        server VARCHAR,
        field5 VARCHAR,
        field6 VARCHAR,
        field7 VARCHAR,
        field8 VARCHAR
    );
    """)
    
    # Attach and merge each temp database
    total_rows = 0
    for i, temp_db in enumerate(temp_dbs):
        try:
            # Skip empty or corrupted databases
            if not os.path.exists(temp_db) or os.path.getsize(temp_db) < 1000:
                continue
                
            print(f"Merging database {i+1}/{len(temp_dbs)}: {temp_db}")
            
            # Attach the temp database
            conn.execute(f"ATTACH '{temp_db}' AS temp_db")
            
            # Count rows to add
            rows_to_add = conn.execute("SELECT COUNT(*) FROM temp_db.domains").fetchone()[0]
            
            # Insert data from temp database
            conn.execute("INSERT INTO domains SELECT * FROM temp_db.domains")
            
            # Update total
            total_rows += rows_to_add
            
            # Detach the temp database
            conn.execute("DETACH temp_db")
            
            # Optional: remove the temp database file
            os.remove(temp_db)
            
        except Exception as e:
            print(f"Error merging database {temp_db}: {e}")
    
    conn.close()
    return total_rows

def load_direct_copy(file_path: str, db_path: str, memory_limit: str, 
                     compression: str) -> int:
    """Load the entire file using a direct COPY statement."""
    conn = setup_database(db_path, memory_limit)
    
    # Prepare the COPY statement
    copy_options = f"(DELIMITER ';', HEADER 0, QUOTE '\"'"
    if compression != 'none' and compression != 'uncompressed':
        copy_options += f", COMPRESSION '{compression}'"
    copy_options += ")"
    
    # Execute the COPY command
    conn.execute(f"COPY domains FROM '{file_path}' {copy_options}")
    
    # Count the rows
    row_count = conn.execute("SELECT COUNT(*) FROM domains").fetchone()[0]
    conn.close()
    
    return row_count

def create_indexes(db_path: str):
    """Create indexes after data is loaded."""
    conn = duckdb.connect(db_path)
    
    print("Creating indexes...")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_domain ON domains(domain)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_ip ON domains(ip)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_country ON domains(country)")
    
    print("Running ANALYZE to optimize query planning...")
    try:
        conn.execute("ANALYZE domains")
    except:
        print("Note: ANALYZE command not available in this DuckDB version. Indexes are still created.")
    
    conn.close()

def main():
    args = parse_args()
    start_time = time.time()
    
    # Determine total rows (exact number from argument or estimate)
    total_rows = 217038672  # Set to the exact number you know
    
    # Create temp directory if it doesn't exist
    if not os.path.exists(args.temp_dir):
        os.makedirs(args.temp_dir)
    
    if args.direct:
        print(f"Using direct COPY to load data from {args.file}...")
        rows_loaded = load_direct_copy(
            args.file, args.db_path, args.memory_limit, args.compression
        )
    else:
        # Split the file into chunks
        chunks = get_file_chunks(args.file, total_rows, args.chunk_size)
        print(f"Split file into {len(chunks)} chunks")
        
        # Add chunk IDs
        chunks_with_ids = [(i, chunk) for i, chunk in enumerate(chunks)]
        
        # Determine parallelism
        workers = min(args.workers, len(chunks))
        print(f"Using {workers} parallel workers for loading...")
        
        # Process chunks in parallel, each into a separate temp database
        process_func = partial(process_chunk, 
                              file_path=args.file,
                              temp_dir=args.temp_dir,
                              memory_limit=args.memory_limit,
                              compression=args.compression)
        
        temp_dbs = []
        rows_processed = 0
        
        with multiprocessing.Pool(workers) as pool:
            with tqdm(total=total_rows, desc="Processing chunks") as pbar:
                for rows, temp_db in pool.imap_unordered(process_func, chunks_with_ids):
                    rows_processed += rows
                    if rows > 0:  # Only add non-empty databases
                        temp_dbs.append(temp_db)
                    pbar.update(rows)
        
        print(f"Processed {rows_processed:,} rows into {len(temp_dbs)} temporary databases")
        print("Merging databases...")
        
        # Merge all temp databases into final database
        rows_loaded = merge_temp_databases(args.db_path, temp_dbs, args.memory_limit)
        
        # Clean up temp directory if it's empty
        remaining_files = glob.glob(os.path.join(args.temp_dir, "*"))
        if not remaining_files:
            try:
                os.rmdir(args.temp_dir)
            except:
                pass
    
    # Create indexes after loading
    create_indexes(args.db_path)
    
    end_time = time.time()
    duration = end_time - start_time
    
    print(f"\nLoaded {rows_loaded:,} rows in {duration:.2f} seconds")
    print(f"Average speed: {rows_loaded / duration:,.2f} rows/second")
    
    # Stats summary
    if duration > 60:
        minutes = int(duration / 60)
        seconds = duration % 60
        print(f"Total time: {minutes} minutes and {seconds:.2f} seconds")
    
    # Estimate query performance
    conn = duckdb.connect(args.db_path)
    try:
        table_size = conn.execute("SELECT pg_table_size('domains')").fetchone()[0]
        print(f"Table size: {table_size / (1024*1024*1024):.2f} GB")
        
        # Check if file size is reasonable compared to input file
        input_size = os.path.getsize(args.file)
        ratio = table_size / input_size
        print(f"Storage efficiency: {ratio:.2f}x original file size")
    except:
        print("Note: Could not determine table size statistics with this DuckDB version.")
    
    # Verify a random query is fast
    query_start = time.time()
    result = conn.execute("SELECT COUNT(*) FROM domains WHERE country = 'US'").fetchone()[0]
    query_time = (time.time() - query_start) * 1000
    print(f"Verification query: {result:,} US domains found in {query_time:.2f} ms")
    
    conn.close()

if __name__ == "__main__":
    main()
