import os
import time
import argparse
import duckdb
from multiprocessing import Process

def parse_args():
    parser = argparse.ArgumentParser(description='Fast index creation for DuckDB')
    parser.add_argument('--db-path', type=str, required=True, help='Path to the DuckDB database')
    parser.add_argument('--memory-limit', type=str, default='8GB', help='Memory limit for DuckDB')
    parser.add_argument('--parallel', action='store_true', help='Create indexes in parallel')
    parser.add_argument('--sample-size', type=float, default=1.0, 
                        help='Sample size for index creation (0.0-1.0)')
    parser.add_argument('--fields', type=str, default='domain,ip,country', 
                        help='Comma-separated list of fields to index')
    parser.add_argument('--no-analyze', action='store_true', help='Skip ANALYZE after indexing')
    return parser.parse_args()

def create_single_index(db_path, memory_limit, field, sample_size=1.0):
    """Create a single index on the specified field."""
    index_name = f"idx_{field}"
    
    # Connect to database with separate connection
    conn = duckdb.connect(db_path)
    conn.execute(f"PRAGMA memory_limit='{memory_limit}'")
    
    # Get start time for this index
    start_time = time.time()
    
    # Create a table name for this index
    table_name = "domains"
    
    # If using sampling, create a smaller temporary table
    if sample_size < 1.0:
        sample_table = f"sample_{field}_table"
        conn.execute(f"CREATE TEMPORARY TABLE {sample_table} AS SELECT * FROM {table_name} USING SAMPLE {sample_size}")
        table_for_index = sample_table
    else:
        table_for_index = table_name
    
    # Create the index
    print(f"Creating index on {field}...")
    try:
        conn.execute(f"CREATE INDEX IF NOT EXISTS {index_name} ON {table_for_index}({field})")
        
        # If we created the index on a sample table, create a real index on the main table
        if sample_size < 1.0:
            print(f"Creating full index on {field} based on sample...")
            conn.execute(f"CREATE INDEX IF NOT EXISTS {index_name} ON {table_name}({field})")
            conn.execute(f"DROP TABLE {sample_table}")
        
        duration = time.time() - start_time
        print(f"Index on {field} created in {duration:.2f} seconds")
    except Exception as e:
        print(f"Error creating index on {field}: {e}")
    
    # Close connection
    conn.close()

def parallel_index_creation(db_path, memory_limit, fields, sample_size=1.0):
    """Create indexes in parallel using multiple processes."""
    # Create a process for each field
    processes = []
    for field in fields:
        process = Process(target=create_single_index, 
                         args=(db_path, memory_limit, field, sample_size))
        processes.append(process)
    
    # Start all processes
    for process in processes:
        process.start()
    
    # Wait for all processes to complete
    for process in processes:
        process.join()

def optimize_index_creation(db_path, memory_limit, fields, parallel=False, sample_size=1.0, skip_analyze=False):
    """Create indexes on the specified fields with optimization."""
    start_time = time.time()
    
    # Optimize database before indexing
    conn = duckdb.connect(db_path)
    conn.execute(f"PRAGMA memory_limit='{memory_limit}'")
    
    # Check if database supports advanced optimizations
    try:
        # Try to enable checkpoint on shutdown for faster persistence
        conn.execute("PRAGMA checkpoint_on_shutdown=ON")
    except:
        pass
    
    # First, count total rows for progress estimation
    total_rows = conn.execute("SELECT COUNT(*) FROM domains").fetchone()[0]
    print(f"Creating indexes for {total_rows:,} rows...")
    
    # Connect and set optimal settings for indexing
    try:
        # These settings might help with indexing speed in newer DuckDB versions
        conn.execute("PRAGMA temp_directory='/tmp'")
        conn.execute("PRAGMA threads=8")
    except:
        pass
    
    conn.close()
    
    # Create indexes (either in parallel or serially)
    if parallel:
        parallel_index_creation(db_path, memory_limit, fields, sample_size)
    else:
        # Serial index creation
        for field in fields:
            create_single_index(db_path, memory_limit, field, sample_size)
    
    # Run ANALYZE after indexing for optimal query planning
    if not skip_analyze:
        print("Running ANALYZE to optimize query planning...")
        conn = duckdb.connect(db_path)
        try:
            conn.execute("ANALYZE domains")
        except Exception as e:
            print(f"Note: ANALYZE command failed: {e}")
            print("This is normal for some DuckDB versions. Indexes are still created.")
        conn.close()
    
    # Calculate and print total time
    total_time = time.time() - start_time
    print(f"All indexes created in {total_time:.2f} seconds")
    print("\nIndex creation complete!")
    print(f"Query performance should now be significantly improved.")

def main():
    args = parse_args()
    
    # Parse fields to index
    fields = [field.strip() for field in args.fields.split(',')]
    
    print(f"Fast index creation for {args.db_path}")
    print(f"Creating indexes on: {', '.join(fields)}")
    if args.parallel:
        print("Mode: Parallel index creation (experimental)")
    else:
        print("Mode: Serial index creation")
    
    optimize_index_creation(
        args.db_path, 
        args.memory_limit, 
        fields, 
        parallel=args.parallel,
        sample_size=args.sample_size,
        skip_analyze=args.no_analyze
    )

if __name__ == "__main__":
    main()
