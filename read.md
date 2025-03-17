# Faster Indexing Techniques

# Parallel Index Creation: Create indexes on different columns simultaneously
# Sampling-based Indexing: First create indexes on a sample of the data to speed up full index creation
# Memory Optimization: Allocate more memory to the indexing process
# Field Selection: Only index the fields you'll actually query frequently

# How to Use the Fast Indexer
```
python faster_indexing.py --db-path domains.duckdb --memory-limit 12GB
```
Options for Faster Indexing:
# Create indexes in parallel (faster but more resource-intensive)
```
python faster_indexing.py --db-path domains.duckdb --parallel --memory-limit 24GB

# Index only the most important fields
python faster_indexing.py --db-path domains.duckdb --fields domain,ip

# Use sampling to speed up indexing (creates preliminary index on 10% of data)
python faster_indexing.py --db-path domains.duckdb --sample-size 0.1

# Skip the ANALYZE phase (saves time if you don't need perfect query plans)
python faster_indexing.py --db-path domains.duckdb --no-analyze
```
