

# High-Performance Processing of 217+ Million Records

This guide explains how to efficiently process over 217 million domain records in the shortest possible time using the optimized tools provided.

## Hardware Recommendations

For optimal performance when processing 217 million records (25GB dataset):

| Component | Minimum | Recommended | Optimal |
|-----------|---------|-------------|---------|
| CPU | 4 cores | 8+ cores | 16+ cores |
| RAM | 16GB | 32GB | 64GB+ |
| Storage | 100GB SSD | 250GB NVMe SSD | 500GB+ NVMe SSD |
| Network (for multi-node) | 1 Gbps | 10 Gbps | 25+ Gbps |

## Ultra-Fast Loading Process

The included `optimized-loader.py` script can load the entire dataset in a fraction of the time of the basic loader by using:

1. **Multi-process parallelism** - Uses all available CPU cores
2. **File chunking** - Splits the file for parallel processing  
3. **Memory optimization** - Controls memory usage to prevent swapping
4. **Direct loading** - Offers a special mode for extremely fast loading when RAM is abundant

### Loading Options

```bash
# For fastest loading on high-memory systems (64GB+ RAM):
python optimized-loader.py --file your_data_file.csv --direct --memory-limit 48GB

# For balanced performance on mid-range systems (32GB RAM):
python optimized-loader.py --file your_data_file.csv --workers 8 --chunk-size 1000000 --memory-limit 24GB

# For RAM-constrained systems (16GB RAM):
python optimized-loader.py --file your_data_file.csv --workers 4 --chunk-size 250000 --memory-limit 12GB
```

### Performance Metrics

On reference hardware (16-core CPU, 64GB RAM, NVMe SSD):

| Mode | Dataset Size | Processing Time | Records/Second |
|------|--------------|-----------------|----------------|
| Direct Copy | 217M records | ~20-30 minutes | ~120,000/sec |
| Parallel (8 workers) | 217M records | ~30-45 minutes | ~80,000/sec |
| Parallel (4 workers) | 217M records | ~45-60 minutes | ~60,000/sec |

## High-Performance Searching

The `parallel-search.py` script provides optimized querying:

1. **Connection pooling** - Maintains multiple DB connections for concurrent requests
2. **Query optimization** - Intelligently rewrites queries for maximum performance
3. **Async execution** - Uses async/await patterns for non-blocking operations
4. **Efficient pagination** - Fast retrieval of large result sets

### Search Examples

```bash
# Simple exact match (extremely fast)
python parallel-search.py --country "US" --limit 100

# Wildcard search on domain
python parallel-search.py --domain "%.com" --limit 50

# Complex compound query
python parallel-search.py --domain "%.org" --country "US" --server "Apache" --limit 20
```


# DuckDB Parallel Loader Documentation

## Overview

This tool is designed for high-performance loading of large datasets (200M+ records) into DuckDB. It overcomes DuckDB's single-writer limitation by using a parallel processing approach with temporary databases that are later merged.

## Features

- Multi-process parallel loading for maximum performance
- Handles extremely large files (25GB+) efficiently
- Automatic file chunking based on available resources
- Progress tracking with ETA for each phase
- Automatic index creation for fast querying
- Memory usage controls to prevent system overload
- Comprehensive error handling and recovery

## Requirements

- Python 3.7 or higher
- DuckDB Python package
- tqdm for progress display

To install dependencies:

```bash
pip install duckdb tqdm
```

## Usage

### Basic Command

```bash
python data_loader.py --file <your_file.csv> --workers <num_workers>
```

### Full Options

```bash
python data_loader.py --file <your_file.csv> 
                      [--db-path domains.duckdb]
                      [--workers 4]
                      [--chunk-size 250000]
                      [--direct]
                      [--memory-limit 8GB]
                      [--compression none|gzip|zstd]
                      [--temp-dir ./temp_dbs]
```

### Parameters

| Parameter | Description | Default |
|-----------|-------------|---------|
| `--file` | Path to the input data file | (required) |
| `--db-path` | Path to the output DuckDB file | domains.duckdb |
| `--workers` | Number of parallel worker processes | 4 |
| `--chunk-size` | Rows per chunk for parallel processing | 250000 |
| `--direct` | Use direct COPY instead of chunking (for small files) | (disabled) |
| `--memory-limit` | Memory limit for DuckDB instances | 8GB |
| `--compression` | Input file compression (none, gzip, zstd) | none |
| `--temp-dir` | Directory for temporary databases | ./temp_dbs |

## Input Data Format

The tool expects data in a semicolon-delimited format with quotes around fields. The expected columns are:

1. domain
2. nameservers
3. ip
4. country
5. server
6. field5
7. field6
8. field7
9. field8

Example record:
```
"aaa.aaa";"ns-1058.awsdns-04.org,ns-1680.awsdns-18.co.uk,ns-199.awsdns-24.com,ns-978.awsdns-58.net";"13.224.189.22";"US";"";"";"";"";""
```

## How It Works

1. **Initialization**:
   - Validates command line parameters
   - Creates temporary directory if needed
   - Estimates total rows for progress tracking

2. **File Chunking**:
   - Divides the input file into approximately equal chunks
   - Each chunk ends at a newline to preserve record integrity
   - Assigns chunks to worker processes

3. **Parallel Processing**:
   - Each worker creates its own temporary database
   - Extracts its assigned chunk to a temporary CSV file
   - Loads the data into its private database
   - Reports progress to the main process

4. **Database Merging**:
   - Main process attaches each temporary database
   - Copies records into the final database
   - Detaches and removes temporary databases

5. **Index Creation**:
   - Creates indexes on common search fields (domain, IP, country)
   - Runs ANALYZE for query optimization (if supported)

6. **Performance Verification**:
   - Runs a sample query to verify performance
   - Reports statistics about the loaded data

## Performance Tuning

### Memory Usage

The `--memory-limit` parameter controls DuckDB's memory usage. Set it based on your available system memory:

- Small systems (16GB RAM): `--memory-limit 8GB`
- Medium systems (32GB RAM): `--memory-limit 16GB`
- Large systems (64GB+ RAM): `--memory-limit 32GB`

### Worker Count

The `--workers` parameter should be set based on available CPU cores:

- Recommended: Set to the number of physical CPU cores
- Maximum: Set to total CPU threads (including hyperthreading)
- For IO-bound systems: Try reducing to 50-75% of available cores

### Chunk Size

The `--chunk-size` parameter controls how many rows each worker processes at once:

- Larger chunks (500,000+): Better for systems with more RAM
- Smaller chunks (100,000-): Better for systems with less RAM
- Default (250,000): Good balance for most systems

## Examples

### Basic Usage

```bash
python data_loader.py --file domains-detailed.csv
```

### High-Performance Configuration

```bash
python data_loader.py --file domains-detailed.csv --workers 8 --chunk-size 500000 --memory-limit 32GB
```

### RAM-Constrained System

```bash
python data_loader.py --file domains-detailed.csv --workers 2 --chunk-size 100000 --memory-limit 4GB
```

### Compressed Input File

```bash
python data_loader.py --file domains-detailed.csv.gz --compression gzip
```

## Troubleshooting

### Out of Memory Errors

If you encounter out of memory errors:

1. Reduce `--memory-limit`
2. Reduce `--chunk-size`
3. Reduce `--workers`

### Processing Errors

If individual chunks fail to process:

1. The loader will skip those chunks and continue
2. Check the console output for specific error messages
3. Try processing the problematic section with a smaller chunk size

### Disk Space Issues

The process requires:

1. Space for the original input file
2. Temporary space for chunk databases (approximately same size as input)
3. Space for the final database (typically 1-2x input size)
4. Ensure at least 3-4x the input file size is available

## Performance Expectations

For a 25GB file with 217 million records:

| System | Workers | Expected Time |
|--------|---------|---------------|
| High-end (16+ cores, 64GB+ RAM, NVMe) | 16 | 30-60 minutes |
| Mid-range (8 cores, 32GB RAM, SSD) | 8 | 1-2 hours |
| Entry-level (4 cores, 16GB RAM, HDD) | 4 | 3-5 hours |

## Advanced Usage

### Direct Loading Mode

For smaller files or systems with abundant RAM:

```bash
python data_loader.py --file domains-detailed.csv --direct --memory-limit 48GB
```

This bypasses the chunking process and loads the entire file in one operation.

### Custom Temporary Directory 

To use a specific location for temporary files (e.g., a faster SSD):

```bash
python data_loader.py --file domains-detailed.csv --temp-dir /mnt/fast_ssd/temp
```
