# CSV Reader - Optimization Performance CSV Parser for Python

A Optimization performance CSV parsing library written in Rust with Python bindings. Designed for efficiently processing large CSV files with minimal memory footprint.

## Features

- **High Performance**: Parse CSV files significantly faster than Python's native CSV module
- **Memory Efficient**: Process files in optimized batches to reduce memory usage
- **Large File Support**: Efficiently handle multi-gigabyte CSV files
- **Customizable Batch Size**: Control memory usage and batch processing

## Installation

```bash
pip install rs-csv-reader
# Or if you're using UV
uv pip install rs-csv-reader
```

## Usage

### Basic Usage

```python
from csv_reader import CSVParser

# Create a parser with default settings
parser = CSVParser("large_file.csv", batch_size=5000)

# Read the file in batches
batches = parser.read()

# Process each batch
for batch in batches:
    for row in batch:
        # Each row is a dictionary with column names as keys
        print(row['id'], row['amount'])

# Count rows without loading the entire file
total_rows = parser.count_rows()
print(f"Total rows: {total_rows}")
```

### Reading Specific Chunks

Efficiently read specific portions of a CSV file without loading the entire file:

```python
# Read a specific chunk (starting from row 10000, reading 1000 rows)
chunk = parser.read_chunk(start_row=10000, num_rows=1000)

# Process the chunk
for row in chunk:
    process_row(row)
```

### Get File Information

```python
# Get file metadata
file_info = parser.get_file_info()
print(f"File size: {file_info['size_mb']} MB")
print(f"Headers: {file_info['headers']}")
```

## Performance

Can see on this repository profiling testing, testing with:
- Intel core i7
- 16 gb memory
- 8 core cpu

Benchmarks on a 2-million row CSV file:

| Parser | Time (s) | Memory (MB) | Rows/sec |
|--------|----------|-------------|----------|
| Python CSV | 4.23 | 1583.94 | 473028.05 |
| Pandas (full) | 9.42 | 1260.59 | 212226.58 |
| Pandas (chunked) | 9.13 | 1231.05 | 219060.70 |
| CSV Reader | 2.95 | 3183.24 | 678927.10 |

## How It Works

This library uses Rust's high-performance CSV parsing capabilities with smart buffering techniques:

- For files under 100MB: Loads the entire file into memory for maximum speed
- For larger files: Uses efficient buffered reading with a 64KB buffer
- Processes data in batches to balance memory usage and performance

## Building from Source

### Prerequisites

- Rust toolchain (install from [rust-lang.org](https://www.rust-lang.org/tools/install))
- Maturin (`pip install maturin`)
- Python development headers (`python-dev` or `python3-dev` package on Linux)

```bash
# Clone the repository
git clone https://github.com/yourusername/csv-reader.git
cd csv-reader

# Build the Rust library
maturin build --release

# Install the built wheel
pip install target/wheels/rs_csv_reader-*.whl
```

You can also use development mode for a faster workflow during development:

```bash
maturin develop --release
```

## Requirements

- Python 3.7+
- No additional dependencies required!

## License

This project is licensed under the MIT License - see the LICENSE file for details.

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

1. Fork the repository
2. Create your feature branch (`git checkout -b feature/amazing-feature`)
3. Commit your changes (`git commit -m 'Add some amazing feature'`)
4. Push to the branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request

## Acknowledgments

- The Rust CSV crate for providing the underlying parsing engine
- PyO3 for making Rust-Python bindings seamless


## Profiling Tools
For doing a testing, this the tools for test the CSV reader **https://github.com/yosephbernandus/csv_reader_profiling**

