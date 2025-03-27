use csv::ReaderBuilder;
use pyo3::prelude::*;
use pyo3::types::{PyDict, PyList};
use std::fs::File;
use std::io::{BufReader, Read, Seek, SeekFrom};
use std::path::Path;

// Custom buffer size for faster I/O
const BUF_SIZE: usize = 64 * 1024; // 64KB buffer

#[pyclass]
struct CSVParser {
    filename: String,
    batch_size: usize,
    #[pyo3(get)]
    has_headers: bool,
    file_size: u64,
}

#[pymethods]
impl CSVParser {
    #[new]
    fn new(filename: String, batch_size: usize, has_headers: Option<bool>) -> PyResult<Self> {
        // Get file size during initialization to avoid reopening for size check
        let file_size = match File::open(&filename) {
            Ok(file) => match file.metadata() {
                Ok(metadata) => metadata.len(),
                Err(_) => 0,
            },
            Err(e) => {
                return Err(PyErr::new::<pyo3::exceptions::PyIOError, _>(format!(
                    "Failed to open file: {}",
                    e
                )));
            }
        };

        Ok(CSVParser {
            filename,
            batch_size,
            has_headers: has_headers.unwrap_or(true),
            file_size,
        })
    }

    // Read the CSV file and return batches of rows as Python objects
    fn read(&self, py: Python) -> PyResult<Vec<PyObject>> {
        // Fast path: read entire file into memory for large files
        if self.file_size > 0 && self.file_size < 100 * 1024 * 1024 {
            // check if under 100 MB 1024 as kb
            return self.read_optimized(py); // Will read whole file to memory first
        }

        // Write with chunking for larger files
        let path = Path::new(&self.filename);
        let file = match File::open(path) {
            Ok(f) => BufReader::with_capacity(BUF_SIZE, f),
            Err(e) => {
                return Err(PyErr::new::<pyo3::exceptions::PyIOError, _>(format!(
                    "Failed to open file: {}",
                    e
                )));
            }
        };

        let mut reader = ReaderBuilder::new()
            .flexible(true)
            .has_headers(self.has_headers)
            .from_reader(file);

        let headers = match reader.headers() {
            Ok(h) => h.clone(),
            Err(e) => {
                return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(format!(
                    "Failed to read CSV headers: {}",
                    e
                )));
            }
        };

        // Pre-allocate the vector to reduce reallocations
        let mut batches: Vec<PyObject> =
            Vec::with_capacity((self.file_size / (self.batch_size as u64 * 100) + 1) as usize);

        let mut current_batch = PyList::empty(py);
        let mut current_rows = Vec::with_capacity(self.batch_size);
        let mut count: usize = 0;

        // Process records in batches for better memory usage
        let iter = reader.records();
        for result in iter {
            let record = match result {
                Ok(r) => r,
                Err(e) => {
                    return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(format!(
                        "Failed to read CSV record: {}",
                        e
                    )));
                }
            };

            // Create Python dict for this record
            let row = PyDict::new(py);

            // Efficient field extraction
            for (i, field) in record.iter().enumerate() {
                if i < headers.len() {
                    let header = headers.get(i).unwrap_or("None");
                    // Direct set without unnecessary conversions
                    row.set_item(header, field)?;
                }
            }

            // Store row
            current_rows.push(row.to_object(py));
            count += 1;

            // When batch is full, add to batches and create new batch
            if count >= self.batch_size {
                // Build list from collected rows
                for row in &current_rows {
                    let _ = current_batch.append(row.clone_ref(py))?;
                }

                batches.push(current_batch.to_object(py));
                current_batch = PyList::empty(py);
                current_rows.clear();
                count = 0;
            }
        }

        // Don't forget remaining rows
        if count > 0 {
            for row in &current_rows {
                let _ = current_batch.append(row.clone_ref(py))?;
            }
            batches.push(current_batch.to_object(py));
        }

        Ok(batches)
    }

    // Optimized method for reading entire file at once (for smaller files)
    fn read_optimized(&self, py: Python) -> PyResult<Vec<PyObject>> {
        let path = Path::new(&self.filename);

        // Read the entire file into memory at once
        let mut content = Vec::with_capacity(self.file_size as usize);
        {
            let mut file = match File::open(path) {
                Ok(f) => f,
                Err(e) => {
                    return Err(PyErr::new::<pyo3::exceptions::PyIOError, _>(format!(
                        "Failed to open file: {}",
                        e
                    )));
                }
            };

            if let Err(e) = file.read_to_end(&mut content) {
                return Err(PyErr::new::<pyo3::exceptions::PyIOError, _>(format!(
                    "Failed to read file: {}",
                    e
                )));
            }
        }

        // Process the content with a memory reader (faster than file I/O)
        let mut reader = ReaderBuilder::new()
            .flexible(true)
            .has_headers(self.has_headers)
            .from_reader(content.as_slice());

        let headers = match reader.headers() {
            Ok(h) => h.clone(),
            Err(e) => {
                return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(format!(
                    "Failed to read CSV headers: {}",
                    e
                )));
            }
        };

        // Pre-allocate results
        let estimated_rows = content.len() / 50; // Rough estimate of rows based on byte size
                                                 // heuristic value as count as
                                                 // A few numeric fields (4-8 bytes each)
                                                 // A few short text fields (10-20 bytes each)
                                                 // Commas between fields (1 byte each)
                                                 // A newline character (1-2 bytes)
        let estimated_batches = (estimated_rows / self.batch_size) + 1; // + 1 is for the remainder batch if any
        let mut batches: Vec<PyObject> = Vec::with_capacity(estimated_batches);

        // Process in batches
        let mut current_batch = PyList::empty(py);
        let mut current_rows = Vec::with_capacity(self.batch_size);
        let mut count: usize = 0;

        // Process all records at once
        for result in reader.records() {
            let record = match result {
                Ok(r) => r,
                Err(e) => {
                    return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(format!(
                        "Failed to read CSV record: {}",
                        e
                    )));
                }
            };

            // Create dict with capacity for all fields
            let row = PyDict::new(py);

            // Process all fields
            for (i, field) in record.iter().enumerate() {
                if i < headers.len() {
                    let header = headers.get(i).unwrap_or("None");
                    row.set_item(header, field)?;
                }
            }

            // Add to batch
            current_rows.push(row.to_object(py));
            count += 1;

            // When batch is full, push to batches
            if count >= self.batch_size {
                // Build list from collected rows
                for row in &current_rows {
                    let _ = current_batch.append(row.clone_ref(py))?;
                }

                batches.push(current_batch.to_object(py));
                current_batch = PyList::empty(py);
                current_rows.clear();
                count = 0;
            }
        }

        // Add any remaining rows
        if count > 0 {
            for row in &current_rows {
                let _ = current_batch.append(row.clone_ref(py))?;
            }
            batches.push(current_batch.to_object(py));
        }

        Ok(batches)
    }

    // Get the total number of rows in the CSV file (optimized)
    fn count_rows(&self) -> PyResult<usize> {
        let path = Path::new(&self.filename);
        let file = match File::open(path) {
            Ok(f) => BufReader::with_capacity(BUF_SIZE, f),
            Err(e) => {
                return Err(PyErr::new::<pyo3::exceptions::PyIOError, _>(format!(
                    "Failed to open file: {}",
                    e
                )));
            }
        };

        let mut reader = ReaderBuilder::new()
            .has_headers(self.has_headers)
            .from_reader(file);

        // If headers exist, we need to account for them
        if self.has_headers {
            if reader.headers().is_err() {
                return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(
                    "Failed to read headers".to_string(),
                ));
            }
        }

        // Count rows efficiently
        let mut count = 0;
        for result in reader.records() {
            if result.is_ok() {
                count += 1;
            }
        }

        Ok(count)
    }

    // Optimized method to read a specific chunk of the CSV file
    fn read_chunk(&self, py: Python, start_row: usize, num_rows: usize) -> PyResult<PyObject> {
        if start_row == 0 && self.has_headers {
            // Just use the regular read method with a limit
            let path = Path::new(&self.filename);
            let file = match File::open(path) {
                Ok(f) => BufReader::with_capacity(BUF_SIZE, f),
                Err(e) => {
                    return Err(PyErr::new::<pyo3::exceptions::PyIOError, _>(format!(
                        "Failed to open file: {}",
                        e
                    )));
                }
            };

            let mut reader = ReaderBuilder::new()
                .has_headers(self.has_headers)
                .from_reader(file);

            let headers = match reader.headers() {
                Ok(h) => h.clone(),
                Err(e) => {
                    return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(format!(
                        "Failed to read CSV headers: {}",
                        e
                    )));
                }
            };

            let chunk = PyList::empty(py);

            // Process only up to num_rows
            for (_, result) in reader.records().take(num_rows).enumerate() {
                let record = match result {
                    Ok(r) => r,
                    Err(e) => {
                        return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(format!(
                            "Failed to read CSV record: {}",
                            e
                        )));
                    }
                };

                let row = PyDict::new(py);

                for (i, field) in record.iter().enumerate() {
                    if i < headers.len() {
                        let header = headers.get(i).unwrap_or("None");
                        row.set_item(header, field)?;
                    }
                }

                let _ = chunk.append(row.to_object(py))?;
            }

            return Ok(chunk.to_object(py));
        }

        // For seeking to a specific row, we need a more efficient approach
        // This is a more complex implementation for larger start_row values
        let chunk = self.read_chunk_optimized(py, start_row, num_rows)?;
        Ok(chunk)
    }

    // Advanced chunk reading with seeking optimization
    fn read_chunk_optimized(
        &self,
        py: Python,
        start_row: usize,
        num_rows: usize,
    ) -> PyResult<PyObject> {
        let path = Path::new(&self.filename);

        // If we're starting far into the file, try to estimate the position
        // and seek to it before reading to avoid processing unnecessary rows
        if start_row > 1000 {
            // Use the file size to estimate bytes per row
            if self.file_size > 0 {
                // First estimate bytes per row by sampling
                let estimated_bytes_per_row = self.estimate_bytes_per_row()?;

                if estimated_bytes_per_row > 0.0 {
                    // Create a seekable reader
                    let file = match File::open(path) {
                        Ok(f) => f,
                        Err(e) => {
                            return Err(PyErr::new::<pyo3::exceptions::PyIOError, _>(format!(
                                "Failed to open file: {}",
                                e
                            )));
                        }
                    };

                    let mut reader = BufReader::with_capacity(BUF_SIZE, file);
                    let mut buffer = [0; 1];
                    while reader.read_exact(&mut buffer).is_ok() {
                        if buffer[0] == b'\n' {
                            break;
                        }
                    }

                    // Estimate position for start_row
                    let header_offset = if self.has_headers {
                        estimated_bytes_per_row
                    } else {
                        0.0
                    };
                    let estimated_pos =
                        (estimated_bytes_per_row * start_row as f64) + header_offset;

                    // Seek to estimated position
                    if estimated_pos < self.file_size as f64 {
                        // Seek to slightly before estimated position to ensure we don't miss a row
                        let safe_pos =
                            (estimated_pos - estimated_bytes_per_row * 2.0).max(0.0) as u64;
                        if let Err(e) = reader.seek(SeekFrom::Start(safe_pos)) {
                            return Err(PyErr::new::<pyo3::exceptions::PyIOError, _>(format!(
                                "Failed to seek in file: {}",
                                e
                            )));
                        }

                        // Skip to next line boundary
                        let mut buffer = [0; 1];
                        while reader.read_exact(&mut buffer).is_ok() {
                            if buffer[0] == b'\n' {
                                break;
                            }
                        }

                        // Now recreate the reader at this position
                        let pos = reader.stream_position().unwrap_or(0);
                        drop(reader);

                        let file = match File::open(path) {
                            Ok(f) => f,
                            Err(e) => {
                                return Err(PyErr::new::<pyo3::exceptions::PyIOError, _>(format!(
                                    "Failed to open file: {}",
                                    e
                                )));
                            }
                        };

                        let mut reader = BufReader::with_capacity(BUF_SIZE, file);

                        // Seek to our calculated position
                        if let Err(e) = reader.seek(SeekFrom::Start(pos)) {
                            return Err(PyErr::new::<pyo3::exceptions::PyIOError, _>(format!(
                                "Failed to seek in file: {}",
                                e
                            )));
                        }

                        // Create new reader from this position
                        let mut csv_reader = ReaderBuilder::new()
                            .has_headers(false) // Important: no headers since we're mid-file
                            .from_reader(reader);

                        // Read headers first to know field names
                        // We need to get the headers from the beginning of the file
                        let headers = {
                            let header_file = match File::open(path) {
                                Ok(f) => f,
                                Err(e) => {
                                    return Err(PyErr::new::<pyo3::exceptions::PyIOError, _>(
                                        format!("Failed to open file for headers: {}", e),
                                    ));
                                }
                            };

                            let mut header_reader = ReaderBuilder::new()
                                .has_headers(true)
                                .from_reader(header_file);

                            match header_reader.headers() {
                                Ok(h) => h.clone(),
                                Err(e) => {
                                    return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(
                                        format!("Failed to read CSV headers: {}", e),
                                    ));
                                }
                            }
                        };

                        // Now read records from our seeked position
                        let chunk = PyList::empty(py);
                        let mut current_row = 0;

                        for result in csv_reader.records().take(num_rows) {
                            let record = match result {
                                Ok(r) => r,
                                Err(e) => {
                                    return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(
                                        format!("Failed to read CSV record: {}", e),
                                    ));
                                }
                            };

                            let row = PyDict::new(py);

                            for (i, field) in record.iter().enumerate() {
                                if i < headers.len() {
                                    let header = headers.get(i).unwrap_or("None");
                                    row.set_item(header, field)?;
                                }
                            }

                            let _ = chunk.append(row.to_object(py))?;
                            current_row += 1;

                            if current_row >= num_rows {
                                break;
                            }
                        }

                        return Ok(chunk.to_object(py));
                    }
                }
            }
        }

        // Fallback: read row-by-row until we reach start_row
        let file = match File::open(path) {
            Ok(f) => BufReader::with_capacity(BUF_SIZE, f),
            Err(e) => {
                return Err(PyErr::new::<pyo3::exceptions::PyIOError, _>(format!(
                    "Failed to open file: {}",
                    e
                )));
            }
        };

        let mut reader = ReaderBuilder::new()
            .has_headers(self.has_headers)
            .from_reader(file);

        let headers = match reader.headers() {
            Ok(h) => h.clone(),
            Err(e) => {
                return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(format!(
                    "Failed to read CSV headers: {}",
                    e
                )));
            }
        };

        let chunk = PyList::empty(py);

        // Skip rows until start_row
        let mut records = reader.records();
        for _ in 0..start_row {
            if records.next().is_none() {
                // Reached end of file before start_row
                return Ok(chunk.to_object(py));
            }
        }

        // Read num_rows rows
        for _ in 0..num_rows {
            match records.next() {
                Some(Ok(record)) => {
                    let row = PyDict::new(py);

                    for (i, field) in record.iter().enumerate() {
                        if i < headers.len() {
                            let header = headers.get(i).unwrap_or("None");
                            row.set_item(header, field)?;
                        }
                    }

                    let _ = chunk.append(row.to_object(py))?;
                }
                Some(Err(e)) => {
                    return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(format!(
                        "Failed to read CSV record: {}",
                        e
                    )));
                }
                None => break, // End of file
            }
        }

        Ok(chunk.to_object(py))
    }

    // Helper method to estimate bytes per row
    fn estimate_bytes_per_row(&self) -> PyResult<f64> {
        let path = Path::new(&self.filename);
        let file = match File::open(path) {
            Ok(f) => f,
            Err(e) => {
                return Err(PyErr::new::<pyo3::exceptions::PyIOError, _>(format!(
                    "Failed to open file: {}",
                    e
                )));
            }
        };

        let mut reader = BufReader::with_capacity(BUF_SIZE, file);
        let start_pos = match reader.stream_position() {
            Ok(pos) => pos,
            Err(e) => {
                return Err(PyErr::new::<pyo3::exceptions::PyIOError, _>(format!(
                    "Failed to get stream position: {}",
                    e
                )));
            }
        };

        // Create a CSV reader that will read from our buffered reader
        let mut csv_reader = ReaderBuilder::new()
            .has_headers(self.has_headers)
            .from_reader(reader.by_ref());

        // Skip header if needed
        if self.has_headers {
            if csv_reader.headers().is_err() {
                return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(
                    "Failed to read headers".to_string(),
                ));
            }
        }

        // Count bytes for sample rows
        let sample_size = 100;
        let mut row_count = 0;

        for _ in 0..sample_size {
            match csv_reader.records().next() {
                Some(Ok(_)) => row_count += 1,
                Some(Err(e)) => {
                    return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(format!(
                        "Error reading sample row: {}",
                        e
                    )));
                }
                None => break, // End of file
            }
        }

        // Get the current position after reading sample rows
        let end_pos = match reader.stream_position() {
            Ok(pos) => pos,
            Err(e) => {
                return Err(PyErr::new::<pyo3::exceptions::PyIOError, _>(format!(
                    "Failed to get stream position: {}",
                    e
                )));
            }
        };

        if row_count > 0 {
            Ok((end_pos - start_pos) as f64 / row_count as f64)
        } else {
            // If we couldn't read any rows, return a default value
            Ok(100.0) // Default guess: 100 bytes per row
        }
    }

    // New method: get file information
    fn get_file_info(&self, py: Python) -> PyResult<PyObject> {
        let path = Path::new(&self.filename);
        let metadata = match std::fs::metadata(path) {
            Ok(m) => m,
            Err(e) => {
                return Err(PyErr::new::<pyo3::exceptions::PyIOError, _>(format!(
                    "Failed to get file metadata: {}",
                    e
                )));
            }
        };

        let info = PyDict::new(py);
        info.set_item("filename", &self.filename)?;
        info.set_item("size_bytes", metadata.len())?;
        info.set_item("size_mb", (metadata.len() as f64) / (1024.0 * 1024.0))?;
        info.set_item("batch_size", self.batch_size)?;
        info.set_item("has_headers", self.has_headers)?;

        // Try to get sample headers
        let file = match File::open(path) {
            Ok(f) => f,
            Err(e) => {
                return Err(PyErr::new::<pyo3::exceptions::PyIOError, _>(format!(
                    "Failed to open file: {}",
                    e
                )));
            }
        };

        let mut reader = ReaderBuilder::new()
            .has_headers(self.has_headers)
            .from_reader(file);

        if self.has_headers {
            match reader.headers() {
                Ok(headers) => {
                    // Convert headers to a vector of strings first
                    let header_vec: Vec<&str> = headers.iter().collect();
                    let header_list = PyList::new(py, &header_vec);
                    info.set_item("headers", header_list)?;
                }
                Err(_) => {
                    info.set_item("headers", PyList::empty(py))?;
                }
            }
        }

        Ok(info.to_object(py))
    }
}

#[pymodule]
fn csv_reader(_py: Python, m: &PyModule) -> PyResult<()> {
    m.add_class::<CSVParser>()?;
    Ok(())
}
