# Methodology

This document describes the procedures and controls used to ensure the fairness, reproducibility, and accuracy of the benchmark comparing `iceberg-loader` and `dlt`.

## Overview

The benchmark measures the time and resource consumption required to write data into Apache Iceberg tables. It compares a direct-to-Iceberg writer (`iceberg-loader`) against a pipeline-based writer (`dlt`). The benchmark covers three distinct environments: local filesystem with SQLite catalog, remote object storage with Polaris REST catalog, and real-world data ingestion via a public CSV API.

## Environment

Measurements were performed on the following hardware and software stack:

- **Hardware**: Apple M5 (MacBook Pro Mac17,2), 10 cores (4 performance + 6 efficiency), 24 GiB RAM, macOS 26.6.2 arm64.
- **Python**: 3.14.3.
- **Dependencies**: `uv.lock` pins the exact versions to ensure reproducibility.
  - `iceberg-loader` 0.1.4 (git tag v0.1.4)
  - `dlt` 1.30.0
  - `PyArrow` 25.0.1
  - `PyIceberg` 0.12.0
- **Infrastructure**:
  - Apache Polaris 1.8.0 REST Catalog
  - MinIO RELEASE.2025-10-15T17-29-55Z

The environment is reproduced using `uv sync`.

## Workloads

The benchmark uses three scripts, each targeting a different scenario:

### 1. Local SQLite/Filesystem (`run.py`)
Compares writers using a local SQLite catalog and filesystem warehouse.
- **Data**: Deterministic Arrow input (generated from row index).
- **Scenarios**:
  - `initial`: The entire table is written in one call.
  - `append`: Data is written in 10 equal consecutive slices.
- **Command**: `.venv/bin/python run.py`

### 2. Polaris REST + MinIO (`rest_run.py`)
Compares writers using a live REST catalog and S3-compatible storage.
- **Data**: Same deterministic Arrow input as `run.py`.
- **Command**: `.venv/bin/python rest_run.py`

### 3. NYC 311 CSV (`public_api_run.py`)
Measures ingestion of real-world data from a public API.
- **Data**: NYC 311 Service Requests CSV.
- **Logic**: Downloads the CSV once per run, saves a snapshot, and reads the records as Arrow strings. Chunks are formed based on a byte target.
- **Commands**:
  - 1 GiB: `.venv/bin/python public_api_run.py --sizes-mib 1024 --chunk-mib 256 --repeats 3 --warmups 0`
  - 10 GB: `.venv/bin/python public_api_run.py --sizes-gb 10 --chunk-mib 256 --repeats 3`

## Timing Scope

The timer uses `time.perf_counter()`. It wraps **only the write call**.

- **Excluded**: Data construction, CSV downloading, schema reading, read-back verification, and checksumming.
- **Included**: In `public_api_run.py`, the timer includes CSV parsing and one Iceberg commit per chunk.

## Warmups and Order-Bias Control

To eliminate noise and systemic bias:
- **Warmups**: Negative repeat counts are treated as warmups and are excluded from median calculations. `run.py` and `rest_run.py` default to 1 warmup and 5 measured repetitions. `public_api_run.py` defaults to 0 warmups.
- **Order-Bias**: The order of writers alternates by repeat parity. For even repetitions, the order is `('iceberg-loader', 'dlt')`; for odd repetitions, the order is reversed.

## Process Isolation

To prevent environment leaks or import-cache advantages:
- Each repetition is executed in a **fresh subprocess**.
- Each subprocess loads only one writer.
- This ensures that imports are "cold" for every run.

## Commit Semantics

Both writers are configured to commit once per chunk or part.
- **iceberg-loader**: In `public_api_run.py`, `commit_interval` is set to the number of batches in the chunk.
- **dlt**: One `pipeline.run` is executed per chunk.
Raw data confirms equal snapshot counts for every configuration.

## Correctness Checks

Validation is performed outside the timer to ensure data integrity:
- **`run.py`**: Checks row count, column-name set, schema equality, and full-content SHA-256.
- **`rest_run.py`**: Checks table location, full read-back row count, schema, and full-content SHA-256.
- **`public_api_run.py`**: Checks Iceberg schema column names against CSV headers, verifies all types are strings, checks Parquet metadata row count, performs a read-back sample of up to 1000 rows, and verifies table location and remote file existence.

## Metrics

The following metrics are captured for each run:
- `seconds`: Delta of `perf_counter` during the write call.
- `peak_rss_delta_bytes`: Process high-water mark increase relative to the state after input construction (`max(0, peak - before)`). Unit is 1 byte on macOS.
- `local_disk_bytes`: Footprint of the local run directory.
- `remote_bytes`: Total size of remote objects created.
- `table_bytes`: Size of the target Iceberg table.
- `data_files`: Count of Parquet files.
- `snapshots`: Count of Iceberg snapshots.

`local_disk_bytes` and `remote_bytes` are reported separately: `dlt` keeps local pipeline state while `iceberg-loader` does not, so a combined disk metric would not be comparable.

## Why dlt is Slower

The gap is architectural rather than an artifact of the benchmark: dlt stages intermediate Parquet files and runs a full pipeline per chunk, while iceberg-loader appends directly from memory. The gap grows in the `append` scenario, where dlt pays that pipeline cost for every slice. A detailed write-up lives in [the companion article](../anonse.md); where it accumulates by scenario, see [Results](RESULTS.md).

## Fairness Controls

- **Identical Input**: Both writers read the same input data in the same parent run. This is verified by identical source SHA-256 values in the raw JSON output.
- **Identical Defaults**: Both writers use the same PyIceberg defaults (zstd compression, format-version 2), resulting in nearly identical final table sizes.

## Limitations

- The benchmark does not measure the time to download the source CSV.
- Results are specific to the hardware and versions listed; they should be viewed as relative performance comparisons rather than absolute throughput benchmarks.
