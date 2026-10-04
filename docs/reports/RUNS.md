# Run notes

One file for all benchmark runs; the numbers themselves live in [Results](../RESULTS.md) (generated from raw JSON).

Shared protocol for every run: each repetition executes in a fresh subprocess with a single writer loaded, so imports are cold and writer environments cannot leak into each other. Every repetition uses a unique REST namespace and S3 prefix; afterwards the table and namespace are dropped, S3 objects and local pipeline state are deleted, and absence is verified. Correctness checks — read-back, schema, row count, full-content SHA-256 — run outside the timer. Hardware, software versions, and the exact timer scope are documented in [Methodology](../METHODOLOGY.md#environment) and [Methodology → Timing scope](../METHODOLOGY.md#timing-scope). All runs in this file date to 2026-10-02.

## Local (SQLite + filesystem)

Command: `.venv/bin/python run.py`

- The measured interval starts after construction of the common Arrow input and ends when the write API returns. It includes first-use library imports, table creation, commits, and dlt's extract/normalize/load stages.
- `peak_rss_delta_bytes` is the process high-water increase relative to the point after input construction. It is sensitive to allocator reuse and should not be read as total memory footprint.
- `local_disk_bytes` covers the whole local footprint (SQLite catalog + warehouse + dlt state); `remote_bytes` is 0 because there is no object storage.
- Run directories are deleted unless `--keep-runs` is passed. `results/raw.json` is regenerated locally and gitignored; the committed copy is [LOCAL_RAW_2026-10-02.json](../data/LOCAL_RAW_2026-10-02.json).

## REST (Polaris + MinIO)

Command: `.venv/bin/python rest_run.py` — one warmup and five measured repetitions per writer, size, and scenario.

- The input is synthetic, highly compressible, and already in Arrow memory; the timed interval includes each library's public write path, REST calls, and MinIO writes, including dlt's intermediate staging.
- At these file sizes, fixed pipeline and commit costs materially affect the result.
- These numbers do not isolate PyIceberg's core writer and do not represent workloads that begin with raw files or network sources.

## Public API, 100 MiB (NYC 311)

Command: `.venv/bin/python public_api_run.py --sizes-mib 100 --chunk-mib 64 --repeats 1 --warmups 0`

- **One trial per writer**: a functional and scale smoke check, not a statistical performance claim.
- The timed interval starts after the shared CSV download and includes streaming CSV parsing, REST catalog operations, and MinIO writes.
- The downloaded CSV was deleted after the trial.

## Public API, 1 GiB (NYC 311)

Command: `.venv/bin/python public_api_run.py --sizes-mib 1024 --chunk-mib 256 --repeats 3 --warmups 0`

- A common snapshot was downloaded once and shared by both writers; the source CSV was deleted as part of cleanup. The snapshot hash in the raw/source metadata identifies this run.
- The live NYC 311 dataset changes over time: later runs download a fresh snapshot and are not directly comparable to this one.
- A 10 GB source is supported via `--sizes-gb 10`; that size has not been measured in this run.
- dlt leaves local pipeline state after the run while iceberg-loader leaves none — this is why local and remote disk metrics are reported separately.