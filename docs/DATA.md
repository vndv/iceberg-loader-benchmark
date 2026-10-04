# Data

This document details the datasets used in the benchmark, including source origins, schema normalization, and identity guarantees.

## NYC 311 Dataset (Public API)

The "Public API" workload uses real-world data from the NYC 311 Service Requests dataset.

- **Dataset**: NYC 311 Service Requests from 2020 to Present.
- **Socrata ID**: `erm2-nwe9`.
- **Publisher**: NYC Office of Technology and Innovation (OTI).
- **Export Endpoint**: `https://data.cityofnewyork.us/api/views/erm2-nwe9/rows.csv?accessType=DOWNLOAD`
- **License/Attribution**: Published under NYC's Open Data Law. Attribution: "311", Office of Technology and Innovation (OTI). See [NYC Open Data Terms of Use](https://www.nyc.gov/main/terms-of-use).

### Schema Normalization

The CSV export returns 44 columns using human-readable display labels rather than Socrata fieldNames. To ensure a stable schema, the benchmark applies the following normalization rules to each header:
1. Strip leading/trailing whitespace.
2. Convert to lowercase.
3. Replace spaces with underscores.
4. Remove parentheses `(` and `)`.
5. Force the first column name to `unique_key`.

**Note on Columns 6–8**: These columns have display labels that differ from their Socrata fieldNames due to a 2025 renaming by NYC:
- `problem_formerly_complaint_type` (fieldName: `complaint_type`)
- `problem_detail_formerly_descriptor` (fieldName: `descriptor`)
- `additional_details` (fieldName: `descriptor_2`)

### Full Normalized Schema (44 Columns)

All fields are read as Arrow strings to avoid writer type-inference differences. The complete normalized schema is recorded verbatim as `schema` in the source-metadata JSON for each snapshot: [PUBLIC_API_1G_SOURCE_2026-10-02.json](data/PUBLIC_API_1G_SOURCE_2026-10-02.json) and [PUBLIC_API_SOURCE_2026-10-02.json](data/PUBLIC_API_SOURCE_2026-10-02.json).

### Snapshot Metadata

Because the NYC 311 dataset changes over time, specific snapshots are recorded using SHA-256 hashes:

| Snapshot | Bytes | Rows | SHA-256 |
| :--- | :--- | :--- | :--- |
| 1 GiB | 1,073,741,619 | 1,721,470 | `17b97f0e66b9f1de2386784f68ab5b29ad50316065f2f337cf83ff1522e5ce36` |
| 100 MiB | 104,857,554 | 165,451 | `b7bd4a6dd66110afacc6110754d9e70a1d1389ebb88ceffd13dc230e97fac9f8` |

Source metadata (URL, ETag, Last-Modified, and normalized schema) is stored in the corresponding `.json` files in `docs/data/`.

## Generated Arrow Data (Local + REST)

The dataset is a pure function of the row index `i`: five fields `id`, `amount`, `category`, `event_time` (UTC `timestamp[us]`), and `note` (null on every 11th row, otherwise a payload string). The exact formulas live in `make_data()` in [run.py](../run.py), which is the source of truth; `rest_run.py` uses the same generator.
