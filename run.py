"""Reproducible Arrow-to-Iceberg comparison of iceberg-loader and dlt."""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import os
import platform
import resource
import shutil
import statistics
import subprocess
import sys
import time
import uuid
from pathlib import Path

import pyarrow as pa

NAMESPACE = 'bench'
TABLE = 'events'
APPENDS = 10
FIELDS = ('id', 'amount', 'category', 'event_time', 'note')


def make_data(rows: int) -> pa.Table:
    """Build identical, deterministic input for either writer."""
    ids = pa.array(range(rows), type=pa.int64())
    return pa.table(
        {
            'id': ids,
            'amount': pa.array((i % 100_000 for i in range(rows)), type=pa.int64()),
            'category': pa.array((f'category_{i % 37:02d}' for i in range(rows)), type=pa.string()),
            'event_time': pa.array(
                (1_700_000_000_000_000 + i * 1_000_000 for i in range(rows)), type=pa.timestamp('us', tz='UTC')
            ),
            'note': pa.array(
                (None if i % 11 == 0 else f'payload_{i % 997:03d}' for i in range(rows)), type=pa.string()
            ),
        },
    )


def digest(table: pa.Table) -> str:
    """Hash every expected field in ID order, independent of physical row order."""
    ordered = table.select(FIELDS).sort_by('id')
    result = hashlib.sha256()
    for batch in ordered.to_batches(max_chunksize=8192):
        for row in batch.to_pylist():
            values = [row[field] for field in FIELDS]
            result.update(json.dumps(values, default=str, separators=(',', ':'), ensure_ascii=False).encode())
            result.update(b'\n')
    return result.hexdigest()


def catalog_for(root: Path):
    from pyiceberg.catalog.sql import SqlCatalog

    return SqlCatalog('default', uri=f'sqlite:///{root / "catalog.db"}', warehouse=f'file://{root / "warehouse"}')


def write_iceberg_loader(data: pa.Table, root: Path, scenario: str) -> None:
    from iceberg_loader import LoaderConfig, load_data_to_iceberg

    catalog = catalog_for(root)
    catalog.create_namespace(NAMESPACE)
    config = LoaderConfig(write_mode='append')
    for part in parts(data, scenario):
        load_data_to_iceberg(part, (NAMESPACE, TABLE), catalog, config)


def write_dlt(data: pa.Table, root: Path, scenario: str) -> None:
    # Set catalog configuration before importing dlt. Never use its ephemeral fallback.
    os.environ['PYICEBERG_CATALOG__DEFAULT__TYPE'] = 'sql'
    os.environ['PYICEBERG_CATALOG__DEFAULT__URI'] = f'sqlite:///{root / "catalog.db"}'
    os.environ['PYICEBERG_CATALOG__DEFAULT__WAREHOUSE'] = f'file://{root / "warehouse"}'
    import dlt

    pipeline = dlt.pipeline(
        pipeline_name='iceberg_benchmark',
        destination=dlt.destinations.filesystem(bucket_url=f'file://{root / "warehouse"}'),
        dataset_name=NAMESPACE,
        pipelines_dir=str(root / 'dlt_state'),
    )
    for part in parts(data, scenario):
        pipeline.run(part, table_name=TABLE, table_format='iceberg', write_disposition='append')


def parts(data: pa.Table, scenario: str):
    if scenario == 'initial':
        yield data
    else:
        for index in range(APPENDS):
            start = index * data.num_rows // APPENDS
            stop = (index + 1) * data.num_rows // APPENDS
            yield data.slice(start, stop - start)


def directory_bytes(path: Path) -> int:
    return sum(p.stat().st_size for p in path.rglob('*') if p.is_file())


def run_child(args: argparse.Namespace) -> None:
    root = Path(args.root).resolve()
    root.mkdir(parents=True, exist_ok=True)
    rows = args.rows[0]
    data = make_data(rows)
    before_rss = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    started = time.perf_counter()
    if args.writer == 'iceberg-loader':
        write_iceberg_loader(data, root, args.scenario)
    else:
        write_dlt(data, root, args.scenario)
    elapsed = time.perf_counter() - started
    # macOS reports bytes, Linux KiB.
    peak_rss = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    rss_unit = 1 if sys.platform == 'darwin' else 1024

    iceberg = catalog_for(root).load_table((NAMESPACE, TABLE))
    actual = iceberg.scan().to_arrow()
    if actual.num_rows != data.num_rows:
        raise AssertionError(f'row count: expected {data.num_rows}, got {actual.num_rows}')
    if set(actual.column_names) != set(data.column_names):
        raise AssertionError(f'columns: expected {data.column_names}, got {actual.column_names}')
    if not actual.select(data.column_names).schema.equals(data.schema, check_metadata=False):
        raise AssertionError(f'schema: expected {data.schema}, got {actual.schema}')
    expected_digest = digest(data)
    actual_digest = digest(actual)
    if actual_digest != expected_digest:
        raise AssertionError(f'content checksum mismatch: {expected_digest} != {actual_digest}')

    table_path = Path(iceberg.location().removeprefix('file://'))
    data_files = list((table_path / 'data').rglob('*.parquet'))
    result = {
        'writer': args.writer,
        'scenario': args.scenario,
        'rows': rows,
        'repeat': args.repeat,
        'warmup': args.warmup,
        'seconds': elapsed,
        'peak_rss_delta_bytes': max(0, peak_rss - before_rss) * rss_unit,
        'local_disk_bytes': directory_bytes(root),
        'remote_bytes': 0,
        'table_bytes': directory_bytes(table_path),
        'data_files': len(data_files),
        'snapshots': len(list(iceberg.snapshots())),
        'sha256': actual_digest,
        'pyarrow': pa.__version__,
        'pyiceberg': __import__('importlib.metadata', fromlist=['version']).version('pyiceberg'),
        'dlt': __import__('importlib.metadata', fromlist=['version']).version('dlt') if args.writer == 'dlt' else '',
        'python': platform.python_version(),
        'platform': platform.platform(),
    }
    print('BENCHMARK_RESULT=' + json.dumps(result, sort_keys=True), flush=True)


def run_parent(args: argparse.Namespace) -> None:
    output = Path(args.output).resolve()
    output.mkdir(parents=True, exist_ok=True)
    run_id = uuid.uuid4().hex
    records = []
    for rows in args.rows:
        if rows < APPENDS:
            raise ValueError(f'rows must be at least {APPENDS}')
        for scenario in ('initial', 'append'):
            for repeat in range(-args.warmups, args.repeats):
                # Alternate order on successive rounds to reduce order bias.
                writers = ('iceberg-loader', 'dlt') if repeat % 2 == 0 else ('dlt', 'iceberg-loader')
                for writer in writers:
                    root = output / 'runs' / run_id / f'{rows}_{scenario}_{repeat}_{writer}'
                    command = [
                        sys.executable,
                        str(Path(__file__).resolve()),
                        '--child',
                        '--writer',
                        writer,
                        '--scenario',
                        scenario,
                        '--rows',
                        str(rows),
                        '--root',
                        str(root),
                        '--repeat',
                        str(repeat),
                        '--warmup',
                        str(int(repeat < 0)),
                    ]
                    print(f'Running {writer} {scenario} rows={rows} repeat={repeat}', flush=True)
                    completed = subprocess.run(command, text=True, capture_output=True, check=False)  # noqa: S603
                    if completed.returncode:
                        raise RuntimeError(f'{command}\nstdout:\n{completed.stdout}\nstderr:\n{completed.stderr}')
                    line = next((x for x in completed.stdout.splitlines() if x.startswith('BENCHMARK_RESULT=')), None)
                    if line is None:
                        raise RuntimeError(f'No result from {writer}:\n{completed.stdout}\n{completed.stderr}')
                    record = json.loads(line.partition('=')[2])
                    records.append(record)
                    (output / 'raw.json').write_text(json.dumps(records, indent=2) + '\n')
                    if not args.keep_runs:
                        shutil.rmtree(root)
    measured = [record for record in records if not record['warmup']]
    with (output / 'raw.csv').open('w', newline='') as handle:
        writer = csv.DictWriter(handle, fieldnames=measured[0].keys())
        writer.writeheader()
        writer.writerows(measured)
    summary = []
    for rows in args.rows:
        for scenario in ('initial', 'append'):
            for writer in ('iceberg-loader', 'dlt'):
                group = [r for r in measured if (r['rows'], r['scenario'], r['writer']) == (rows, scenario, writer)]
                summary.append(
                    {
                        'rows': rows,
                        'scenario': scenario,
                        'writer': writer,
                        'repeats': len(group),
                        **{
                            f'median_{key}': statistics.median(r[key] for r in group)
                            for key in (
                                'seconds',
                                'peak_rss_delta_bytes',
                                'local_disk_bytes',
                                'remote_bytes',
                                'table_bytes',
                                'data_files',
                                'snapshots',
                            )
                        },
                    }
                )
    (output / 'summary.json').write_text(json.dumps(summary, indent=2) + '\n')
    if not args.keep_runs:
        (output / 'runs' / run_id).rmdir()
    print(json.dumps(summary, indent=2))


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--rows', nargs='+', type=int, default=[100_000, 1_000_000])
    parser.add_argument('--repeats', type=int, default=5)
    parser.add_argument('--warmups', type=int, default=1)
    parser.add_argument('--output', default='results')
    parser.add_argument('--keep-runs', action='store_true')
    parser.add_argument('--child', action='store_true', help=argparse.SUPPRESS)
    parser.add_argument('--writer', choices=('iceberg-loader', 'dlt'), help=argparse.SUPPRESS)
    parser.add_argument('--scenario', choices=('initial', 'append'), help=argparse.SUPPRESS)
    parser.add_argument('--root', help=argparse.SUPPRESS)
    parser.add_argument('--repeat', type=int, default=0, help=argparse.SUPPRESS)
    parser.add_argument('--warmup', type=int, default=0, help=argparse.SUPPRESS)
    args = parser.parse_args()
    if args.child:
        run_child(args)
    else:
        if args.repeats < 1 or args.warmups < 0:
            parser.error('repeats must be >= 1 and warmups >= 0')
        run_parent(args)


if __name__ == '__main__':
    main()
