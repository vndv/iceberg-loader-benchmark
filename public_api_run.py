"""Download NYC 311 CSV once and compare both writers on the same real records."""

from __future__ import annotations

import argparse
import csv
import hashlib
import io
import json
import os
import platform
import resource
import shutil
import statistics
import subprocess
import sys
import tempfile
import time
import uuid
from contextlib import suppress
from pathlib import Path

import pyarrow as pa
import pyarrow.csv as pacsv
import requests
from rest_run import catalog, cleanup, dlt_pipeline, namespace_path, remote_files, s3_filesystem, settings
from run import TABLE, directory_bytes

SOURCE_URL = 'https://data.cityofnewyork.us/api/views/erm2-nwe9/rows.csv?accessType=DOWNLOAD'
MIB = 1024 * 1024
MAX_SOURCE_BYTES = 10_000_000_000


def sha256_file(path: Path) -> str:
    result = hashlib.sha256()
    with path.open('rb') as handle:
        for block in iter(lambda: handle.read(8 * MIB), b''):
            result.update(block)
    return result.hexdigest()


def csv_schema(path: Path) -> pa.Schema:
    with path.open(newline='', encoding='utf-8-sig') as handle:
        names = next(csv.reader(handle))
    if not names or names[0] != 'unique_key' or len(names) != len(set(names)):
        raise ValueError('Expected NYC 311 CSV with unique_key and distinct column names')
    return pa.schema([pa.field(name, pa.string()) for name in names])


def iter_tables(path: Path, schema: pa.Schema, chunk_bytes: int):
    options = pacsv.ConvertOptions(
        column_types={field.name: pa.string() for field in schema},
        strings_can_be_null=True,
    )
    with path.open('rb') as source:
        reader = pacsv.open_csv(
            source,
            read_options=pacsv.ReadOptions(block_size=8 * MIB),
            convert_options=options,
        )
        batches = []
        size = 0
        for batch in reader:
            batches.append(batch)
            size += batch.nbytes
            if size >= chunk_bytes:
                yield pa.Table.from_batches(batches)
                batches = []
                size = 0
        if batches:
            yield pa.Table.from_batches(batches)


def source_snapshot(sizes: list[int], directory: Path) -> tuple[dict[int, Path], dict]:
    """Stream a public CSV export into complete-row fixtures capped at requested sizes."""
    csv.field_size_limit(10 * MIB)
    if not sizes or min(sizes) <= 0 or max(sizes) > MAX_SOURCE_BYTES:
        raise ValueError('Source sizes must be greater than zero and at most 10 GB')
    paths = {size: directory / f'nyc311_{size}.csv' for size in sizes}
    files = {size: path.open('w', encoding='utf-8', newline='') for size, path in paths.items()}
    writers = {size: csv.writer(handle) for size, handle in files.items()}
    counts = dict.fromkeys(sizes, 0)
    next_report = 100 * MIB
    headers = {'Accept-Encoding': 'gzip', 'User-Agent': 'iceberg-loader-benchmark/0.1'}
    if token := os.getenv('SOCRATA_APP_TOKEN'):
        headers['X-App-Token'] = token
    response_meta = {}
    try:
        with requests.get(SOURCE_URL, headers=headers, stream=True, timeout=(20, 180)) as response:
            response.raise_for_status()
            response.raw.decode_content = True
            response_meta = {
                'url': SOURCE_URL,
                'etag': response.headers.get('ETag'),
                'last_modified': response.headers.get('Last-Modified'),
                'truth_last_modified': response.headers.get('X-SODA2-Truth-Last-Modified'),
            }
            reader = csv.reader(io.TextIOWrapper(response.raw, encoding='utf-8-sig', newline=''))
            header = next(reader)
            if not header or header[0] != 'Unique Key':
                raise ValueError(f'Unexpected NYC 311 export header: {header[:3]}')
            # The export uses display labels; normalize to stable API field names.
            api_names = [name.strip().lower().replace(' ', '_').replace('(', '').replace(')', '') for name in header]
            api_names[0] = 'unique_key'
            if len(api_names) != len(set(api_names)):
                raise ValueError('Duplicate normalized column names in source export')
            for writer in writers.values():
                writer.writerow(api_names)
            for row in reader:
                if len(row) != len(header):
                    raise ValueError(f'CSV field count mismatch: {len(row)} != {len(header)}')
                for size in list(writers):
                    handle = files[size]
                    before = handle.tell()
                    writers[size].writerow(row)
                    if handle.tell() > size:
                        handle.seek(before)
                        handle.truncate()
                        handle.close()
                        del writers[size]
                    else:
                        counts[size] += 1
                largest_file = files[max(sizes)]
                progress = largest_file.tell() if not largest_file.closed else paths[max(sizes)].stat().st_size
                if progress >= next_report:
                    print(f'Downloaded {progress / MIB:.0f} MiB of {max(sizes) / MIB:.0f} MiB', flush=True)
                    next_report += 100 * MIB
                if not writers:
                    break
    finally:
        for handle in files.values():
            if not handle.closed:
                handle.close()
    details = {
        'source': response_meta,
        'schema': api_names,
        'fixtures': {
            str(size): {'bytes': paths[size].stat().st_size, 'rows': counts[size], 'sha256': sha256_file(paths[size])}
            for size in sizes
        },
    }
    if any(counts[size] == 0 for size in sizes):
        raise ValueError('The API export did not provide enough rows')
    return paths, details


def write_loader(
    path: Path, schema: pa.Schema, namespace: str, chunk_bytes: int, config: dict[str, str]
) -> tuple[int, int]:
    from iceberg_loader import LoaderConfig, load_data_to_iceberg

    cat = catalog(config)
    cat.create_namespace(namespace)
    rows = chunks = 0
    for table in iter_tables(path, schema, chunk_bytes):
        batch_count = len(table.to_batches())
        load_data_to_iceberg(
            table,
            (namespace, TABLE),
            cat,
            LoaderConfig(write_mode='append', commit_interval=batch_count),
        )
        rows += table.num_rows
        chunks += 1
    return rows, chunks


def write_dlt_chunks(
    path: Path, schema: pa.Schema, root: Path, namespace: str, chunk_bytes: int, config: dict[str, str]
) -> tuple[int, int]:
    pipeline = dlt_pipeline(root, namespace, config)
    rows = chunks = 0
    for table in iter_tables(path, schema, chunk_bytes):
        pipeline.run(table, table_name=TABLE, table_format='iceberg', write_disposition='append')
        rows += table.num_rows
        chunks += 1
    return rows, chunks


def child(args: argparse.Namespace) -> None:
    config = settings()
    source = Path(args.source)
    root = Path(args.root)
    root.mkdir(parents=True, exist_ok=False)
    schema = csv_schema(source)
    actual_source_sha256 = sha256_file(source)
    if actual_source_sha256 != args.source_sha256:
        raise AssertionError(f'Source SHA-256 mismatch: {actual_source_sha256} != {args.source_sha256}')
    chunk_bytes = args.chunk_mib * MIB
    before_rss = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    started = time.perf_counter()
    if args.writer == 'iceberg-loader':
        rows, chunks = write_loader(source, schema, args.namespace, chunk_bytes, config)
    else:
        rows, chunks = write_dlt_chunks(source, schema, root, args.namespace, chunk_bytes, config)
    seconds = time.perf_counter() - started
    peak_rss = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss

    table = catalog(config).load_table((args.namespace, TABLE))
    if table.schema().column_names != schema.names:
        raise AssertionError('Iceberg schema columns differ from CSV source')
    iceberg_types = [str(field.field_type) for field in table.schema().fields]
    if iceberg_types != ['string'] * len(schema):
        raise AssertionError(f'Unexpected Iceberg column types: {iceberg_types}')
    planned = list(table.scan().plan_files())
    recorded_rows = sum(task.file.record_count for task in planned)
    if recorded_rows != rows:
        raise AssertionError(f'Iceberg row count {recorded_rows} != CSV row count {rows}')
    sample = table.scan(limit=min(rows, 1000)).to_arrow()
    if sample.num_rows != min(rows, 1000) or not sample.schema.equals(schema, check_metadata=False):
        raise AssertionError('Iceberg read-back sample differs in row count or schema')
    location = table.location()
    expected_prefix = namespace_path(config, args.namespace) + '/'
    if not location.startswith(f's3://{expected_prefix}'):
        raise AssertionError(f'Table outside benchmark namespace: {location}')
    fs = s3_filesystem(config)
    all_remote = remote_files(fs, namespace_path(config, args.namespace))
    table_path = location.removeprefix('s3://')
    table_files = [item for item in all_remote if item.path.startswith(table_path + '/')]
    if not table_files:
        raise AssertionError('No remote Iceberg table files found')
    rss_unit = 1 if sys.platform == 'darwin' else 1024
    result = {
        'target_source_bytes': args.target_source_bytes,
        'source_bytes': source.stat().st_size,
        'source_sha256': args.source_sha256,
        'writer': args.writer,
        'repeat': args.repeat,
        'warmup': bool(args.warmup),
        'rows': rows,
        'chunks': chunks,
        'seconds': seconds,
        'peak_rss_delta_bytes': max(0, peak_rss - before_rss) * rss_unit,
        'local_disk_bytes': directory_bytes(root),
        'remote_bytes': sum(item.size for item in all_remote),
        'table_bytes': sum(item.size for item in table_files),
        'data_files': sum(item.path.endswith('.parquet') and '/data/' in item.path for item in table_files),
        'snapshots': len(list(table.snapshots())),
        'python': platform.python_version(),
        'platform': platform.platform(),
        'pyarrow': pa.__version__,
        'pyiceberg': __import__('importlib.metadata', fromlist=['version']).version('pyiceberg'),
        'dlt': __import__('importlib.metadata', fromlist=['version']).version('dlt') if args.writer == 'dlt' else '',
    }
    print('BENCHMARK_RESULT=' + json.dumps(result, sort_keys=True), flush=True)


def parent(args: argparse.Namespace) -> None:
    config = settings()
    catalog(config).list_namespaces()
    output = Path(args.output).resolve()
    output.mkdir(parents=True, exist_ok=True)
    run_id = uuid.uuid4().hex[:12]
    records = []
    with tempfile.TemporaryDirectory(prefix='iceberg_public_api_', dir=output) as temporary:
        temp = Path(temporary)
        if args.source_file:
            source = Path(args.source_file).resolve()
            with source.open(newline='', encoding='utf-8-sig') as handle:
                header = next(csv.reader(handle))
            api_names = [name.strip().lower().replace(' ', '_').replace('(', '').replace(')', '') for name in header]
            api_names[0] = 'unique_key'
            sources = {source.stat().st_size: source}
            details = {
                'source': {'url': 'user-supplied CSV'},
                'schema': api_names,
                'fixtures': {
                    str(source.stat().st_size): {'bytes': source.stat().st_size, 'sha256': sha256_file(source)}
                },
            }
        else:
            sizes = (
                sorted({int(size * 1_000_000_000) for size in args.sizes_gb})
                if args.sizes_gb is not None
                else sorted({size * MIB for size in args.sizes_mib})
            )
            if shutil.disk_usage(output).free < 2 * sum(sizes):
                raise RuntimeError('Insufficient free disk space for source snapshots and benchmark state')
            sources, details = source_snapshot(sizes, temp)
        (output / 'source.json').write_text(json.dumps(details, indent=2) + '\n')
        for size, source in sources.items():
            for repeat in range(-args.warmups, args.repeats):
                writers = ('iceberg-loader', 'dlt') if repeat % 2 == 0 else ('dlt', 'iceberg-loader')
                for writer in writers:
                    namespace = f'bench_{run_id}_{size}_{repeat + args.warmups}_{writer.replace("-", "_")}'
                    root = output / 'runs' / namespace
                    command = [
                        sys.executable,
                        str(Path(__file__).resolve()),
                        '--child',
                        '--source',
                        str(source),
                        '--source-sha256',
                        details['fixtures'][str(size)]['sha256'],
                        '--target-source-bytes',
                        str(size),
                        '--writer',
                        writer,
                        '--root',
                        str(root),
                        '--namespace',
                        namespace,
                        '--chunk-mib',
                        str(args.chunk_mib),
                        '--repeat',
                        str(repeat),
                        '--warmup',
                        str(int(repeat < 0)),
                    ]
                    print(f'Running {writer} source={size / MIB:.0f} MiB repeat={repeat}', flush=True)
                    try:
                        completed = subprocess.run(command, text=True, capture_output=True, check=False)  # noqa: S603
                        if completed.returncode:
                            raise RuntimeError(f'{writer} failed:\n{completed.stderr[-5000:]}')
                        line = next(
                            line for line in completed.stdout.splitlines() if line.startswith('BENCHMARK_RESULT=')
                        )
                        records.append(json.loads(line.partition('=')[2]))
                        (output / 'raw.json').write_text(json.dumps(records, indent=2) + '\n')
                    finally:
                        try:
                            cleanup(config, namespace)
                        finally:
                            if root.exists():
                                shutil.rmtree(root)
    measured = [record for record in records if not record['warmup']]
    summary = []
    for size in sources:
        for writer in ('iceberg-loader', 'dlt'):
            group = [r for r in measured if r['target_source_bytes'] == size and r['writer'] == writer]
            summary.append(
                {
                    'target_source_bytes': size,
                    'source_bytes': group[0]['source_bytes'],
                    'writer': writer,
                    'repeats': len(group),
                    'rows': group[0]['rows'],
                    'chunks': group[0]['chunks'],
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
    with suppress(OSError):
        (output / 'runs').rmdir()
    print(json.dumps(summary, indent=2))


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    sizes = parser.add_mutually_exclusive_group()
    sizes.add_argument('--sizes-mib', nargs='+', type=int, default=[100])
    sizes.add_argument('--sizes-gb', nargs='+', type=float)
    parser.add_argument('--chunk-mib', type=int, default=256)
    parser.add_argument('--repeats', type=int, default=1)
    parser.add_argument('--warmups', type=int, default=0)
    parser.add_argument('--output', default='results/public_api')
    parser.add_argument('--source-file', help='Use an existing NYC 311 CSV instead of downloading it')
    parser.add_argument('--child', action='store_true', help=argparse.SUPPRESS)
    parser.add_argument('--source', help=argparse.SUPPRESS)
    parser.add_argument('--source-sha256', help=argparse.SUPPRESS)
    parser.add_argument('--target-source-bytes', type=int, help=argparse.SUPPRESS)
    parser.add_argument('--writer', choices=('iceberg-loader', 'dlt'), help=argparse.SUPPRESS)
    parser.add_argument('--root', help=argparse.SUPPRESS)
    parser.add_argument('--namespace', help=argparse.SUPPRESS)
    parser.add_argument('--repeat', type=int, default=0, help=argparse.SUPPRESS)
    parser.add_argument('--warmup', type=int, default=0, help=argparse.SUPPRESS)
    args = parser.parse_args()
    if args.child:
        child(args)
    else:
        requested_sizes = args.sizes_gb if args.sizes_gb is not None else args.sizes_mib
        if args.repeats < 1 or args.warmups < 0 or args.chunk_mib < 1 or any(size <= 0 for size in requested_sizes):
            parser.error('repeats and chunk size must be positive; warmups cannot be negative')
        parent(args)


if __name__ == '__main__':
    main()
