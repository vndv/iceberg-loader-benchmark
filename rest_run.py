"""Benchmark both writers against a live REST catalog and S3-compatible storage."""

from __future__ import annotations

import argparse
import csv
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
from contextlib import suppress
from pathlib import Path
from urllib.parse import urlparse

import pyarrow as pa
import pyarrow.fs as pafs
from run import APPENDS, TABLE, digest, directory_bytes, make_data, parts


# Non-secret local development defaults matching examples/docker-compose.yml; real deployments should override via environment variables.
def settings() -> dict[str, str]:
    rest_uri = os.getenv('ICEBERG_REST_URI', 'http://localhost:8181/api/catalog')
    return {
        'rest_uri': rest_uri,
        'warehouse': os.getenv('ICEBERG_WAREHOUSE', 'datalake'),
        'credential': os.getenv('ICEBERG_CREDENTIAL', 'root:root'),
        'oauth_uri': os.getenv('ICEBERG_OAUTH2_SERVER_URI', f'{rest_uri}/v1/oauth/tokens'),
        's3_endpoint': os.getenv('S3_ENDPOINT', 'http://localhost:9000'),
        's3_access_key': os.getenv('S3_ACCESS_KEY', 'minio'),
        's3_secret_key': os.getenv('S3_SECRET_KEY', 'minio123'),
        's3_region': os.getenv('S3_REGION', 'us-east-1'),
        's3_root': os.getenv('BENCHMARK_S3_ROOT', 's3://datalake/warehouse'),
    }


def catalog(config: dict[str, str]):
    from iceberg_loader import get_rest_catalog

    return get_rest_catalog(
        uri=config['rest_uri'],
        warehouse=config['warehouse'],
        credential=config['credential'],
        s3_endpoint=config['s3_endpoint'],
        s3_access_key=config['s3_access_key'],
        s3_secret_key=config['s3_secret_key'],
        s3_region=config['s3_region'],
        extra_properties={
            'scope': 'PRINCIPAL_ROLE:ALL',
            'oauth2-server-uri': config['oauth_uri'],
            'header.X-Iceberg-Access-Delegation': '',
        },
    )


def s3_filesystem(config: dict[str, str]) -> pafs.S3FileSystem:
    endpoint = urlparse(config['s3_endpoint'])
    return pafs.S3FileSystem(
        access_key=config['s3_access_key'],
        secret_key=config['s3_secret_key'],
        region=config['s3_region'],
        scheme=endpoint.scheme,
        endpoint_override=endpoint.netloc,
    )


def namespace_path(config: dict[str, str], namespace: str) -> str:
    root = urlparse(config['s3_root'])
    if root.scheme != 's3' or not root.netloc or not root.path.strip('/'):
        raise ValueError('BENCHMARK_S3_ROOT must be s3://BUCKET/NONEMPTY_PREFIX')
    if not namespace.startswith('bench_') or not namespace.replace('_', '').isalnum():
        raise ValueError(f'Unsafe benchmark namespace: {namespace}')
    return f'{root.netloc}/{root.path.strip("/")}/{namespace}'


def remote_files(fs: pafs.S3FileSystem, path: str) -> list[pafs.FileInfo]:
    return [
        item
        for item in fs.get_file_info(pafs.FileSelector(path, recursive=True, allow_not_found=True))
        if item.type == pafs.FileType.File
    ]


def configure_dlt_catalog(config: dict[str, str]) -> None:
    values = {
        'TYPE': 'rest',
        'URI': config['rest_uri'],
        'WAREHOUSE': config['warehouse'],
        'CREDENTIAL': config['credential'],
        'SCOPE': 'PRINCIPAL_ROLE:ALL',
        'OAUTH2_SERVER_URI': config['oauth_uri'],
        'HEADER_X_ICEBERG_ACCESS_DELEGATION': '',
        'S3_ENDPOINT': config['s3_endpoint'],
        'S3_ACCESS_KEY_ID': config['s3_access_key'],
        'S3_SECRET_ACCESS_KEY': config['s3_secret_key'],
        'S3_REGION': config['s3_region'],
        'S3_PATH_STYLE_ACCESS': 'true',
    }
    for key, value in values.items():
        os.environ[f'PYICEBERG_CATALOG__DEFAULT__{key}'] = value
    os.environ['ICEBERG_CATALOG__ICEBERG_CATALOG_TYPE'] = 'rest'


def dlt_pipeline(root: Path, namespace: str, config: dict[str, str]):
    configure_dlt_catalog(config)
    import dlt

    dlt.config['iceberg_catalog.iceberg_catalog_config'] = {
        'type': 'rest',
        'uri': config['rest_uri'],
        'warehouse': config['warehouse'],
        'credential': config['credential'],
        'scope': 'PRINCIPAL_ROLE:ALL',
        'oauth2-server-uri': config['oauth_uri'],
        'header.X-Iceberg-Access-Delegation': '',
        's3.endpoint': config['s3_endpoint'],
        's3.access-key-id': config['s3_access_key'],
        's3.secret-access-key': config['s3_secret_key'],
        's3.path-style-access': 'true',
        's3.region': config['s3_region'],
    }

    credentials = {
        'aws_access_key_id': config['s3_access_key'],
        'aws_secret_access_key': config['s3_secret_key'],
        'region_name': config['s3_region'],
        'endpoint_url': config['s3_endpoint'],
    }
    return dlt.pipeline(
        pipeline_name='iceberg_rest_benchmark',
        destination=dlt.destinations.filesystem(bucket_url=config['s3_root'], credentials=credentials),
        dataset_name=namespace,
        pipelines_dir=str(root / 'dlt_state'),
    )


def write_dlt(data: pa.Table, root: Path, namespace: str, scenario: str, config: dict[str, str]) -> None:
    pipeline = dlt_pipeline(root, namespace, config)
    for part in parts(data, scenario):
        pipeline.run(part, table_name=TABLE, table_format='iceberg', write_disposition='append')


def write_loader(data: pa.Table, namespace: str, scenario: str, config: dict[str, str]) -> None:
    from iceberg_loader import LoaderConfig, load_data_to_iceberg

    cat = catalog(config)
    cat.create_namespace(namespace)
    for part in parts(data, scenario):
        load_data_to_iceberg(part, (namespace, TABLE), cat, LoaderConfig(write_mode='append'))


def run_child(args: argparse.Namespace) -> None:
    config = settings()
    root = Path(args.root)
    root.mkdir(parents=True, exist_ok=False)
    rows = args.rows[0]
    data = make_data(rows)
    before_rss = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    started = time.perf_counter()
    if args.writer == 'iceberg-loader':
        write_loader(data, args.namespace, args.scenario, config)
    else:
        write_dlt(data, root, args.namespace, args.scenario, config)
    seconds = time.perf_counter() - started
    peak_rss = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    rss_unit = 1 if sys.platform == 'darwin' else 1024

    table = catalog(config).load_table((args.namespace, TABLE))
    expected_location = namespace_path(config, args.namespace) + '/'
    location = urlparse(table.location())
    if location.scheme != 's3' or not f'{location.netloc}{location.path}'.startswith(expected_location):
        raise AssertionError(f'Table outside benchmark namespace: {table.location()}')
    actual = table.scan().to_arrow()
    if actual.num_rows != data.num_rows or not actual.schema.equals(data.schema, check_metadata=False):
        raise AssertionError(f'Row count or schema mismatch: {actual.num_rows}, {actual.schema}')
    checksum = digest(actual)
    if checksum != digest(data):
        raise AssertionError('Full-content SHA-256 mismatch')

    fs = s3_filesystem(config)
    all_remote = remote_files(fs, namespace_path(config, args.namespace))
    table_prefix = f'{location.netloc}{location.path}'
    table_files = [f for f in all_remote if f.path.startswith(table_prefix + '/')]
    result = {
        'catalog': 'REST (Polaris)',
        'writer': args.writer,
        'scenario': args.scenario,
        'rows': rows,
        'repeat': args.repeat,
        'warmup': bool(args.warmup),
        'seconds': seconds,
        'peak_rss_delta_bytes': max(0, peak_rss - before_rss) * rss_unit,
        'local_disk_bytes': directory_bytes(root),
        'remote_bytes': sum(f.size for f in all_remote),
        'table_bytes': sum(f.size for f in table_files),
        'data_files': sum(f.path.endswith('.parquet') and '/data/' in f.path for f in table_files),
        'snapshots': len(list(table.snapshots())),
        'sha256': checksum,
        'python': platform.python_version(),
        'platform': platform.platform(),
        'pyarrow': pa.__version__,
        'pyiceberg': __import__('importlib.metadata', fromlist=['version']).version('pyiceberg'),
        'dlt': __import__('importlib.metadata', fromlist=['version']).version('dlt') if args.writer == 'dlt' else '',
    }
    print('BENCHMARK_RESULT=' + json.dumps(result, sort_keys=True), flush=True)


def cleanup(config: dict[str, str], namespace: str) -> None:
    """Delete only the uniquely generated namespace and its S3 prefix."""
    from pyiceberg.exceptions import NoSuchNamespaceError, NoSuchTableError

    path = namespace_path(config, namespace)
    cat = catalog(config)
    with suppress(NoSuchTableError):
        cat.drop_table((namespace, TABLE))
    with suppress(NoSuchNamespaceError):
        cat.drop_namespace(namespace)
    fs = s3_filesystem(config)
    if remote_files(fs, path):
        fs.delete_dir(path)
    if remote_files(fs, path):
        raise RuntimeError(f'S3 cleanup incomplete for {path}')
    if cat.namespace_exists(namespace):
        raise RuntimeError(f'Catalog cleanup incomplete for {namespace}')


def run_parent(args: argparse.Namespace) -> None:
    config = settings()
    # Preflight before any data is written.
    cat = catalog(config)
    cat.list_namespaces()
    fs = s3_filesystem(config)
    fs.get_file_info(namespace_path(config, 'bench_preflight'))
    output = Path(args.output).resolve()
    output.mkdir(parents=True, exist_ok=True)
    run_id = uuid.uuid4().hex[:12]
    records = []
    for rows in args.rows:
        if rows < APPENDS:
            raise ValueError(f'rows must be at least {APPENDS}')
        for scenario in ('initial', 'append'):
            for repeat in range(-args.warmups, args.repeats):
                writers = ('iceberg-loader', 'dlt') if repeat % 2 == 0 else ('dlt', 'iceberg-loader')
                for writer in writers:
                    namespace = f'bench_{run_id}_{rows}_{scenario}_{repeat + args.warmups}_{writer.replace("-", "_")}'
                    root = output / 'runs' / namespace
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
                        '--namespace',
                        namespace,
                        '--repeat',
                        str(repeat),
                        '--warmup',
                        str(int(repeat < 0)),
                    ]
                    print(f'Running {writer} {scenario} rows={rows} repeat={repeat}', flush=True)
                    try:
                        completed = subprocess.run(command, text=True, capture_output=True, check=False)  # noqa: S603
                        if completed.returncode:
                            raise RuntimeError(f'{writer} failed:\n{completed.stderr[-5000:]}')
                        line = next(x for x in completed.stdout.splitlines() if x.startswith('BENCHMARK_RESULT='))
                        records.append(json.loads(line.partition('=')[2]))
                        (output / 'raw.json').write_text(json.dumps(records, indent=2) + '\n')
                    finally:
                        try:
                            cleanup(config, namespace)
                        finally:
                            if root.exists():
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
    with suppress(OSError):
        (output / 'runs').rmdir()
    print(json.dumps(summary, indent=2))


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--rows', nargs='+', type=int, default=[100_000, 1_000_000])
    parser.add_argument('--repeats', type=int, default=5)
    parser.add_argument('--warmups', type=int, default=1)
    parser.add_argument('--output', default='results/rest')
    parser.add_argument('--child', action='store_true', help=argparse.SUPPRESS)
    parser.add_argument('--writer', choices=('iceberg-loader', 'dlt'), help=argparse.SUPPRESS)
    parser.add_argument('--scenario', choices=('initial', 'append'), help=argparse.SUPPRESS)
    parser.add_argument('--root', help=argparse.SUPPRESS)
    parser.add_argument('--namespace', help=argparse.SUPPRESS)
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
