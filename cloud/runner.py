#!/usr/bin/env python3
"""Optional course Polars Cloud backend using identical t8i.small instances. Creates paid compute.

Use --prepare-only to stage/upload public text without starting compute.
"""
import argparse
from concurrent.futures import ThreadPoolExecutor
import csv
from dataclasses import asdict
from datetime import datetime, timezone
import fcntl
import hashlib
import json
import os
from pathlib import Path
import statistics
import subprocess
import sys
import time
import traceback
from uuid import UUID

ROOT = Path(__file__).resolve().parent
BASE = ROOT.parent
sys.path.insert(0, str(BASE))
if sys.platform == 'darwin':
    os.environ.setdefault('SSL_CERT_FILE', '/etc/ssl/cert.pem')
os.environ.setdefault('POLARS_CLOUD_CONFIG_DIR', str(BASE / '.auth'))
os.environ.setdefault('POLARS_TIMEOUT_MS', '3600000')

from src.polars_query import wordcount
from cloud.validation import file_sha, canonicalize
from infrastructure.preflight import inspect, verify_cloud_hardware
sys.path.insert(0, str(BASE / 'infrastructure'))
from credit_metrics import collect as collect_credit_metrics


def utc():
    return datetime.now(timezone.utc).isoformat()


def write_json(path, data):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    temp = path.with_suffix(path.suffix + '.tmp')
    temp.write_text(json.dumps(data, ensure_ascii=False, indent=2, default=str) + '\n')
    temp.replace(path)


def read_json(path):
    return json.loads(Path(path).read_text())


def aws(args, operation, **parameters):
    command = ['aws', '--region', args.region, '--output', 'json']
    if args.profile:
        command += ['--profile', args.profile]
    command += operation
    for key, value in parameters.items():
        command += ['--' + key.replace('_', '-'), json.dumps(value) if isinstance(value, (list, dict)) else str(value)]
    proc = subprocess.run(command, check=True, capture_output=True, text=True,
        env=dict(os.environ, AWS_PAGER='', AWS_CLI_AUTO_PROMPT='off'))
    return json.loads(proc.stdout) if proc.stdout.strip() else {}


def stage_text(source, destination, parts=64):
    """Contiguous plain-text shards; concatenation is byte-identical to source."""
    source = Path(source)
    destination = Path(destination)
    checksum = file_sha(source)
    if (destination / 'manifest.json').exists():
        manifest = read_json(destination / 'manifest.json')
        assert manifest['source_sha256'] == checksum and manifest['parts_requested'] == parts
        for item in manifest['files']:
            assert file_sha(destination / item['name']) == item['sha256']
        return manifest
    destination.mkdir(parents=True, exist_ok=False)
    target_bytes = max(1, (source.stat().st_size + parts - 1) // parts)
    output = None
    files = []
    joined = hashlib.sha256()
    try:
        with source.open('rb') as fp:
            for line in fp:
                if output is None or (output.tell() >= target_bytes and len(files) < parts):
                    if output:
                        output.close()
                    path = destination / ('part-%03d.txt' % len(files))
                    files.append(path)
                    output = path.open('xb')
                output.write(line)
                joined.update(line)
    finally:
        if output:
            output.close()
    assert joined.hexdigest() == checksum
    manifest = {'source': str(source), 'source_bytes': source.stat().st_size,
        'source_sha256': checksum, 'parts_requested': parts,
        'format': 'unmodified UTF-8 line records; contiguous chunks; no compression',
        'files': [{'name': p.name, 'bytes': p.stat().st_size, 'sha256': file_sha(p)} for p in files]}
    write_json(destination / 'manifest.json', manifest)
    return manifest


def upload(args, staging, manifest):
    prefix = 'course-wordcount/%s/input64-v1/' % manifest['source_sha256']
    uri = 's3://' + args.bucket + '/' + prefix
    t0 = time.perf_counter()
    command = ['aws', '--region', args.region]
    if args.profile:
        command += ['--profile', args.profile]
    command += ['s3', 'sync', str(staging), uri, '--exclude', '*', '--include', '*.txt', '--only-show-errors']
    subprocess.run(command, check=True, env=dict(os.environ, AWS_PAGER='', AWS_CLI_AUTO_PROMPT='off'))
    objects = aws(args, ['s3api', 'list-objects-v2'], bucket=args.bucket, prefix=prefix).get('Contents', [])
    assert {Path(o['Key']).name: o['Size'] for o in objects} == {f['name']: f['bytes'] for f in manifest['files']}
    return {'uri': uri + '*.txt', 'upload_and_size_check_seconds': time.perf_counter() - t0,
            'objects': len(objects), 'source_sha256': manifest['source_sha256']}


def metadata(pc, ctx):
    from polars_cloud import constants
    model = constants.API_CLIENT.get_compute_cluster(ctx.workspace.id, ctx._compute_id)
    keys = ['id', 'workspace_id', 'instance_type', 'cluster_size', 'vcpus', 'ram_mib',
            'req_storage', 'big_instance_type', 'compute_plane_version', 'status']
    value = {key: getattr(model, key, None) for key in keys}
    nodes = constants.API_CLIENT.get_compute_cluster_nodes(ctx.workspace.id, ctx._compute_id)
    value['nodes'] = [{key: getattr(node, key, None) for key in
                       ['private_address', 'cpus', 'memory_mb', 'storage_mb']} for node in nodes]
    return value


def remote_status(pc, workspace, compute_id):
    # list() fetches server state; get_status() caches STOPPED immediately after stop().
    for ctx, status in pc.ComputeContext.list(workspace):
        if str(ctx._compute_id) == str(compute_id):
            return status
    raise RuntimeError('Compute missing from workspace listing: ' + str(compute_id))


def wait_ec2_release(args, compute_id):
    """Cloud STOPPED can precede EC2 termination and quota release."""
    deadline = time.monotonic() + 600
    while True:
        response = aws(args, ['ec2', 'describe-instances'], filters=[
            {'Name': 'tag:PolarsClusterId', 'Values': [str(compute_id)]},
            {'Name': 'instance-state-name', 'Values': ['pending', 'running', 'stopping', 'stopped', 'shutting-down']}])
        instances = [i for r in response['Reservations'] for i in r['Instances']]
        if not instances:
            return {'ec2_release_confirmed': True, 'verified_utc': utc()}
        if time.monotonic() >= deadline:
            raise TimeoutError('EC2 instances still exist for ' + str(compute_id))
        print('Waiting for %d EC2 instances to terminate and release quota...' % len(instances), flush=True)
        time.sleep(10)


def details(pc, query):
    """Best-effort observatory metrics via the installed official SDK's client.

    total_node_time_ns is NOT CPU time. Never substitute it for process CPU.
    """
    try:
        inner = query._client.get_query_details(query._query_id, query._cluster._get_token())
        data = asdict(pc.QueryDetail._from_inner(inner))
        data.pop('user_name', None)
        return data
    except Exception as exc:
        return {'unavailable': repr(exc)}


def execute_round(pc, ctx, args, n, repetition, source_uri, output, expected):
    output.mkdir()
    run_id = '%dn-r%02d' % (n, repetition)
    prefix = 'course-wordcount/results/' + args.experiment + '/' + run_id + '/'
    uri = 's3://' + args.bucket + '/' + prefix
    record = {'workers_requested': n, 'run_id': run_id, 'started_utc': utc(), 'status': 'running',
              'input_uri': source_uri, 'output_uri': uri, 'n_retries': 0,
              'cpu_seconds': None, 'peak_rss_bytes': None,
              'resource_metric_note': 'SDK stage statistics do not expose task CPU/RSS; unavailable is not zero.'}
    write_json(output / 'result.json', record)
    query = None
    try:
        plan = wordcount(source_uri)
        start = time.perf_counter()
        query = (plan.remote(context=ctx, n_retries=0, engine='streaming')
                 .distributed(min_workers=n, max_workers=n, single_worker_ops='forbid')
                 .sink_parquet(uri))
        record['submit_seconds'] = time.perf_counter() - start
        record['query_id'] = str(query._query_id)
        record['dashboard_url'] = 'https://cloud.pola.rs/portal/%s/%s/queries/%s' % (
            args.organization_id, args.workspace, record['query_id'])
        write_json(output / 'result.json', record)
        result = query.await_result(silent=True)
        record['query_wall_seconds'] = time.perf_counter() - start
        record['stage_workers'] = [s.num_workers_used for s in result.stage_statistics()]
        record['max_stage_workers'] = max(record['stage_workers'], default=0)
        record['failed_stages'] = result.failed_stages
        record['output_rows_reported'] = result.n_rows_total
        if record['max_stage_workers'] != n:
            raise RuntimeError('Requested %s workers but maximum observed stage used %s' % (n, record['max_stage_workers']))
        if record['failed_stages']:
            raise RuntimeError('Query had failed/rescheduled stages; exclude from clean benchmark timings')
        for kind in ['ir', 'physical']:
            try:
                (output / (kind + '-plan.txt')).write_text(result.plan(kind))
            except Exception as exc:
                record[kind + '_plan_error'] = repr(exc)
        write_json(output / 'query-detail.json', details(pc, query))
        download_start = time.perf_counter()
        local = output / 'parquet'
        command = ['aws', '--region', args.region]
        if args.profile:
            command += ['--profile', args.profile]
        command += ['s3', 'sync', uri, str(local), '--only-show-errors']
        subprocess.run(command, check=True, env=dict(os.environ, AWS_PAGER=''))
        record['download_seconds'] = time.perf_counter() - download_start
        import polars as pl
        frame = pl.read_parquet(str(local / '*.parquet'))
        t0 = time.perf_counter()
        record['validation'] = canonicalize(frame, output / 'wordcounts.jsonl', expected['expected_sha256'])
        record['validation_seconds'] = time.perf_counter() - t0
        record['throughput_mib_s'] = expected['input_bytes'] / 2**20 / record['query_wall_seconds']
        record['status'] = 'success'
    except BaseException as exc:
        record.update(status='failed', error=repr(exc), traceback=traceback.format_exc())
        if query:
            try:
                write_json(output / 'query-detail.json', details(pc, query))
                query.cancel()
            except Exception:
                pass
        raise
    finally:
        record['finished_utc'] = utc()
        write_json(output / 'result.json', record)
    return record


def run_scale(pc, workspace, args, n, source_uri, output, expected):
    output.mkdir()
    context = pc.ComputeContext(workspace=workspace, instance_type=args.course['instance_type'], storage=args.course['disk_gib'],
                               cluster_size=n, idle_timeout_mins=10)
    lifecycle = {'workers': n, 'status': 'starting', 'started_utc': utc()}
    write_json(output / 'lifecycle.json', lifecycle)
    try:
        t0 = time.perf_counter()
        context.start(wait=True)
        lifecycle['startup_seconds'] = time.perf_counter() - t0
        lifecycle['compute_id'] = str(context._compute_id)
        lifecycle['status'] = 'running'
        write_json(output / 'lifecycle.json', lifecycle)
        deadline = time.monotonic() + 300
        config = metadata(pc, context)
        while len(config['nodes']) < n and time.monotonic() < deadline:
            print('Waiting for worker registration: %d/%d' % (len(config['nodes']), n), flush=True)
            time.sleep(5)
            config = metadata(pc, context)
        lifecycle['workers_ready_seconds'] = time.perf_counter() - t0
        write_json(output / 'compute.json', config)
        assert config['cluster_size'] == n and config['instance_type'] == args.course['instance_type']
        assert config['vcpus'] == args.vcpus * n and config['ram_mib'] == args.memory_mib * n, config
        assert len(config['nodes']) == n, config
        assert all(node['cpus'] == args.vcpus for node in config['nodes']), config
        hardware = verify_cloud_hardware(args.course, context._compute_id, n)
        write_json(output / 'ec2-hardware.json', hardware)
        write_json(output / 'lifecycle.json', lifecycle)
        rounds = []
        for i in range(1, args.repetitions + 1):
            print('Running %d nodes, repetition %d...' % (n, i), flush=True)
            result = execute_round(pc, context, args, n, i, source_uri, output / ('r%02d' % i), expected)
            rounds.append(result)
            print('Query %.3fs; exact correctness PASS; observed workers %s' %
                  (result['query_wall_seconds'], result['max_stage_workers']), flush=True)
        times = [r['query_wall_seconds'] for r in rounds]
        aggregate = {'workers': n, 'repetitions': len(rounds), 'query_seconds_median': statistics.median(times),
            'query_seconds_min': min(times), 'query_seconds_max': max(times),
            'query_seconds_stdev': statistics.stdev(times) if len(times) > 1 else None,
            'startup_seconds': lifecycle['startup_seconds'],
            'source_sha256': expected['source_sha256'], 'expected_sha256': expected['expected_sha256'],
            'all_correct': all(r['validation']['correct'] for r in rounds)}
        write_json(output / 'aggregate.json', aggregate)
        lifecycle['status'] = 'success'
        write_json(output / 'cpu-credit-metrics.json', collect_credit_metrics(
            args.course, [i['InstanceId'] for i in hardware['nodes']], lifecycle['started_utc'], utc()))
        return aggregate
    except BaseException as exc:
        lifecycle.update(status='failed', error=repr(exc), traceback=traceback.format_exc())
        raise
    finally:
        lifecycle['compute_id'] = str(context._compute_id) if context._compute_id else None
        try:
            if context._compute_id is None:
                lifecycle['compute_id_not_returned'] = True
                lifecycle['cleanup_note'] = 'Start rejected before SDK received an ID; service may briefly create partial resources. Inspect workspace EC2 state.'
            else:
                context.stop()
            deadline = time.monotonic() + 600
            while context._compute_id is not None and not lifecycle.get('stop_confirmed') and time.monotonic() < deadline:
                status = remote_status(pc, workspace, context._compute_id)
                lifecycle['last_compute_status'] = str(status)
                if status.is_terminal() or status.is_uninitialized():
                    lifecycle['stop_confirmed'] = True
                    break
                time.sleep(5)
            if context._compute_id is not None and not lifecycle.get('stop_confirmed'):
                raise TimeoutError('Cloud compute stop was requested but not confirmed')
            if context._compute_id is not None:
                lifecycle.update(wait_ec2_release(args, context._compute_id))
        except BaseException as exc:
            lifecycle['cleanup_error'] = repr(exc)
        lifecycle['finished_utc'] = utc()
        write_json(output / 'lifecycle.json', lifecycle)
        if lifecycle.get('cleanup_error'):
            raise RuntimeError('Inspect this cluster in Polars Cloud; stop not confirmed: ' + lifecycle['cleanup_error'])


def main():
    from project import course, dataset, frozen_baseline
    c = course()
    frozen_baseline()
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--workers', type=int, nargs='+', choices=c['workers'], default=c['workers'])
    parser.add_argument('--profile')
    parser.add_argument('--workspace', help='Polars Cloud workspace ID; overrides course configuration')
    parser.add_argument('--bucket', help='S3 output bucket; overrides course configuration')
    parser.add_argument('--input')
    parser.add_argument('--instance-type')
    parser.add_argument('--repetitions', type=int)
    parser.add_argument('--experiment', default='course-polars-' + time.strftime('%Y%m%dT%H%M%SZ', time.gmtime()))
    parser.add_argument('--prepare-only', action='store_true')
    args = parser.parse_args()
    c = dict(c, instance_type=args.instance_type or c['instance_type'], repetitions=args.repetitions or c['repetitions'])
    import re
    if not re.fullmatch(r'[a-zA-Z0-9_-]{1,80}', args.experiment) or len(set(args.workers)) != len(args.workers):
        parser.error('simple experiment name and unique workers required')
    args.course = dict(c, profile=args.profile)
    args.workspace = args.workspace or c['polars']['workspace_id']
    args.bucket = args.bucket or c['polars']['bucket']
    args.region = c['region']
    args.repetitions = c['repetitions']
    if not args.workspace or not args.bucket:
        parser.error('TA must configure polars.workspace_id and polars.bucket in course.json')
    lock = (ROOT / '.experiment.lock').open('w')
    fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
    if args.input and args.input.startswith('s3://'):
        from data.s3_dataset import inspect as inspect_s3
        uri, publication = inspect_s3(args.input, c['region'], args.profile)
        manifest = publication['dataset']
        prepared = None
    elif args.input:
        from data.local_input import prepare_local
        from data.prepare import verify
        path = Path(args.input).expanduser().resolve()
        prepared, manifest = (path, verify(path)) if path.is_dir() else prepare_local(path)
    else:
        prepared, manifest = dataset(c)
    expected = dict(manifest, source_sha256=manifest['input_sha256'])
    if prepared is None:
        input_manifest = publication
        uploaded = {'uri': uri + 'shards/*.txt', 'source_sha256': expected['source_sha256'], 'upload_and_size_check_seconds': 0}
    else:
        source = prepared / 'input.txt'
        staged = ROOT / 'staged' / expected['source_sha256']
        input_manifest = stage_text(source, staged)
        uploaded = upload(args, staged, input_manifest)
        write_json(staged / 's3.json', uploaded)
    if args.prepare_only:
        print(json.dumps(uploaded, indent=2))
        return
    preflight = inspect(args.course)
    if not preflight['cloud_credit_default_matches']:
        raise ValueError('Selected instance family default credit mode differs from EC2 unlimited; Cloud cannot silently substitute credit mode')
    args.vcpus = preflight['instance']['VCpuInfo']['DefaultVCpus']
    args.memory_mib = preflight['instance']['MemoryInfo']['SizeInMiB']
    import polars as pl
    import polars_cloud as pc
    pc.authenticate(interactive=False)
    workspace = pc.Workspace(id=UUID(args.workspace))
    if not workspace.aws.is_connected():
        raise RuntimeError('Workspace is not linked to AWS')
    args.organization_id = str(workspace.organization.id)
    output = ROOT / 'results' / args.experiment
    output.mkdir(parents=True, exist_ok=False)
    write_json(output / 'preflight.json', preflight)
    status = {'status': 'running', 'started_utc': utc(), 'args': vars(args)}
    write_json(output / 'status.json', status)
    write_json(output / 'provenance.json', {'polars': pl.__version__, 'polars_cloud': pc.__version__,
        'workspace_id': str(workspace.id), 'workspace_name': workspace.name,
        'course_config': c, 'dataset': manifest, 'input': input_manifest, 's3': uploaded,
        'query_sha256': file_sha(BASE / 'src/polars_query.py'),
        'runner_sha256': file_sha(Path(__file__)),
        'comparison_limits': 'Cloud S3-text-to-Parquet vs EC2 preloaded-local-text-to-local-output. Same input/model/count/credit mode, different I/O boundaries.'})
    aggregates = []
    try:
        for n in args.workers:
            aggregates.append(run_scale(pc, workspace, args, n, uploaded['uri'], output / ('%dn' % n), expected))
        with (output / 'scaling.csv').open('w', newline='') as fp:
            writer = csv.DictWriter(fp, fieldnames=list(aggregates[0]))
            writer.writeheader()
            writer.writerows(aggregates)
        status['status'] = 'success'
    except BaseException as exc:
        status.update(status='failed', error=repr(exc), traceback=traceback.format_exc())
        raise
    finally:
        status['finished_utc'] = utc()
        write_json(output / 'status.json', status)
        lock.close()
    subprocess.run([sys.executable, str(BASE / 'infrastructure/report.py')], check=True)


if __name__ == '__main__':
    main()
