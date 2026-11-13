#!/usr/bin/env python3
"""Student entry point. AWS compute is created only by the run subcommand."""
import argparse
import json
import os
from pathlib import Path
import subprocess
import sys

ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(ROOT / 'infrastructure'))
from common import sha256, read_json, write_json, utc


def course():
    c = read_json(ROOT / 'course.json')
    if c['cpu_credits'] != 'unlimited':
        raise ValueError('Credit mode must remain explicit and comparable: unlimited')
    return c


def frozen_baseline():
    for name, expected in read_json(ROOT / 'baseline/SHA256SUMS.json').items():
        if sha256(ROOT / 'baseline' / name) != expected:
            raise ValueError('Frozen baseline changed: ' + name)


def dataset(c):
    from data.prepare import verify
    directory = ROOT / c['dataset']['directory']
    m = verify(directory)
    if m['name'] != c['dataset']['name'] or m['input_bytes'] < c['dataset']['target_bytes']:
        raise ValueError('This is not the released course dataset (small fixtures cannot enter benchmark reports)')
    if m.get('python_major_minor') != c['python_major_minor'] or m.get('unicode_version') != c['unicode_version']:
        raise ValueError('Dataset tokenizer runtime differs from the course contract')
    return directory, m


def fetch(c, source='huggingface', s3_uri=None, profile=None):
    directory = ROOT / c['dataset']['directory']
    if source == 'huggingface':
        if not (directory / 'manifest.json').exists():
            print('Fetching original Hugging Face Wikipedia sources and rebuilding the fixed course dataset. '
                  'This includes extraction and reference-count generation; no course S3 download.', flush=True)
            command = [sys.executable, str(ROOT / 'data/prepare.py'), '--output', str(directory),
                       '--target-bytes', str(c['dataset']['target_bytes'])]
            if directory.exists():
                command.append('--resume')
            subprocess.run(command, check=True)
        _, m = dataset(c)
        reference = read_json(ROOT / 'data/course-publication.json')['manifest']['dataset']
        for field in ['input_sha256', 'expected_sha256', 'attribution_sha256']:
            if m[field] != reference[field]:
                raise ValueError('Rebuilt data differs from the fixed course dataset: ' + field)
        print('Verified course dataset: %.6f GB, SHA %s' % (m['input_bytes'] / 1e9, m['input_sha256']))
        return
    uri = s3_uri or c['dataset'].get('s3_uri')
    if not uri or not uri.startswith('s3://'):
        raise ValueError('TA must publish the prepared dataset and set dataset.s3_uri in course.json')
    from data.s3_dataset import command
    subprocess.run(command(c['region'], profile) + ['s3', 'sync', uri, str(directory), '--only-show-errors'], check=True)
    if not (directory / 'input.txt').exists() and (directory / 's3-manifest.json').exists():
        from data.s3_dataset import validate_manifest
        m = validate_manifest(read_json(directory / 's3-manifest.json'))
        import shutil
        temporary = directory / 'input.reconstructing'
        with temporary.open('wb') as output:
            for item in m['parts']:
                if sha256(directory / item['key']) != item['sha256']:
                    raise ValueError('Corrupted downloaded S3 shard')
                with (directory / item['key']).open('rb') as fp:
                    shutil.copyfileobj(fp, output, 1024 * 1024)
        if sha256(temporary) != m['dataset']['input_sha256']:
            raise ValueError('Reconstructed text hash differs')
        temporary.replace(directory / 'input.txt')
    _, m = dataset(c)
    print('Verified course dataset: %.6f GB, SHA %s' % (m['input_bytes'] / 1e9, m['input_sha256']))


def run(args, c):
    c = dict(c, instance_type=args.instance_type or c['instance_type'],
             repetitions=args.repetitions or c['repetitions'])
    source = args.input or c['dataset'].get('s3_uri') or str(ROOT / c['dataset']['directory'])
    s3 = source.startswith('s3://')
    if s3:
        from data.s3_dataset import inspect
        source, published = inspect(source, c['region'], args.profile, args.manifest)
        m = published['dataset']
        directory = None
    else:
        from data.prepare import verify
        from data.local_input import prepare_local
        path = Path(source).expanduser().resolve()
        if path.is_dir():
            directory, m = path, verify(path)
        elif path.is_file():
            directory, m = prepare_local(path)
        else:
            raise ValueError('Input must be a local UTF-8 file, prepared directory, or S3 dataset prefix')
    if args.backend == 'polars':
        command = [sys.executable, str(ROOT / 'cloud/runner.py'), '--workers', *map(str, args.workers)]
        if args.profile:
            command += ['--profile', args.profile]
        command += ['--instance-type', c['instance_type'], '--repetitions', str(c['repetitions']), '--input', source]
        if args.manifest:
            command += ['--manifest', args.manifest]
        subprocess.run(command, check=True)
        return
    key = args.ssh_key or os.environ.get('WC_SSH_KEY')
    if not key:
        raise ValueError('Set WC_SSH_KEY once to your own EC2 SSH private-key path')
    algorithm = ROOT / ('baseline' if args.backend == 'baseline' else 'src')
    prefix = 'course-' + args.backend + '-' + c['instance_type'].replace('.', '-') + ('-s3-' if s3 else '-local-')
    command = [sys.executable, str(ROOT / 'infrastructure/run.py'), '--input', source if s3 else str(directory / 'input.txt'),
               '--algorithm-dir', str(algorithm),
               '--implementation', args.backend, '--workers', *map(str, args.workers),
               '--repetitions', str(c['repetitions']), '--repeat', '1', '--instance-type', c['instance_type'],
               '--cpu-credits', c['cpu_credits'], '--disk-gib', str(c['disk_gib']),
               '--timeout', str(c['timeout_seconds']), '--region', c['region'],
               '--name-prefix', prefix, '--ssh-key', str(Path(key).expanduser())]
    if not s3:
        command += ['--oracle', str(directory / 'expected.sqlite')]
    elif args.manifest:
        command += ['--manifest', args.manifest]
    for name in ['profile', 'subnet_id', 'ssh_cidr', 'ami']:
        value = getattr(args, name, None)
        if value:
            command += ['--' + name.replace('_', '-'), value]
    if args.keep_cluster:
        command.append('--keep-cluster')
    print('Running %s on %s; dataset SHA %s' % (args.backend, c['instance_type'], m['input_sha256']), flush=True)
    subprocess.run(command, check=True)
    subprocess.run([sys.executable, str(ROOT / 'infrastructure/report.py')], check=True)


def main():
    p = argparse.ArgumentParser(description=__doc__)
    sub = p.add_subparsers(dest='command', required=True)
    download = sub.add_parser('fetch', help='rebuild fixed data from original Hugging Face sources; no compute')
    download.add_argument('--source', choices=['huggingface', 's3'], default='huggingface')
    download.add_argument('--s3-uri', help='optional S3 mirror; course bucket requires a us-east-1 VPC endpoint')
    download.add_argument('--profile')
    check = sub.add_parser('check', help='offline, real 4-worker algorithm smoke test')
    check.add_argument('--backend', choices=['baseline', 'student'], default='student')
    sub.add_parser('report', help='generate report from verified local results')
    preflight = sub.add_parser('preflight', help='read-only AWS instance/credit offering checks')
    preflight.add_argument('--instance-type')
    preflight.add_argument('--profile')
    publication = sub.add_parser('publish', help='prepare deterministic shards and upload a dataset to a private S3 prefix')
    publication.add_argument('--input', help='local UTF-8 file or prepared directory; defaults to course data')
    publication.add_argument('--s3-uri', required=True)
    publication.add_argument('--profile')
    publication.add_argument('--set-default', action='store_true', help='save this published prefix in course.json')
    r = sub.add_parser('run', help='create paid compute, run 3 repetitions, fetch, verify, clean up')
    r.add_argument('--backend', choices=['baseline', 'student', 'polars'], default='student')
    r.add_argument('--input', help='local UTF-8 file/prepared directory, or s3://bucket/dataset-prefix/')
    r.add_argument('--manifest', help='local publication record for a restricted custom S3 prefix')
    r.add_argument('--instance-type', help='AWS model, e.g. t8i.small or t2.micro; same model used by either backend')
    r.add_argument('--repetitions', type=int, help='defaults to course configuration; use 1 for a single trial')
    r.add_argument('--workers', type=int, choices=[4, 8, 16], nargs='+', default=[4, 8, 16])
    for name in ['ssh-key', 'profile', 'subnet-id', 'ssh-cidr', 'ami']:
        r.add_argument('--' + name)
    r.add_argument('--keep-cluster', action='store_true')
    args = p.parse_args()
    if getattr(args, 'repetitions', None) is not None and args.repetitions < 1:
        p.error('repetitions must be positive')
    if getattr(args, 'workers', None) and len(set(args.workers)) != len(args.workers):
        p.error('worker counts must be unique')
    c = course()
    frozen_baseline()
    if args.command == 'fetch':
        fetch(c, args.source, args.s3_uri, args.profile)
    elif args.command == 'check':
        subprocess.run([sys.executable, str(ROOT / 'tests/run_local.py'), '--backend', args.backend], check=True)
    elif args.command == 'report':
        subprocess.run([sys.executable, str(ROOT / 'infrastructure/report.py')], check=True)
    elif args.command == 'preflight':
        from infrastructure.preflight import inspect
        print(json.dumps(inspect(dict(c, instance_type=args.instance_type or c['instance_type'], profile=args.profile)), indent=2))
    elif args.command == 'publish':
        from data.s3_dataset import publish
        from data.local_input import prepare_local
        source = Path(args.input).expanduser().resolve() if args.input else ROOT / c['dataset']['directory']
        directory = source if source.is_dir() else prepare_local(source)[0]
        result = publish(directory, args.s3_uri, c['region'], args.profile)
        if args.set_default:
            c['dataset']['s3_uri'] = result['uri']
            write_json(ROOT / 'course.json', c)
        print(json.dumps(result, indent=2))
    else:
        run(args, c)


if __name__ == '__main__':
    try:
        main()
    except (Exception, KeyboardInterrupt) as exc:
        print('ERROR: ' + str(exc), file=sys.stderr)
        raise SystemExit(1)
