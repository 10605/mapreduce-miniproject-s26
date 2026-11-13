"""Prepare, publish and inspect a private S3 dataset with deterministic shards."""
import hashlib
import json
import os
from pathlib import Path, PurePosixPath
import re
import subprocess
import sys
import time
from urllib.parse import urlparse, quote

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / 'infrastructure'))
from common import read_json, write_json, sha256


def command(region, profile=None):
    return ['aws', '--region', region] + (['--profile', profile] if profile else [])


def validate_manifest(m):
    if m.get('schema') != 'wordcount-s3-v1' or m.get('canonical_parts') != 16:
        raise ValueError('Expected a published WordCount S3 manifest, not an arbitrary single object')
    for field in ['input_sha256', 'expected_sha256', 'oracle_sha256']:
        if not re.fullmatch(r'[a-f0-9]{64}', m.get('dataset', {}).get(field, '')):
            raise ValueError('Invalid dataset identity hash: ' + field)
    keys = []
    for item in m['parts'] + [m['oracle']]:
        path = PurePosixPath(item['key'])
        if path.is_absolute() or '..' in path.parts or not re.fullmatch(r'[a-f0-9]{64}', item['sha256']):
            raise ValueError('Invalid S3 manifest object path/hash')
        keys.append(item['key'])
    if len(set(keys)) != len(keys) or len(m['parts']) != 16:
        raise ValueError('Duplicate/missing S3 object entries')
    if sum(p['bytes'] for p in m['parts']) != m['dataset']['input_bytes']:
        raise ValueError('Shard sizes do not equal input size')
    for n in [4, 8, 16]:
        nodes = m['layouts'][str(n)]
        if len(nodes) != n or [i for node in nodes for i in node['parts']] != list(range(16)):
            raise ValueError('Layout skips/repeats input shards')
    return m


def pinned_publication(uri, manifest_path=None):
    """Use release metadata without reading the restricted course bucket locally."""
    uri = uri.removesuffix('s3-manifest.json').rstrip('/') + '/'
    path = Path(manifest_path) if manifest_path else ROOT / 'data/course-publication.json'
    if path.exists():
        record = read_json(path)
        if record['uri'].rstrip('/') + '/' == uri:
            return record
        if manifest_path:
            raise ValueError('Local manifest belongs to another S3 prefix')
    elif manifest_path:
        raise ValueError('Local publication record does not exist')
    return None


def inspect(uri, region, profile=None, manifest_path=None):
    if not uri.startswith('s3://'):
        raise ValueError('Expected s3://bucket/prefix/ or s3://bucket/prefix/s3-manifest.json')
    manifest_uri = uri if uri.endswith('/s3-manifest.json') else uri.rstrip('/') + '/s3-manifest.json'
    pinned = pinned_publication(uri, manifest_path)
    if pinned:
        if pinned.get('required_region', region) != region:
            raise ValueError('Course S3 input requires region ' + pinned['required_region'])
        return pinned['uri'], validate_manifest(pinned['manifest'])
    result = subprocess.run(command(region, profile) + ['s3', 'cp', manifest_uri, '-', '--only-show-errors'],
                            check=True, text=True, capture_output=True)
    m = validate_manifest(json.loads(result.stdout))
    return manifest_uri.rsplit('/', 1)[0] + '/', m


def signed_plan(uri, m, workers, region, profile=None):
    validate_manifest(m)
    pinned = pinned_publication(uri)
    def sign(item):
        if pinned:
            parsed = urlparse(uri)
            return dict(item, url='https://' + parsed.netloc + '.s3.' + region + '.amazonaws.com/'
                        + quote(parsed.path.lstrip('/') + item['key'], safe='/'))
        url = subprocess.check_output(command(region, profile) + ['s3', 'presign', uri + item['key'],
                                      '--expires-in', '43200'], text=True).strip()
        return dict(item, url=url)
    parts = [sign(p) for p in m['parts']]
    return {'dataset': m['dataset'], 'public_manifest': m,
            'source_uri': uri, 'workers': workers, 'oracle': sign(m['oracle']),
            'nodes': [dict(node, objects=[parts[i] for i in node['parts']]) for node in m['layouts'][str(workers)]]}


def stage_parts(prepared):
    """Pre-stage immutable canonical text while the independent oracle is building."""
    prepared = Path(prepared).resolve()
    source = prepared / 'input.txt'
    directory = prepared / 's3-publish-v1'
    manifest = directory / 'shards/manifest.json'
    if manifest.exists():
        m = read_json(manifest)
        if m['source_sha256'] != sha256(source):
            raise ValueError('Input changed since shard staging')
        return m
    (directory / 'shards').mkdir(parents=True, exist_ok=False)
    size = source.stat().st_size
    threshold = max(1, (size + 15) // 16)
    paths = [directory / 'shards' / ('shard-%02d.txt' % i) for i in range(16)]
    files = [p.open('wb') for p in paths]
    digests = [hashlib.sha256() for _ in files]
    digest = hashlib.sha256()
    total = 0
    try:
        with source.open('rb') as fp:
            for raw in fp:
                if not raw.endswith(b'\n'):
                    raise ValueError('Published source must already have normalized LF records')
                index = min(total // threshold, 15)
                files[index].write(raw)
                digests[index].update(raw)
                digest.update(raw)
                total += len(raw)
    finally:
        for f in files:
            f.close()
    m = {'source_sha256': digest.hexdigest(), 'input_bytes': total, 'workers': 16, 'repeat': 1,
         'shards': [{'file': p.name, 'bytes': p.stat().st_size, 'sha256': d.hexdigest()} for p, d in zip(paths, digests)]}
    write_json(manifest, m)
    return m


def publish(prepared, uri, region='us-east-1', profile=None):
    from data.prepare import verify
    from infrastructure.dataset import prepare
    prepared = Path(prepared).resolve()
    dataset = verify(prepared)
    directory = prepared / 's3-publish-v1'
    manifest_path = directory / 's3-manifest.json'
    if not manifest_path.exists():
        layout = stage_parts(prepared)
        if layout['source_sha256'] != dataset['input_sha256']:
            raise ValueError('Staged shard source differs from independent dataset manifest')
        parts = [{'key': 'shards/' + p['file'], 'bytes': p['bytes'], 'sha256': p['sha256']} for p in layout['shards']]
        layouts = {}
        for n in [4, 8, 16]:
            nodes = []
            for index in range(n):
                indices = list(range(index * (16 // n), (index + 1) * (16 // n)))
                digest = hashlib.sha256()
                total = 0
                for i in indices:
                    with (directory / parts[i]['key']).open('rb') as fp:
                        for b in iter(lambda: fp.read(1024 * 1024), b''):
                            digest.update(b)
                            total += len(b)
                nodes.append({'parts': indices, 'bytes': total, 'sha256': digest.hexdigest()})
            layouts[str(n)] = nodes
        import shutil
        for name in ['expected.sqlite', 'expected.jsonl', 'articles.jsonl', 'manifest.json']:
            shutil.copyfile(prepared / name, directory / name)
        m = {'schema': 'wordcount-s3-v1', 'canonical_parts': 16, 'dataset': dataset,
             'parts': parts, 'layouts': layouts,
             'oracle': {'key': 'expected.sqlite', 'bytes': (directory / 'expected.sqlite').stat().st_size,
                        'sha256': dataset['oracle_sha256']}}
        validate_manifest(m)
        write_json(manifest_path, m)
    m = validate_manifest(read_json(manifest_path))
    # Recheck bytes before uploading cached shards, not just their file sizes.
    for item in m['parts'] + [m['oracle']]:
        if sha256(directory / item['key']) != item['sha256']:
            raise ValueError('Corrupt cached publication object: ' + item['key'])
    uri = uri.rstrip('/') + '/'
    base = command(region, profile)
    existing = subprocess.run(base + ['s3', 'cp', uri + 's3-manifest.json', '-', '--only-show-errors'],
                              text=True, capture_output=True)
    if existing.returncode == 0 and json.loads(existing.stdout) != m:
        raise ValueError('S3 prefix contains another dataset manifest; choose a new prefix')
    # Publish the completion manifest last. No delete and no public ACL.
    active = directory / '.input-upload-state.json'
    state = read_json(active) if active.exists() else {}
    concurrent = state.get('uri') == uri and state.get('input_sha256') == dataset['input_sha256'] and state.get('status') == 'uploading'
    sync = base + ['s3', 'sync', str(directory), uri, '--exclude', 's3-manifest.json',
                   '--exclude', '.*', '--exclude', 'shards/expected*', '--exclude', 'shards/manifest.json',
                   '--only-show-errors', '--sse', 'AES256']
    if concurrent:
        # The same immutable text may already be uploading while the oracle was
        # computed. Upload only metadata here; never launch duplicate 10GB copies.
        sync += ['--exclude', 'shards/*']
    subprocess.run(sync, check=True)
    if concurrent:
        deadline = time.monotonic() + 7200
        while read_json(active).get('status') == 'uploading':
            if time.monotonic() > deadline:
                raise TimeoutError('Parallel input upload has not finished; no completion manifest published')
            print('Waiting for existing canonical input upload...', flush=True)
            time.sleep(20)
        if read_json(active).get('status') != 'success':
            raise RuntimeError('Parallel canonical input upload failed')
    subprocess.run(base + ['s3', 'cp', str(manifest_path), uri + 's3-manifest.json',
                           '--only-show-errors', '--sse', 'AES256'], check=True)
    _, remote = inspect(uri, region, profile)
    if remote != m:
        raise ValueError('Published manifest differs')
    objects = json.loads(subprocess.check_output(base + ['s3api', 'list-objects-v2', '--bucket', urlparse(uri).netloc,
                        '--prefix', urlparse(uri).path.lstrip('/'), '--output', 'json'], text=True))
    sizes = {o['Key']: o['Size'] for o in objects.get('Contents', [])}
    prefix = urlparse(uri).path.lstrip('/')
    for item in m['parts'] + [m['oracle']]:
        if sizes.get(prefix + item['key']) != item['bytes']:
            raise ValueError('S3 object missing or wrong size: ' + item['key'])
    return {'uri': uri, 'input_bytes': dataset['input_bytes'], 'input_sha256': dataset['input_sha256'],
            'parts': 16, 'workers_supported': [4, 8, 16], 'verified': True}
