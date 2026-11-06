"""Private per-node S3 download plan; stream, retry and verify before job timing."""
import argparse
import hashlib
import json
from pathlib import Path
import time
import urllib.request


def stage(plan, target):
    target = Path(target)
    target.parent.mkdir(parents=True, exist_ok=True)
    started = time.monotonic()
    if target.exists() and target.stat().st_size == plan['bytes']:
        digest = hashlib.sha256()
        with target.open('rb') as fp:
            for b in iter(lambda: fp.read(1024 * 1024), b''):
                digest.update(b)
        if digest.hexdigest() == plan['sha256']:
            return {'seconds': time.monotonic() - started, 'bytes': plan['bytes'], 'cache_hit': True}
    temporary = target.with_suffix('.downloading')
    last_error = None
    for attempt in range(3):
        try:
            digest = hashlib.sha256()
            total = 0
            with temporary.open('wb') as output:
                for item in plan['objects']:
                    part_digest = hashlib.sha256()
                    size = 0
                    with urllib.request.urlopen(item['url'], timeout=120) as response:
                        declared = getattr(response, 'headers', {}).get('Content-Length')
                        if declared is not None and int(declared) != item['bytes']:
                            raise ValueError('S3 object length differs from manifest')
                        while size < item['bytes']:
                            chunk = response.read(min(1024 * 1024, item['bytes'] - size))
                            if not chunk:
                                break
                            output.write(chunk)
                            digest.update(chunk)
                            part_digest.update(chunk)
                            size += len(chunk)
                            total += len(chunk)
                    if size != item['bytes'] or part_digest.hexdigest() != item['sha256']:
                        raise ValueError('S3 object checksum mismatch')
            if total != plan['bytes'] or digest.hexdigest() != plan['sha256']:
                raise ValueError('Combined input shard checksum mismatch')
            temporary.replace(target)
            return {'seconds': time.monotonic() - started, 'bytes': total, 'cache_hit': False,
                    'sha256': digest.hexdigest(), 'attempts': attempt + 1}
        except Exception as exc:
            # URLs are bearer credentials: never include urllib's request URL in logs.
            last_error = type(exc).__name__
            time.sleep(attempt + 1)
    raise RuntimeError('S3 download failed after 3 attempts (' + last_error + ')')


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--plan', type=Path, required=True)
    parser.add_argument('--target', type=Path, required=True)
    a = parser.parse_args()
    plan = json.loads(a.plan.read_text())
    try:
        result = stage(plan, a.target)
    finally:
        a.plan.unlink(missing_ok=True)
    print(json.dumps(result))
