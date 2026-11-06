"""Shared stdlib-only helpers (Python >=3.9 on Amazon Linux 2023)."""
import hashlib
import json
import os
from pathlib import Path
import subprocess
import time

BASELINE_COMMIT = '5346bc41d5b099c40725669e9182548702a78562'
REMOTE_ROOT = '/home/ec2-user/wcproject'


def write_json(path, value):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + '.tmp')
    tmp.write_text(json.dumps(value, ensure_ascii=False, indent=2) + '\n', encoding='utf-8')
    tmp.replace(path)


def read_json(path):
    return json.loads(Path(path).read_text(encoding='utf-8'))


def sha256(path):
    digest = hashlib.sha256()
    with open(path, 'rb') as fp:
        for block in iter(lambda: fp.read(1024 * 1024), b''):
            digest.update(block)
    return digest.hexdigest()


def utc():
    return time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime())


def run(args, **kwargs):
    return subprocess.run([str(x) for x in args], check=True, **kwargs)


def ssh_options(key, known_hosts):
    return ['-i', str(key), '-o', 'BatchMode=yes', '-o', 'IdentitiesOnly=yes',
            '-o', 'StrictHostKeyChecking=yes', '-o', 'UserKnownHostsFile=' + str(known_hosts),
            '-o', 'ConnectTimeout=10', '-o', 'ServerAliveInterval=15',
            '-o', 'ServerAliveCountMax=3']


def hash_sources(root):
    root = Path(root)
    return {str(p.relative_to(root)): sha256(p)
            for p in sorted(root.rglob('*.py')) if '.venv' not in p.parts}
