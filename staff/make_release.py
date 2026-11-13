#!/usr/bin/env python3
"""Whitelist-only starter archive, excluding data, credentials and prior results."""
import hashlib
import json
from pathlib import Path
import sys
import zipfile

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from project import course, frozen_baseline


def files():
    top = ['README.md', 'course.json', 'project.py', 'requirements.txt', '.gitignore']
    result = [ROOT / n for n in top]
    for folder in ['baseline', 'src', 'infrastructure', 'cloud', 'tests', 'staff', 'data']:
        result += [p for p in (ROOT / folder).iterdir()
                   if p.is_file() and p.suffix in {'.py', '.json', '.md', '.txt'}
                   and p.name not in {'aws-preflight.json', 'test-results.txt', 'test-results-s3.txt', 'verification.json', 'requested-trial-status.json'}
                   and not p.name.startswith('requested-')
                   and (folder != 'staff' or p.name in {'TA_GUIDE.md', 'make_release.py', 's3_access.py'})]
    result += [ROOT / 'baseline/LICENSE']
    return sorted(set(result))


if __name__ == '__main__':
    course()
    frozen_baseline()
    paths = files()
    target = ROOT.parent / 'wordcount-starter-s3-20261105.zip'
    if target.exists():
        raise SystemExit('Archive exists; choose a new release name instead of overwriting')
    manifest = {str(p.relative_to(ROOT)): hashlib.sha256(p.read_bytes()).hexdigest() for p in paths}
    with zipfile.ZipFile(target, 'x', compression=zipfile.ZIP_DEFLATED) as z:
        for p in paths:
            z.write(p, arcname='wordcount-starter/' + str(p.relative_to(ROOT)))
        z.writestr('wordcount-starter/RELEASE_SHA256SUMS.json', json.dumps(manifest, indent=2) + '\n')
    print(target)
