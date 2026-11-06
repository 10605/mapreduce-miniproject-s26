#!/usr/bin/env python3
"""TA-only deterministic Wikipedia text export and independent disk-backed oracle."""
import argparse
from contextlib import closing
import hashlib
import json
import os
from pathlib import Path
import shutil
import sqlite3
import sys
import urllib.request
import unicodedata

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / 'infrastructure'))
from common import sha256, write_json
from dataset import build_oracle


def download(url, destination, expected_sha, expected_bytes):
    if destination.exists():
        if destination.stat().st_size == expected_bytes and sha256(destination) == expected_sha:
            return
        raise ValueError('Cached source corrupted: ' + str(destination))
    destination.parent.mkdir(parents=True, exist_ok=True)
    temporary = destination.with_suffix('.part')
    with urllib.request.urlopen(url, timeout=120) as response, temporary.open('wb') as fp:
        remaining = expected_bytes
        while remaining:
            chunk = response.read(min(1024 * 1024, remaining))
            if not chunk:
                raise ValueError('Truncated source download')
            fp.write(chunk)
            remaining -= len(chunk)
    if temporary.stat().st_size != expected_bytes or sha256(temporary) != expected_sha:
        raise ValueError('Download checksum mismatch: ' + url)
    temporary.replace(destination)


def export_articles(articles, destination, target_bytes, resume=False):
    """Keep complete articles, never repeat them; stream input and record attribution."""
    destination.mkdir(parents=True, exist_ok=resume)
    total = documents = lines = 0
    digest = hashlib.sha256()
    existing_documents = 0
    if resume and (destination / 'input.txt').exists():
        with (destination / 'input.txt').open('rb') as fp:
            for raw in iter(lambda: fp.read(1024 * 1024), b''):
                digest.update(raw)
                total += len(raw)
                lines += raw.count(b'\n')
        with (destination / 'articles.jsonl').open() as fp:
            existing_documents = sum(1 for _ in fp)
        documents = existing_documents
        print('Verifying resumable prefix: %.3f GB, %d articles' % (total / 1e9, documents), flush=True)
    prefix_digest = hashlib.sha256()
    skipped = 0
    with closing(sqlite3.connect(destination / 'article-ids.sqlite')) as ids:
        ids.execute('CREATE TABLE IF NOT EXISTS ids (id TEXT PRIMARY KEY)')
        with (destination / 'input.txt').open('ab' if resume else 'wb') as output, (destination / 'articles.jsonl').open('a' if resume else 'w', encoding='utf-8') as attribution:
            for article in articles:
                # Normalize LF once, before publishing, identically for all backends.
                text = article['text'].replace('\r\n', '\n').replace('\r', '\n')
                if not text.strip():
                    continue
                raw = (text.rstrip('\n') + '\n').encode('utf-8')
                if skipped < existing_documents:
                    prefix_digest.update(raw)
                    skipped += 1
                    ids.execute('INSERT OR IGNORE INTO ids VALUES (?)', (str(article['id']),))
                    if skipped == existing_documents:
                        if prefix_digest.hexdigest() != digest.hexdigest():
                            raise ValueError('Existing text is not the exact source prefix; cannot resume')
                        ids.commit()
                        print('Exact prefix verified; continuing export', flush=True)
                    continue
                if total >= target_bytes:
                    break
                ids.execute('INSERT INTO ids VALUES (?)', (str(article['id']),))
                output.write(raw)
                digest.update(raw)
                total += len(raw)
                lines += raw.count(b'\n')
                documents += 1
                attribution.write(json.dumps({k: article.get(k) for k in ['id', 'url', 'title']}, ensure_ascii=False) + '\n')
                if documents % 10000 == 0:
                    ids.commit()
                    print('Exported %.3f GB, %d articles' % (total / 1e9, documents), flush=True)
                if total >= target_bytes:
                    break
        ids.commit()
    if skipped != existing_documents:
        raise ValueError('Source ended before the existing prefix was verified')
    if total < target_bytes:
        raise ValueError('Source exhausted before requested size; incomplete output retained for diagnosis')
    return {'input_bytes': total, 'input_sha256': digest.hexdigest(), 'input_lines': lines,
            'documents': documents, 'target_bytes': target_bytes, 'repeat': 1,
            'size_definition': 'uncompressed UTF-8 bytes, complete articles; last article may exceed target slightly'}


def prepare(output, target_bytes, local_parquet=None, resume=False, oracle_workers=1):
    import pyarrow.parquet as pq
    c = json.loads((ROOT / 'course.json').read_text())
    if '.'.join(map(str, sys.version_info[:2])) != c['python_major_minor'] or unicodedata.unidata_version != c['unicode_version']:
        raise ValueError('Build the oracle with course Python 3.13 / Unicode 15.1.0 to match EC2 tokenization')
    source = json.loads((ROOT / 'data/source.json').read_text())
    used = []

    def articles():
        files = ([{'path': str(p)} for p in sorted(local_parquet)] if local_parquet else source['files'])
        for item in files:
            if local_parquet:
                path = Path(item['path'])
            else:
                path = ROOT / 'data/cache' / item['path']
                url = 'https://huggingface.co/datasets/%s/resolve/%s/%s' % (source['repo'], source['revision'], item['path'])
                print('Fetching verified source: ' + item['path'], flush=True)
                download(url, path, item['sha256'], item['bytes'])
            used.append({'path': item['path'], 'sha256': sha256(path), 'bytes': path.stat().st_size})
            for batch in pq.ParquetFile(path).iter_batches(batch_size=256, columns=['id', 'url', 'title', 'text']):
                yield from batch.to_pylist()

    manifest = export_articles(articles(), output, target_bytes, resume=resume)
    if resume:
        for name in ['expected.sqlite', 'expected.jsonl']:
            (output / name).unlink(missing_ok=True)
    print('Building independent WordCount oracle for %.3f GB...' % (manifest['input_bytes'] / 1e9), flush=True)
    if oracle_workers > 1:
        from data.oracle import build_parallel
        build_parallel(output / 'input.txt', output / 'expected.sqlite', oracle_workers)
        db = sqlite3.connect(output / 'expected.sqlite')
    else:
        db = sqlite3.connect(output / 'expected.sqlite')
        build_oracle(output / 'input.txt', db)
    manifest['tokens'] = db.execute('SELECT SUM(count) FROM counts').fetchone()[0]
    manifest['vocabulary'] = db.execute('SELECT COUNT(*) FROM counts').fetchone()[0]
    with (output / 'expected.jsonl').open('w', encoding='utf-8') as fp:
        for row in db.execute('SELECT word,count FROM counts ORDER BY word'):
            fp.write(json.dumps(row, ensure_ascii=False) + '\n')
    db.close()
    manifest.update(name='wikipedia-en-10gb-v1' if not local_parquet else 'local-fixture',
                    source_repo=source['repo'], source_revision=source['revision'], sources=used,
                    tokenization=r"Python re.findall(r'\w+', line.lower())",
                    expected_sha256=sha256(output / 'expected.jsonl'),
                    oracle_sha256=sha256(output / 'expected.sqlite'),
                    attribution_sha256=sha256(output / 'articles.jsonl'),
                    python_major_minor=c['python_major_minor'], unicode_version=c['unicode_version'],
                    license=source['license'])
    write_json(output / 'manifest.json', manifest)
    return manifest


def verify(directory):
    m = json.loads((directory / 'manifest.json').read_text())
    for name, key in [('input.txt', 'input_sha256'), ('expected.sqlite', 'oracle_sha256'),
                      ('expected.jsonl', 'expected_sha256'), ('articles.jsonl', 'attribution_sha256')]:
        if sha256(directory / name) != m[key]:
            raise ValueError('Dataset corrupted: ' + name)
    if (directory / 'input.txt').stat().st_size != m['input_bytes'] or m['repeat'] != 1:
        raise ValueError('Dataset size/repeat mismatch')
    return m


if __name__ == '__main__':
    os.environ.setdefault('SSL_CERT_FILE', '/etc/ssl/cert.pem') if sys.platform == 'darwin' else None
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--output', type=Path, default=ROOT / 'data/prepared')
    p.add_argument('--target-bytes', type=int, default=10_000_000_000)
    p.add_argument('--local-parquet', nargs='+', type=Path)
    p.add_argument('--resume', action='store_true', help='verify and continue an interrupted export, never overwrite published data')
    p.add_argument('--oracle-workers', type=int, default=1, choices=range(1, 9))
    args = p.parse_args()
    if args.target_bytes < 1:
        p.error('target-bytes must be positive')
    if args.output.exists() and (not args.resume or (args.output / 'manifest.json').exists()):
        raise SystemExit('Output exists; verify it or choose another directory. No overwrite performed.')
    print(json.dumps(prepare(args.output, args.target_bytes, args.local_parquet, resume=args.resume,
                             oracle_workers=args.oracle_workers), indent=2))
