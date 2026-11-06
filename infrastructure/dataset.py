"""Deterministic baseline-style input shards and a disk-backed independent oracle."""
import ast
from collections import Counter
import hashlib
import json
import os
from pathlib import Path
import re
import sqlite3
import shutil
import sys
import time

from common import sha256, write_json


def prepare(source, destination, workers, repeat=1, oracle=None):
    if workers < 1 or repeat < 1:
        raise ValueError('workers and repeat must be positive')
    if os.environ.get('PYTHONHASHSEED') != '0':
        raise ValueError('Start Python with PYTHONHASHSEED=0 for reproducible input sharding')
    start = time.monotonic()
    source, destination = Path(source), Path(destination)
    destination.mkdir(parents=True, exist_ok=False)
    if oracle:
        if repeat != 1:
            raise ValueError('Prepared course input must not be repeated')
        shutil.copyfile(oracle, destination / 'expected.sqlite')
    db = sqlite3.connect(destination / 'expected.sqlite')
    if not oracle:
        build_oracle(source, db, repeat)
    token_count = db.execute('SELECT COALESCE(SUM(count),0) FROM counts').fetchone()[0]
    line_count = 0
    shard_paths = [destination / ('shard-%02d.txt' % i) for i in range(workers)]
    outputs = [p.open('w', encoding='utf-8', newline='\n') for p in shard_paths]
    digest = hashlib.sha256()
    total_bytes = 0
    if workers not in (1, 2, 4, 8, 16):
        raise ValueError('workers must divide the 16 canonical input ranges')
    range_bytes = max(1, (source.stat().st_size + 15) // 16)
    try:
        for _ in range(repeat):
            with source.open(encoding='utf-8') as fp:
                for line in fp:
                    line = line if line.endswith('\n') else line + '\n'
                    raw = line.encode('utf-8')
                    recipient = min(total_bytes // range_bytes, 15) // (16 // workers)
                    digest.update(raw)
                    total_bytes += len(raw)
                    line_count += 1
                    outputs[recipient].write(line)
    finally:
        for fp in outputs:
            fp.close()
    with (destination / 'expected.jsonl').open('w', encoding='utf-8') as fp:
        for row in db.execute('SELECT word, count FROM counts ORDER BY word'):
            fp.write(json.dumps(row, ensure_ascii=False) + '\n')
    manifest = {'source_sha256': sha256(source), 'source_bytes': source.stat().st_size,
                'expanded_normalized_sha256': digest.hexdigest(), 'input_bytes': total_bytes,
                'input_lines': line_count, 'tokens': token_count,
                'vocabulary': db.execute('SELECT COUNT(*) FROM counts').fetchone()[0],
                'repeat': repeat, 'workers': workers, 'python': sys.version,
                'tokenization': "Python re.findall(r'\\w+', line.lower())",
                'sharding': '16 canonical contiguous byte ranges, whole LF records, consecutive groups per worker',
                'normalization': 'UTF-8; LF; final newline',
                'expected_sha256': sha256(destination / 'expected.jsonl'),
                'shards': [{'file': p.name, 'bytes': p.stat().st_size, 'sha256': sha256(p)} for p in shard_paths],
                'prepare_seconds': time.monotonic() - start}
    db.close()
    write_json(destination / 'manifest.json', manifest)
    return manifest


def build_oracle(source, db, repeat=1):
    db.execute('CREATE TABLE counts (word TEXT PRIMARY KEY, count INTEGER NOT NULL)')
    batch = Counter()
    line_count = token_count = 0
    # Independent reference: Counter + SQLite, not the provided mapper/reducer.
    with source.open(encoding='utf-8') as fp:
        for n, line in enumerate(fp, 1):
            words = re.findall(r'\w+', line.lower())
            batch.update(words)
            token_count += len(words)
            line_count += 1
            if n % 10000 == 0:
                db.executemany('INSERT INTO counts VALUES (?, ?) ON CONFLICT(word) DO UPDATE SET count=count+excluded.count', batch.items())
                db.commit()
                batch.clear()
    db.executemany('INSERT INTO counts VALUES (?, ?) ON CONFLICT(word) DO UPDATE SET count=count+excluded.count', batch.items())
    db.execute('UPDATE counts SET count=count*?', (repeat,))
    db.commit()


def validate(expected_db, output_paths, destination):
    """Exact comparison, also rejecting duplicate/missing keys and invalid types."""
    start = time.monotonic()
    destination = Path(destination)
    destination.mkdir(parents=True, exist_ok=True)
    db = sqlite3.connect(destination / 'actual.sqlite')
    db.execute('CREATE TABLE actual (word TEXT PRIMARY KEY, count INTEGER NOT NULL)')
    errors = []
    for path in output_paths:
        with open(path, encoding='utf-8') as fp:
            for line in fp:
                try:
                    item = ast.literal_eval(line)
                    if not isinstance(item, tuple) or len(item) != 2:
                        raise ValueError('expected (word, count) tuple')
                    key, value = item
                    if not isinstance(key, str) or type(value) is not int or value < 0:
                        raise ValueError('invalid word/count types or negative count')
                    db.execute('INSERT INTO actual VALUES (?, ?)', (key, value))
                except (ValueError, SyntaxError, TypeError, sqlite3.IntegrityError) as exc:
                    if len(errors) < 10:
                        errors.append(repr(exc))
    db.commit()
    db.execute('ATTACH DATABASE ? AS expected', (str(Path(expected_db).resolve()),))
    missing = db.execute('SELECT COUNT(*) FROM expected.counts e LEFT JOIN actual a ON a.word=e.word WHERE a.word IS NULL').fetchone()[0]
    extra = db.execute('SELECT COUNT(*) FROM actual a LEFT JOIN expected.counts e ON e.word=a.word WHERE e.word IS NULL').fetchone()[0]
    wrong = db.execute('SELECT COUNT(*) FROM actual a JOIN expected.counts e ON e.word=a.word WHERE a.count != e.count').fetchone()[0]
    with (destination / 'wordcounts.jsonl').open('w', encoding='utf-8') as fp:
        for row in db.execute('SELECT word, count FROM actual ORDER BY word'):
            fp.write(json.dumps(row, ensure_ascii=False) + '\n')
    result = {'correct': not (errors or missing or extra or wrong), 'parse_or_duplicate_errors': errors,
              'missing_keys': missing, 'extra_keys': extra, 'wrong_counts': wrong,
              'actual_tokens': db.execute('SELECT COALESCE(SUM(count), 0) FROM actual').fetchone()[0],
              'actual_vocabulary': db.execute('SELECT COUNT(*) FROM actual').fetchone()[0],
              'canonical_output_sha256': sha256(destination / 'wordcounts.jsonl'),
              'validation_seconds': time.monotonic() - start}
    db.close()
    write_json(destination / 'validation.json', result)
    return result
