"""Parallel TA reference: Python re + Counter + SQLite, independent of student code."""
from collections import Counter
from concurrent.futures import ProcessPoolExecutor
import json
from pathlib import Path
import re
import sqlite3


def count_range(task):
    source, start, end, database, progress = task
    db = sqlite3.connect(database)
    db.execute('CREATE TABLE counts (word TEXT PRIMARY KEY, count INTEGER NOT NULL)')
    batch = Counter()
    tokens = lines = 0
    with open(source, 'rb') as fp:
        fp.seek(start)
        while fp.tell() < end:
            raw = fp.readline()
            if not raw:
                break
            words = re.findall(r'\w+', raw.decode('utf-8').lower())
            batch.update(words)
            tokens += len(words)
            lines += 1
            if lines % 10000 == 0:
                db.executemany('INSERT INTO counts VALUES (?,?) ON CONFLICT(word) DO UPDATE SET count=count+excluded.count', batch.items())
                db.commit()
                batch.clear()
                Path(progress).write_text(json.dumps({'bytes_read': fp.tell() - start, 'bytes_total': end - start,
                                                     'tokens': tokens, 'lines': lines}))
    db.executemany('INSERT INTO counts VALUES (?,?) ON CONFLICT(word) DO UPDATE SET count=count+excluded.count', batch.items())
    db.commit()
    db.close()
    Path(progress).write_text(json.dumps({'bytes_read': end - start, 'bytes_total': end - start,
                                         'tokens': tokens, 'lines': lines, 'complete': True}))
    return {'database': str(database), 'tokens': tokens, 'lines': lines}


def build_parallel(source, database, workers=4):
    source, database = Path(source), Path(database)
    scratch = database.parent / 'oracle-parts'
    scratch.mkdir(exist_ok=False)
    size = source.stat().st_size
    boundaries = [0]
    with source.open('rb') as fp:
        for i in range(1, workers):
            anchor = size * i // workers
            if anchor:
                fp.seek(anchor - 1)
                if fp.read(1) != b'\n':
                    fp.readline()
                boundaries.append(fp.tell())
            else:
                boundaries.append(0)
    boundaries.append(size)
    tasks = [(str(source), boundaries[i], boundaries[i + 1], str(scratch / ('w%d.sqlite' % i)),
              str(scratch / ('w%d.json' % i))) for i in range(workers)]
    with ProcessPoolExecutor(max_workers=workers) as pool:
        records = list(pool.map(count_range, tasks))
    target = sqlite3.connect(database)
    target.execute('CREATE TABLE counts (word TEXT PRIMARY KEY, count INTEGER NOT NULL)')
    for i, record in enumerate(records):
        print('Merging independent oracle partition %d/%d' % (i + 1, workers), flush=True)
        part = sqlite3.connect(record['database'])
        cursor = part.execute('SELECT word,count FROM counts ORDER BY word')
        while True:
            rows = cursor.fetchmany(10000)
            if not rows:
                break
            target.executemany('INSERT INTO counts VALUES (?,?) ON CONFLICT(word) DO UPDATE SET count=count+excluded.count', rows)
            target.commit()
        part.close()
    expected_tokens = sum(r['tokens'] for r in records)
    if target.execute('SELECT COALESCE(SUM(count),0) FROM counts').fetchone()[0] != expected_tokens:
        raise ValueError('Independent oracle merge lost tokens')
    target.close()
    return records
