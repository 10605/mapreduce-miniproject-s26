"""Cache an exact, independently validated normalized copy of a local text file."""
import json
from pathlib import Path
import shutil
import sqlite3
import sys
import unicodedata
from data.prepare import verify

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / 'infrastructure'))
from common import sha256, write_json
from dataset import build_oracle


def prepare_local(source):
    source = Path(source).resolve()
    if source.name == 'input.txt' and (source.parent / 'manifest.json').exists():
        return source.parent, verify(source.parent)
    if sys.version_info[:2] != (3, 13) or unicodedata.unidata_version != '15.1.0':
        raise ValueError('Use Python 3.13 to prepare a new local input oracle')
    destination = ROOT / 'data/custom' / sha256(source)
    if (destination / 'manifest.json').exists():
        return destination, verify(destination)
    destination.mkdir(parents=True, exist_ok=False)
    with source.open(encoding='utf-8') as fp, (destination / 'input.txt').open('w', encoding='utf-8', newline='\n') as out:
        for line in fp:
            out.write(line if line.endswith('\n') else line + '\n')
    (destination / 'articles.jsonl').write_text('')
    db = sqlite3.connect(destination / 'expected.sqlite')
    build_oracle(destination / 'input.txt', db)
    m = {'name': 'custom-local', 'repeat': 1, 'input_bytes': (destination / 'input.txt').stat().st_size,
         'input_sha256': sha256(destination / 'input.txt'),
         'tokens': db.execute('SELECT COALESCE(SUM(count),0) FROM counts').fetchone()[0],
         'vocabulary': db.execute('SELECT COUNT(*) FROM counts').fetchone()[0],
         'python_major_minor': '3.13', 'unicode_version': '15.1.0'}
    with (destination / 'expected.jsonl').open('w', encoding='utf-8') as fp:
        for row in db.execute('SELECT word,count FROM counts ORDER BY word'):
            fp.write(json.dumps(row, ensure_ascii=False) + '\n')
    db.close()
    m.update(expected_sha256=sha256(destination / 'expected.jsonl'), oracle_sha256=sha256(destination / 'expected.sqlite'),
             attribution_sha256=sha256(destination / 'articles.jsonl'))
    write_json(destination / 'manifest.json', m)
    return destination, m
