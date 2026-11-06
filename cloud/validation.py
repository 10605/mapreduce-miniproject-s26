import hashlib
import json
from pathlib import Path

import polars as pl


def file_sha(path):
    value = hashlib.sha256()
    with open(path, 'rb') as fp:
        for chunk in iter(lambda: fp.read(1024 * 1024), b''):
            value.update(chunk)
    return value.hexdigest()


def canonicalize(frame, destination, expected_sha):
    if set(frame.columns) != {'word', 'count'}:
        raise ValueError('Unexpected WordCount schema: ' + str(frame.schema))
    if frame['word'].null_count() or frame['count'].null_count():
        raise ValueError('Null word/count')
    if frame['word'].n_unique() != frame.height:
        raise ValueError('Duplicate output word')
    with Path(destination).open('x', encoding='utf-8', newline='\n') as fp:
        for word, count in frame.select('word', 'count').sort('word').iter_rows():
            if not isinstance(word, str) or type(count) is not int or count < 1:
                raise ValueError('Invalid word/count')
            fp.write(json.dumps([word, count], ensure_ascii=False) + '\n')
    checksum = file_sha(destination)
    result = {'correct': checksum == expected_sha, 'canonical_sha256': checksum,
              'expected_sha256': expected_sha, 'tokens': frame['count'].sum(), 'vocabulary': frame.height}
    if not result['correct']:
        raise ValueError('WordCount does not match the exact SeqWorker oracle: ' + json.dumps(result))
    return result
