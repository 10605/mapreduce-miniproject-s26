import hashlib
import io
import json
import os
from pathlib import Path
import sqlite3
import sys
import tempfile
import unittest
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / 'infrastructure'))
from stage_input import stage
from dataset import prepare, build_oracle
from common import sha256, write_json
from data.s3_dataset import publish, validate_manifest


class S3Tests(unittest.TestCase):
    def test_resume_export_verifies_prefix_and_does_not_repeat(self):
        from data.prepare import export_articles
        articles = [{'id': '1', 'text': 'alpha café'}, {'id': '2', 'text': 'beta 中文'}]
        with tempfile.TemporaryDirectory() as d:
            destination = Path(d) / 'out'
            export_articles(iter(articles), destination, 1)
            result = export_articles(iter(articles), destination, len('alpha café\nbeta 中文\n'.encode()), resume=True)
            self.assertEqual((destination / 'input.txt').read_text(), 'alpha café\nbeta 中文\n')
            self.assertEqual(result['documents'], 2)

    def test_parallel_oracle_matches_whole_input_counter(self):
        from data.oracle import build_parallel
        from collections import Counter
        import re
        with tempfile.TemporaryDirectory() as d:
            root = Path(d)
            text = ''.join('A café 中文 word%d\n' % i for i in range(1000))
            (root / 'text').write_text(text)
            build_parallel(root / 'text', root / 'counts.sqlite', 2)
            with sqlite3.connect(root / 'counts.sqlite') as db:
                self.assertEqual(dict(db.execute('SELECT word,count FROM counts')), dict(Counter(re.findall(r'\w+', text.lower()))))

    def test_download_verifies_parts_and_combined_hash(self):
        a, b = b'first\n', '中文\n'.encode()
        objects = [{'url': 'https://example.invalid/a', 'bytes': len(a), 'sha256': hashlib.sha256(a).hexdigest()},
                   {'url': 'https://example.invalid/b', 'bytes': len(b), 'sha256': hashlib.sha256(b).hexdigest()}]
        plan = {'objects': objects, 'bytes': len(a + b), 'sha256': hashlib.sha256(a + b).hexdigest()}
        with tempfile.TemporaryDirectory() as d, patch('stage_input.urllib.request.urlopen', side_effect=[io.BytesIO(a), io.BytesIO(b)]):
            target = Path(d) / 'input.txt'
            result = stage(plan, target)
            self.assertEqual(target.read_bytes(), a + b)
            self.assertFalse(result['cache_hit'])
            self.assertTrue(stage(plan, target)['cache_hit'])

    def test_corrupt_download_cannot_enter_benchmark(self):
        plan = {'objects': [{'url': 'https://example.invalid/SECRET', 'bytes': 3, 'sha256': '0' * 64}],
                'bytes': 3, 'sha256': '0' * 64}
        with tempfile.TemporaryDirectory() as d, patch('stage_input.urllib.request.urlopen', side_effect=lambda *a, **k: io.BytesIO(b'bad')), patch('stage_input.time.sleep'):
            with self.assertRaises(RuntimeError) as error:
                stage(plan, Path(d) / 'input.txt')
            self.assertNotIn('SECRET', str(error.exception))
            self.assertFalse((Path(d) / 'input.txt').exists())

    def test_local_and_s3_layouts_have_identical_worker_bytes(self):
        with tempfile.TemporaryDirectory() as d:
            root = Path(d)
            prepared = root / 'prepared'
            prepared.mkdir()
            text = ''.join('word%d café 中文 the\n' % i for i in range(3000))
            (prepared / 'input.txt').write_text(text)
            (prepared / 'articles.jsonl').write_text('')
            db = sqlite3.connect(prepared / 'expected.sqlite')
            build_oracle(prepared / 'input.txt', db)
            db.close()
            (prepared / 'expected.jsonl').write_text('fixture')
            m = {'name': 'fixture', 'repeat': 1, 'input_bytes': len(text.encode()),
                 'input_sha256': sha256(prepared / 'input.txt'), 'expected_sha256': sha256(prepared / 'expected.jsonl'),
                 'oracle_sha256': sha256(prepared / 'expected.sqlite'), 'attribution_sha256': sha256(prepared / 'articles.jsonl')}
            write_json(prepared / 'manifest.json', m)
            with patch('data.s3_dataset.subprocess.run', side_effect=RuntimeError('stop before AWS')):
                with self.assertRaisesRegex(RuntimeError, 'stop before AWS'):
                    publish(prepared, 's3://fixture/prefix/')
            publication = validate_manifest(json.loads((prepared / 's3-publish-v1/s3-manifest.json').read_text()))
            for n in [4, 8, 16]:
                with patch.dict(os.environ, {'PYTHONHASHSEED': '0'}):
                    local = prepare(prepared / 'input.txt', root / str(n), n, oracle=prepared / 'expected.sqlite')
                for i, node in enumerate(publication['layouts'][str(n)]):
                    self.assertEqual(node['sha256'], local['shards'][i]['sha256'])
            corrupted = json.loads(json.dumps(publication))
            corrupted['layouts']['16'][0]['parts'] = [1]
            with self.assertRaisesRegex(ValueError, 'skips/repeats'):
                validate_manifest(corrupted)


if __name__ == '__main__':
    unittest.main()
