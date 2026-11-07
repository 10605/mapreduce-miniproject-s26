"""Exercise disk spilling, multi-pass merges, and the upstream sort contract."""
import importlib.util
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
spec = importlib.util.spec_from_file_location('upstream_merge_sort', ROOT / 'baseline/merge_sort.py')
ms = importlib.util.module_from_spec(spec)
spec.loader.exec_module(ms)


class MergeSortTests(unittest.TestCase):
    def test_in_place_multiple_disk_passes_preserve_duplicate_order(self):
        with tempfile.TemporaryDirectory() as d:
            root = Path(d)
            spill = root / 'disk'
            spill.mkdir()
            rows = [json.dumps(['中文', 'café', 'a', 'A', 'İ'][i % 5], ensure_ascii=False)
                    + '\t' + str(i) + '\n' for i in reversed(range(400))]
            source = root / 'records'
            source.write_text(''.join(rows))
            with patch.object(ms.tempfile, 'tempdir', str(spill)):
                ms.merge_sort_file(source, source, batch_size=7, fanin=3)
            self.assertEqual(source.read_text(), ''.join(sorted(rows, key=ms.line_prefix_key)))
            self.assertEqual(list(spill.iterdir()), [])

    def test_merge_presorted_files_and_empty_shards(self):
        with tempfile.TemporaryDirectory() as d:
            root = Path(d)
            shards = []
            rows = ['"a"\t1\n', '"a"\t2\n', '"z"\t3\n']
            for i, values in enumerate([rows[:2], [], rows[2:]]):
                path = root / str(i)
                path.write_text(''.join(values))
                shards.append(path)
            output = root / 'output'
            ms.merge_sorted_files(shards, output)
            self.assertEqual(output.read_text(), ''.join(rows))

    def test_empty_and_missing_final_newline(self):
        with tempfile.TemporaryDirectory() as d:
            p = Path(d) / 'records'
            for text, expected in [('', ''), ('"b"\t1\n"a"\t2', '"a"\t2\n"b"\t1\n')]:
                p.write_text(text)
                ms.merge_sort_file(p, p, batch_size=1, fanin=2)
                self.assertEqual(p.read_text(), expected)
