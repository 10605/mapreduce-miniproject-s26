#!/usr/bin/env python3
"""Four local subprocess workers; no AWS, no billable resources."""
import argparse
from concurrent.futures import ThreadPoolExecutor
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / 'infrastructure'))
from common import write_json
from dataset import prepare, validate


def check(backend):
    with tempfile.TemporaryDirectory(prefix='wc-course-') as tmp:
        tmp = Path(tmp)
        runtime = tmp / 'runtime'
        (runtime / 'baseline').mkdir(parents=True)
        algorithm = ROOT / ('baseline' if backend == 'baseline' else 'src')
        for f in algorithm.glob('*.py'):
            shutil.copyfile(f, runtime / 'baseline' / f.name)
        for f in ['worker.py', 'common.py']:
            shutil.copyfile(ROOT / 'infrastructure' / f, runtime / f)
        source = tmp / 'source.txt'
        source.write_text('The the A café 中文\n\nI İ Σ ΟΣ a_2 ² Ⅻ\n' +
                          ''.join('word%d the other\n' % i for i in range(500)))
        env = dict(os.environ, PYTHONHASHSEED='0', PYTHONUTF8='1')
        # Input hash partitioning requires the seed at process startup.
        subprocess.run([sys.executable, '-c',
                        'from dataset import prepare; import sys; prepare(sys.argv[1],sys.argv[2],4)',
                        str(source), str(tmp / 'dataset')], check=True, env=dict(env, PYTHONPATH=str(ROOT / 'infrastructure')))
        nodes = [tmp / ('node%d' % i) for i in range(4)]
        for i, node in enumerate(nodes):
            node.mkdir()
            shutil.copyfile(tmp / 'dataset' / ('shard-%02d.txt' % i), node / 'input.txt')
        cluster = {'nodes': [{'private_ip': 'node%d' % i} for i in range(4)],
                   'test_nodes': list(map(str, nodes))}
        write_json(tmp / 'cluster.json', cluster)
        for phase in ['map_sort', 'gather_reduce']:
            def worker(i):
                result = subprocess.run([sys.executable, str(ROOT / 'tests/worker_process.py'),
                    '--runtime', str(runtime), '--cluster', str(tmp / 'cluster.json'),
                    '--index', str(i), '--phase', phase, '--workdir', str(nodes[i])],
                    env=env, text=True, capture_output=True)
                if result.returncode:
                    raise RuntimeError(result.stdout + result.stderr)
            with ThreadPoolExecutor(max_workers=4) as pool:
                list(pool.map(worker, range(4)))
        actual = validate(tmp / 'dataset/expected.sqlite', [p / 'counts.txt' for p in nodes], tmp / 'validation')
        if not actual['correct']:
            raise AssertionError(actual)
        return {'backend': backend, 'workers': 4, 'correct': True,
                'canonical_output_sha256': actual['canonical_output_sha256'],
                'note': 'offline local-copy shuffle; correctness evidence, not AWS performance'}


if __name__ == '__main__':
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--backend', choices=['baseline', 'student'], default='student')
    print(json.dumps(check(p.parse_args().backend), indent=2))
