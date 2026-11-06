import argparse
import importlib.util
import json
from pathlib import Path
import sqlite3
import subprocess
import shlex
import sys
import tempfile
import unittest
from types import SimpleNamespace
from unittest.mock import Mock, patch

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / 'infrastructure'))
from project import course, frozen_baseline
from data.prepare import export_articles, prepare, verify
from infrastructure.preflight import verify_cloud_hardware
from infrastructure.report import compatible
from infrastructure import cluster as deployment


class CourseTests(unittest.TestCase):
    def test_resume_finds_retained_timestamped_cluster(self):
        import run as entry
        with tempfile.TemporaryDirectory() as d, patch.object(entry, 'HERE', Path(d)):
            for name, phase in [('course16', 'terminated'), ('course16-20261105T000000Z', 'deployed')]:
                p = Path(d) / 'clusters' / name / 'state.json'
                p.parent.mkdir(parents=True)
                p.write_text(json.dumps({'name': name, 'phase': phase}))
            self.assertEqual(entry.choose_cluster_name('course16'), 'course16-20261105T000000Z')

    def test_background_launcher_releases_output_before_job_finishes(self):
        with tempfile.TemporaryDirectory() as d:
            (Path(d) / 'logs').mkdir()
            with patch.object(deployment, 'REMOTE_ROOT', d):
                command = deployment.background_command([sys.executable, '-c', 'import time; time.sleep(2)'], 'fixture')
                result = subprocess.run(['bash', '-c', command], capture_output=True, text=True, timeout=1)
            self.assertEqual(result.returncode, 0)
            self.assertTrue(result.stdout.strip().isdigit())

    def test_preflight_executes_using_configured_remote_root(self):
        with tempfile.TemporaryDirectory() as d, patch.object(deployment, 'HERE', Path(d)):
            root = Path(d) / 'custom-runtime'
            root.mkdir()
            (root / 'cluster.json').write_text(json.dumps({'id': 'fixture-id', 'nodes': [{'private_ip': 'fixture'}]}))
            (root / '.scp-probe').write_text('fixture-id')
            ctx = deployment.Cluster(argparse.Namespace(name='fixture', region='us-east-1', profile=None))
            ctx.nodes = Mock(return_value=[{'public_ip': 'fixture'}])
            def ssh(host, command, **kwargs):
                script = shlex.split(command)[2]
                self.assertIn(repr(str(root)), script)
                def copy(args, **kwargs):
                    self.assertEqual(args[-2], 'ec2-user@fixture:' + str(root / '.scp-probe'))
                    Path(args[-1]).write_text('fixture-id')
                old = list(sys.path)
                try:
                    with patch('subprocess.run', side_effect=copy):
                        exec(script, {})
                finally:
                    sys.path[:] = old
            ctx.public_ssh = ssh
            with patch.object(deployment, 'REMOTE_ROOT', str(root)):
                ctx.preflight()

    def test_wait_retries_new_instance_eventual_consistency(self):
        with tempfile.TemporaryDirectory() as d, patch.object(deployment, 'HERE', Path(d)):
            ctx = deployment.Cluster(argparse.Namespace(name='fixture', region='us-east-1', profile=None))
            ctx.aws = Mock(side_effect=[
                subprocess.CalledProcessError(254, ['aws'], stderr='InvalidInstanceID.NotFound'),
                {'Reservations': [{'Instances': [{'InstanceId': 'i-fixture', 'State': {'Name': 'running'}}]}]},
                {'InstanceStatuses': [{'InstanceId': 'i-fixture', 'InstanceState': {'Name': 'running'},
                                       'SystemStatus': {'Status': 'ok'}, 'InstanceStatus': {'Status': 'ok'}}]}])
            with patch.object(deployment.time, 'sleep'):
                ctx.wait_for_instances(['i-fixture'], timeout=5, interval=0)
            self.assertEqual(ctx.aws.call_count, 3)

    def test_frozen_baseline_is_original(self):
        frozen_baseline()
        for name in ['wc_hz.py', 'hz_worker.py', 'reduce_util.py']:
            self.assertTrue((ROOT / 'baseline' / name).is_file())

    def test_confirmed_common_instance(self):
        c = course()
        self.assertEqual(c['instance_type'], 't8i.small')
        self.assertEqual(c['cpu_credits'], 'unlimited')

    def test_export_keeps_articles_and_does_not_repeat(self):
        with tempfile.TemporaryDirectory() as d:
            items = [{'id': str(i), 'text': t} for i, t in enumerate(['abc\r\ndef', '中文 example', 'unused'])]
            m = export_articles(iter(items), Path(d) / 'out', 9)
            self.assertEqual(m['documents'], 2)
            self.assertEqual((Path(d) / 'out/input.txt').read_text(), 'abc\ndef\n中文 example\n')
            self.assertEqual(m['repeat'], 1)

    def test_duplicate_articles_rejected(self):
        with tempfile.TemporaryDirectory() as d:
            with self.assertRaises(sqlite3.IntegrityError):
                export_articles(iter([{'id': 'same', 'text': 'a'}, {'id': 'same', 'text': 'b'}]), Path(d) / 'out', 100)

    def test_parquet_fixture_oracle_and_hash_check(self):
        import pyarrow as pa
        import pyarrow.parquet as pq
        with tempfile.TemporaryDirectory() as d:
            root = Path(d)
            source = root / 'part.parquet'
            rows = [{'id': '1', 'url': 'https://example.org/1', 'title': 'Example', 'text': 'A a café 中文'},
                    {'id': '2', 'url': 'https://example.org/2', 'title': 'Other', 'text': 'the other'}]
            pq.write_table(pa.Table.from_pylist(rows), source)
            m = prepare(root / 'out', 1, [source])
            self.assertEqual(m['tokens'], 4)
            self.assertEqual(verify(root / 'out')['expected_sha256'], m['expected_sha256'])
            with (root / 'out/input.txt').open('a') as fp:
                fp.write('corrupt')
            with self.assertRaisesRegex(ValueError, 'corrupted'):
                verify(root / 'out')

    def test_cloud_mismatched_hardware_is_rejected(self):
        response = {'Reservations': [{'Instances': [{'InstanceId': 'i-fixture', 'InstanceType': 'c6i.large',
                                                       'State': {'Name': 'running'}}]}]}
        with patch('infrastructure.preflight.aws', return_value=response):
            with self.assertRaisesRegex(ValueError, 'differs'):
                verify_cloud_hardware(course(), 'cluster', 1)

    def test_report_cannot_compare_different_data_or_instance(self):
        a = dict(workers=4, instance_type='t8i.small', cpu_credits='unlimited', disk_gib_requested=100,
                 region='us-east-1', input_sha256='input', output_sha256='output')
        self.assertTrue(compatible(a, dict(a)))
        self.assertFalse(compatible(a, dict(a, input_sha256='other')))
        self.assertFalse(compatible(a, dict(a, instance_type='c6i.large')))

    def test_deploy_packages_edited_student_source(self):
        with tempfile.TemporaryDirectory() as d:
            root = Path(d)
            algorithm = root / 'src'
            algorithm.mkdir()
            for n in deployment.BASELINE_FILES:
                (algorithm / n).write_text('# student revision\n')
            args = argparse.Namespace(name='fixture', region='us-east-1', profile=None, algorithm_dir=str(algorithm))
            with patch.object(deployment, 'HERE', root):
                ctx = deployment.Cluster(args)
                for name, text in [('peer_key', 'test-only'), ('peer_key.pub', 'ssh-ed25519 FIXTURE')]:
                    (ctx.directory / name).write_text(text)
                # Copy only the trusted harness files into the test fixture.
                for name in ['common.py', 'worker.py', 'dataset.py', 'benchmark.py', 'stage_input.py']:
                    (root / name).write_bytes((ROOT / 'infrastructure' / name).read_bytes())
                ctx.state = {'id': 'fixture', 'config': {}}
                ctx.nodes = Mock(return_value=[{'public_ip': 'fixture', 'private_ip': 'fixture'}])
                ctx.public_ssh = Mock(return_value=Mock(stdout='ssh-ed25519 FIXTURE'))
                ctx.public_scp = Mock()
                ctx.preflight = Mock()
                ctx.deploy()
                self.assertEqual((ctx.directory / 'package/baseline/hz_worker.py').read_text(), '# student revision\n')

    def test_optional_native_query_matches_fixed_oracle(self):
        import polars as pl
        from src.polars_query import wordcount
        from cloud.validation import canonicalize
        from collections import Counter
        import re
        import hashlib
        with tempfile.TemporaryDirectory() as d:
            root = Path(d)
            text = 'A a_2 café 中文\u0301\nİ I Σ ΟΣ a\u200db ² Ⅻ a\u203fb\n'
            (root / 'text').write_text(text)
            expected = Counter(re.findall(r'\w+', text.lower()))
            raw = ''.join(json.dumps([k, v], ensure_ascii=False) + '\n' for k, v in sorted(expected.items()))
            expected_sha = hashlib.sha256(raw.encode()).hexdigest()
            result = canonicalize(wordcount(str(root / 'text')).collect(), root / 'actual', expected_sha)
            self.assertTrue(result['correct'])
            with self.assertRaises(ValueError):
                canonicalize(pl.DataFrame({'word': ['wrong'], 'count': [1]}), root / 'wrong', expected_sha)

    def test_cloud_start_failure_stops_same_instance_context(self):
        from cloud import runner
        with tempfile.TemporaryDirectory() as d:
            ctx = Mock()
            ctx._compute_id = 'fixture-id'
            ctx.start.side_effect = ValueError('start failed')
            pc = Mock()
            pc.ComputeContext.return_value = ctx
            args = SimpleNamespace(course=course())
            with patch.object(runner, 'remote_status', return_value=Mock(is_terminal=lambda: True)), \
                 patch.object(runner, 'wait_ec2_release', return_value={'ec2_release_confirmed': True}), \
                 self.assertRaisesRegex(ValueError, 'start failed'):
                runner.run_scale(pc, Mock(), args, 4, 's3://fixture/input', Path(d) / 'out', {})
            self.assertEqual(pc.ComputeContext.call_args.kwargs['instance_type'], 't8i.small')
            self.assertEqual(pc.ComputeContext.call_args.kwargs['cluster_size'], 4)
            ctx.stop.assert_called_once()
            lifecycle = json.loads((Path(d) / 'out/lifecycle.json').read_text())
            self.assertTrue(lifecycle['ec2_release_confirmed'])


if __name__ == '__main__':
    unittest.main()
