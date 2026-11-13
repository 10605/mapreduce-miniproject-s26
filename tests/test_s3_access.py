import copy
import json
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / 'infrastructure'))
from data.s3_dataset import inspect, signed_plan
from infrastructure.s3_gateway import ensure, cleanup
from staff.s3_access import restricted_policy


class FakeCluster:
    def __init__(self, existing=()):
        self.state = {'id': 'test-id', 'name': 'test', 'config': {
            'region': 'us-east-1', 'vpc_id': 'vpc-test', 'subnet_id': 'subnet-test'}}
        self.existing = list(existing)
        self.calls = []

    def save(self):
        pass

    def aws(self, service, operation, **values):
        self.calls.append((operation, values))
        if operation == 'describe-route-tables':
            return {'RouteTables': [{'RouteTableId': 'rtb-main', 'Associations': [{'Main': True}]},
                {'RouteTableId': 'rtb-explicit', 'Associations': [{'SubnetId': 'subnet-test'}]}]}
        if operation == 'describe-vpc-endpoints':
            if values.get('filters', [{}])[0].get('Name') == 'tag:HazbenchCluster':
                return {'VpcEndpoints': [e for e in self.existing if e.get('Tags')]}
            return {'VpcEndpoints': self.existing}
        if operation == 'create-vpc-endpoint':
            endpoint = {'VpcEndpointId': 'vpce-new', 'VpcEndpointType': 'Gateway',
                'RouteTableIds': values['route_table_ids'], 'State': 'available',
                'Tags': values['tag_specifications'][0]['Tags']}
            self.existing.append(endpoint)
            return {'VpcEndpoint': endpoint}
        if operation == 'delete-vpc-endpoints':
            self.existing = []
            return {'Unsuccessful': []}
        raise AssertionError(operation)


class AccessTests(unittest.TestCase):
    def test_starter_archive_includes_pinned_metadata_and_policy_test_dependency(self):
        from staff.make_release import files
        packaged = {str(p.relative_to(ROOT)) for p in files()}
        self.assertTrue({'data/course-publication.json', 'infrastructure/s3_gateway.py',
                         'staff/s3_access.py', 'tests/test_s3_access.py'} <= packaged)

    def test_official_metadata_and_urls_need_no_laptop_s3_or_signing(self):
        record = json.loads((ROOT / 'data/course-publication.json').read_text())
        with patch('data.s3_dataset.subprocess.run', side_effect=AssertionError('S3 contacted')), \
             patch('data.s3_dataset.subprocess.check_output', side_effect=AssertionError('Credentials used')):
            uri, m = inspect(record['uri'], 'us-east-1')
            plan = signed_plan(uri, m, 4, 'us-east-1')
        self.assertEqual(len(plan['nodes']), 4)
        self.assertEqual(sum(n['bytes'] for n in plan['nodes']), m['dataset']['input_bytes'])
        self.assertNotIn('?', plan['nodes'][0]['objects'][0]['url'])
        self.assertTrue(plan['oracle']['url'].startswith('https://10605-f26-testing.s3.us-east-1.'))

    def test_wrong_region_fails_before_renting(self):
        record = json.loads((ROOT / 'data/course-publication.json').read_text())
        with self.assertRaisesRegex(ValueError, 'requires region'):
            inspect(record['uri'], 'us-west-2')

    def test_custom_pinned_manifest_cannot_change_source(self):
        record = json.loads((ROOT / 'data/course-publication.json').read_text())
        with tempfile.TemporaryDirectory() as d:
            p = Path(d) / 'record.json'
            p.write_text(json.dumps(record))
            with self.assertRaisesRegex(ValueError, 'another S3 prefix'):
                inspect('s3://different/prefix/', 'us-east-1', manifest_path=p)

    def test_policy_covers_missing_network_context_and_preserves_management(self):
        original = {'Version': '2012-10-17', 'Statement': [
            {'Sid': 'PublicReadObjects', 'Effect': 'Allow'},
            {'Sid': 'PublicListBucket', 'Effect': 'Allow'},
            {'Sid': 'ExistingAdmin', 'Effect': 'Allow', 'Action': 's3:PutObject'}]}
        snapshot = copy.deepcopy(original)
        final = restricted_policy(original, 'bucket')
        self.assertEqual(original, snapshot)
        self.assertEqual(final['Statement'][0], original['Statement'][2])
        deny = final['Statement'][-1]
        self.assertEqual(deny['Resource'], 'arn:aws:s3:::bucket/*')
        self.assertEqual(deny['Condition'], {'ArnNotLike': {
            'aws:SourceVpcArn': 'arn:aws:ec2:us-east-1:*:vpc/*'}})
        self.assertNotIn('s3:PutBucketPolicy', deny['Action'])
        self.assertIn('s3:GetObjectVersion', deny['Action'])
        self.assertEqual(restricted_policy(final, 'bucket'), final)

    def test_canary_does_not_restrict_course_objects(self):
        original = {'Statement': [{'Sid': 'PublicReadObjects', 'Effect': 'Allow'}]}
        p = restricted_policy(original, 'bucket', 'verification/unique')
        self.assertEqual(p['Statement'][0], original['Statement'][0])
        self.assertEqual(p['Statement'][-1]['Resource'], 'arn:aws:s3:::bucket/verification/unique/*')

    def test_gateway_create_is_tagged_resumable_and_cleanup_scoped(self):
        c = FakeCluster()
        result = ensure(c)
        self.assertTrue(result['created'])
        self.assertEqual(result['route_table_id'], 'rtb-explicit')
        self.assertEqual(ensure(c)['id'], result['id'])
        self.assertEqual(sum(op == 'create-vpc-endpoint' for op, _ in c.calls), 1)
        cleanup(c)
        self.assertTrue(c.state['s3_gateway']['deleted'])

    def test_shared_gateway_is_never_changed_or_deleted(self):
        c = FakeCluster([{'VpcEndpointId': 'vpce-existing', 'VpcEndpointType': 'Gateway',
            'RouteTableIds': ['rtb-explicit'], 'State': 'available'}])
        self.assertFalse(ensure(c)['created'])
        cleanup(c)
        self.assertFalse(any(op.startswith(('create-', 'delete-', 'modify-')) for op, _ in c.calls))

    def test_fetch_rebuild_uses_huggingface_and_checks_course_identity(self):
        import project
        c = project.course()
        ref = json.loads((ROOT / 'data/course-publication.json').read_text())['manifest']['dataset']
        with tempfile.TemporaryDirectory() as d:
            c['dataset']['directory'] = d + '/prepared'
            with patch('project.subprocess.run') as execute, \
                 patch('project.dataset', return_value=(None, ref)):
                project.fetch(c)
            cmd = execute.call_args.args[0]
            self.assertIn(str(ROOT / 'data/prepare.py'), cmd)
            self.assertNotIn('aws', cmd)
        bad = dict(ref, input_sha256='0' * 64)
        with tempfile.TemporaryDirectory() as d:
            c['dataset']['directory'] = d + '/prepared'
            with patch('project.subprocess.run'), patch('project.dataset', return_value=(None, bad)):
                with self.assertRaisesRegex(ValueError, 'differs from the fixed'):
                    project.fetch(c)


if __name__ == '__main__':
    unittest.main()
