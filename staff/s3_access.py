"""Build an auditable S3 read restriction; this module never applies it itself."""
import copy


SID = 'CourseReadsRequireUsEast1VpcEndpoint'


def restricted_policy(previous, bucket, prefix=None):
    """prefix scopes a canary test; None restricts all object reads in the bucket.

    Administrative APIs and uploads remain untouched. No owner exemption for
    reads: owner-signed/presigned downloads must follow the same network rule.
    """
    policy = copy.deepcopy(previous)
    policy.setdefault('Version', '2012-10-17')
    statements = policy.setdefault('Statement', [])
    if not isinstance(statements, list):
        raise ValueError('Expected a list of policy statements')
    if prefix is None:
        policy['Statement'] = [s for s in statements if s.get('Sid') not in
                               {SID, 'PublicReadObjects', 'PublicListBucket', 'RegionalVpcRead'}]
        policy['Statement'].append({
            'Sid': 'RegionalVpcRead', 'Effect': 'Allow', 'Principal': '*',
            'Action': 's3:GetObject', 'Resource': 'arn:aws:s3:::' + bucket + '/*',
            'Condition': {'ArnLike': {'aws:SourceVpcArn': 'arn:aws:ec2:us-east-1:*:vpc/*'}}})
    else:
        if not prefix or prefix.startswith('/') or '..' in prefix.split('/'):
            raise ValueError('Use a nonempty relative canary prefix')
        policy['Statement'] = [s for s in statements if s.get('Sid') != SID]
    policy['Statement'].append({
        'Sid': SID, 'Effect': 'Deny', 'Principal': '*',
        'Action': ['s3:GetObject', 's3:GetObjectVersion'],
        'Resource': 'arn:aws:s3:::' + bucket + '/' + (prefix.rstrip('/') + '/*' if prefix else '*'),
        'Condition': {'ArnNotLike': {'aws:SourceVpcArn': 'arn:aws:ec2:us-east-1:*:vpc/*'}}})
    return policy
