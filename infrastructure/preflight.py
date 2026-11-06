"""Read-only AWS checks shared by the two course backends."""
import json
import os
import subprocess


def aws(c, service, operation, **values):
    command = ['aws', '--region', c['region'], '--output', 'json']
    if c.get('profile'):
        command += ['--profile', c['profile']]
    command += [service, operation]
    for key, value in values.items():
        command += ['--' + key.replace('_', '-'), json.dumps(value) if isinstance(value, (list, dict)) else str(value)]
    result = subprocess.run(command, check=True, text=True, capture_output=True,
                            env=dict(os.environ, AWS_PAGER='', AWS_CLI_AUTO_PROMPT='off'))
    return json.loads(result.stdout) if result.stdout.strip() else {}


def inspect(c):
    info = aws(c, 'ec2', 'describe-instance-types', instance_types=[c['instance_type']])['InstanceTypes'][0]
    if 'x86_64' not in info['ProcessorInfo']['SupportedArchitectures']:
        raise ValueError('This release requires an x86_64 instance')
    offerings = aws(c, 'ec2', 'describe-instance-type-offerings', location_type='availability-zone',
                    filters=[{'Name': 'instance-type', 'Values': [c['instance_type']]}])
    if not offerings['InstanceTypeOfferings']:
        raise ValueError('Requested instance is not offered in this region')
    family = c['instance_type'].split('.')[0]
    default = aws(c, 'ec2', 'get-default-credit-specification', instance_family=family) if info.get('BurstablePerformanceSupported') else None
    # The Python Cloud SDK has no credit-mode argument. Fail before renting if
    # its default would differ from the explicit unlimited EC2 baseline.
    return {'instance': info, 'offerings': offerings, 'default_credit_mode': default,
            'cloud_credit_default_matches': not default or default['InstanceFamilyCreditSpecification']['CpuCredits'] == c['cpu_credits'],
            'polars_instance_support': 'Not confirmed by AWS preflight; actual Cloud launch may reject it. Never substitute another type.'}


def verify_cloud_hardware(c, compute_id, n):
    response = aws(c, 'ec2', 'describe-instances', filters=[
        {'Name': 'tag:PolarsClusterId', 'Values': [str(compute_id)]},
        {'Name': 'instance-state-name', 'Values': ['pending', 'running']}])
    nodes = [i for r in response['Reservations'] for i in r['Instances']]
    if len(nodes) != n or any(i['InstanceType'] != c['instance_type'] or i['State']['Name'] != 'running' for i in nodes):
        raise ValueError('Cloud EC2 type/count/state differs from the course contract')
    ids = [i['InstanceId'] for i in nodes]
    info = aws(c, 'ec2', 'describe-instance-types', instance_types=[c['instance_type']])['InstanceTypes'][0]
    burst = info.get('BurstablePerformanceSupported', False)
    credits = aws(c, 'ec2', 'describe-instance-credit-specifications', instance_ids=ids)['InstanceCreditSpecifications'] if burst else []
    if burst and ({i['InstanceId'] for i in credits} != set(ids) or any(i['CpuCredits'] != c['cpu_credits'] for i in credits)):
        raise ValueError('Cloud CPU credit modes differ from the course baseline')
    volume_ids = [b['Ebs']['VolumeId'] for i in nodes for b in i.get('BlockDeviceMappings', []) if 'Ebs' in b]
    volumes = aws(c, 'ec2', 'describe-volumes', volume_ids=volume_ids)['Volumes'] if volume_ids else []
    return {'nodes': nodes, 'credits': credits, 'volumes': volumes,
            'instance_type_confirmed': c['instance_type'], 'workers_confirmed': n,
            'disk_note': 'Requested storage matches; managed OS/data volume layout is separately recorded, not assumed identical.'}
