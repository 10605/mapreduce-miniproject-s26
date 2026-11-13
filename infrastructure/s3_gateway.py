"""Route S3 downloads through a free regional gateway endpoint.

Reuse an endpoint already attached to the subnet's route table. Otherwise create
an isolated, tagged endpoint and remove only that endpoint during cluster cleanup.
"""
import time


def route_table(cluster, vpc, subnet):
    tables = cluster.aws('ec2', 'describe-route-tables', filters=[
        {'Name': 'vpc-id', 'Values': [vpc]}])['RouteTables']
    explicit = [t for t in tables if any(a.get('SubnetId') == subnet for a in t.get('Associations', []))]
    main = [t for t in tables if any(a.get('Main') for a in t.get('Associations', []))]
    selected = explicit or main
    if len(selected) != 1:
        raise RuntimeError('Cannot determine the subnet route table for S3 staging')
    return selected[0]['RouteTableId']


def ensure(cluster):
    c = cluster.state['config']
    service = 'com.amazonaws.' + c['region'] + '.s3'
    table = route_table(cluster, c['vpc_id'], c['subnet_id'])
    endpoints = cluster.aws('ec2', 'describe-vpc-endpoints', filters=[
        {'Name': 'vpc-id', 'Values': [c['vpc_id']]},
        {'Name': 'service-name', 'Values': [service]}])['VpcEndpoints']
    own = [e for e in endpoints if any(t['Key'] == 'HazbenchCluster' and t['Value'] == cluster.state['id']
                                     for t in e.get('Tags', [])) and e['State'] not in ('deleted', 'deleting', 'failed')]
    usable = [e for e in endpoints if e['VpcEndpointType'] == 'Gateway'
              and table in e.get('RouteTableIds', []) and e['State'] == 'available']
    if own:
        endpoint, created = own[0], True  # Recover a lost CreateVpcEndpoint response.
    elif usable:
        endpoint, created = usable[0], False
    else:
        cluster.state['s3_gateway'] = {'creation_requested': True, 'created': True, 'route_table_id': table}
        cluster.save()
        endpoint = cluster.aws('ec2', 'create-vpc-endpoint', vpc_id=c['vpc_id'],
            service_name=service, vpc_endpoint_type='Gateway', route_table_ids=[table],
            client_token=cluster.state['id'] + '-s3',
            tag_specifications=[{'ResourceType': 'vpc-endpoint', 'Tags': [
                {'Key': 'Name', 'Value': cluster.state['name'] + '-s3'},
                {'Key': 'HazbenchCluster', 'Value': cluster.state['id']}]}])['VpcEndpoint']
        created = True
    record = {'id': endpoint['VpcEndpointId'], 'created': created, 'route_table_id': table,
              'vpc_id': c['vpc_id'], 'region': c['region'], 'type': 'Gateway'}
    cluster.state['s3_gateway'] = record
    cluster.save()
    deadline = time.monotonic() + 180
    while endpoint['State'] != 'available':
        if endpoint['State'] in ('failed', 'deleted', 'deleting', 'rejected') or time.monotonic() >= deadline:
            raise RuntimeError('S3 gateway endpoint did not become available: ' + endpoint['State'])
        time.sleep(3)
        endpoint = cluster.aws('ec2', 'describe-vpc-endpoints',
                               vpc_endpoint_ids=[record['id']])['VpcEndpoints'][0]
    c['s3_gateway'] = record
    cluster.save()
    print('S3 Gateway Endpoint ready (%s): %s' % ('created' if created else 'reused', record['id']), flush=True)
    return record


def cleanup(cluster):
    record = cluster.state.get('s3_gateway', {})
    if not record.get('created'):
        return  # Never delete an endpoint belonging to another cluster/workspace.
    endpoints = cluster.aws('ec2', 'describe-vpc-endpoints', filters=[
        {'Name': 'tag:HazbenchCluster', 'Values': [cluster.state['id']]}])['VpcEndpoints']
    owned = [e['VpcEndpointId'] for e in endpoints if e['State'] not in ('deleted', 'deleting')]
    if owned:
        response = cluster.aws('ec2', 'delete-vpc-endpoints', vpc_endpoint_ids=owned)
        if response.get('Unsuccessful'):
            raise RuntimeError('S3 endpoint cleanup failed: ' + repr(response['Unsuccessful']))
    deadline = time.monotonic() + 180
    while True:
        left = cluster.aws('ec2', 'describe-vpc-endpoints', filters=[
            {'Name': 'tag:HazbenchCluster', 'Values': [cluster.state['id']]}])['VpcEndpoints']
        if all(e['State'] == 'deleted' for e in left):
            break
        if time.monotonic() >= deadline:
            raise RuntimeError('S3 endpoint deletion not confirmed; rerun destroy')
        time.sleep(3)
    record['deleted'] = True
    cluster.save()
