#!/usr/bin/env python3
"""One command: discover -> launch -> deploy -> benchmark -> fetch -> terminate.

Running this command creates billable EC2 resources. No interactive confirmations.
The personal SSH key stays local. Requires an already authenticated AWS CLI.
"""
import argparse
import fcntl
import ipaddress
import json
import os
from pathlib import Path
import subprocess
import sys
import time
import urllib.request

from cluster import Cluster, HERE
from common import REMOTE_ROOT, read_json, sha256
from summarize import summarize

if sys.platform == 'darwin':
    os.environ.setdefault('SSL_CERT_FILE', '/etc/ssl/cert.pem')


def public_cidr():
    with urllib.request.urlopen('https://checkip.amazonaws.com', timeout=15) as response:
        address = response.read(128).decode().strip()
    return str(ipaddress.IPv4Address(address)) + '/32'


def default_region(profile=None):
    value = os.environ.get('AWS_REGION') or os.environ.get('AWS_DEFAULT_REGION')
    if value:
        return value
    command = ['aws'] + (['--profile', profile] if profile else []) + ['configure', 'get', 'region']
    result = subprocess.run(command, capture_output=True, text=True)
    return result.stdout.strip() or 'us-east-1'


def discover_subnet(cluster, count):
    vpcs = cluster.aws('ec2', 'describe-vpcs', filters=[{'Name': 'is-default', 'Values': ['true']}])['Vpcs']
    if not vpcs:
        raise RuntimeError('No default VPC. Supply --subnet-id for an existing public subnet.')
    vpc = vpcs[0]['VpcId']
    subnets = cluster.aws('ec2', 'describe-subnets', filters=[{'Name': 'vpc-id', 'Values': [vpc]},
                                                          {'Name': 'state', 'Values': ['available']}])['Subnets']
    tables = cluster.aws('ec2', 'describe-route-tables', filters=[{'Name': 'vpc-id', 'Values': [vpc]}])['RouteTables']
    offerings = cluster.aws('ec2', 'describe-instance-type-offerings', location_type='availability-zone',
                            filters=[{'Name': 'instance-type', 'Values': [cluster.args.instance_type]}])['InstanceTypeOfferings']
    zones = {o['Location'] for o in offerings}
    main = next((table for table in tables if any(a.get('Main') for a in table.get('Associations', []))), None)
    for subnet in sorted(subnets, key=lambda s: (s['AvailabilityZone'], s['SubnetId'])):
        table = next((t for t in tables if any(a.get('SubnetId') == subnet['SubnetId'] for a in t.get('Associations', []))), main)
        public = table and any(r.get('DestinationCidrBlock') == '0.0.0.0/0' and
                               r.get('GatewayId', '').startswith('igw-') and r.get('State') == 'active' for r in table['Routes'])
        if public and subnet['AvailableIpAddressCount'] >= count and subnet['AvailabilityZone'] in zones:
            return subnet['SubnetId']
    raise RuntimeError('No suitable public subnet in the default VPC. Supply --subnet-id.')


def wait_for_experiment(cluster, maximum_seconds, interval=15):
    name = cluster.experiment()
    host = cluster.state['nodes'][0]['public_ip']
    deadline = time.monotonic() + maximum_seconds
    last = None
    errors = 0
    while time.monotonic() < deadline:
        try:
            value = cluster.public_ssh(host, 'cat ' + REMOTE_ROOT + '/results/' + name + '/status.json',
                                       capture_output=True, text=True, timeout=30)
            status = json.loads(value.stdout)
            errors = 0
            progress = (status['status'], status.get('current_repetition'))
            if progress != last:
                print('Experiment %s: %s, repetition=%s' % (name, *progress), flush=True)
                last = progress
            if status['status'] in ('success', 'failed'):
                return status
        except (subprocess.SubprocessError, ValueError):
            errors += 1
            if errors >= 8:
                raise RuntimeError('Cannot read remote status. Cluster retained; rerun the same command to resume.')
        time.sleep(interval)
    raise TimeoutError('Wait limit reached. Cluster retained; rerun the same command to resume.')


def execute(cluster, args):
    started = bool(cluster.state.get('latest_experiment'))
    fetched = False
    try:
        if not cluster.state:
            cluster.plan()
        if cluster.state['phase'] == 'planned':
            cluster.launch()
        if cluster.state['phase'] != 'deployed':
            # Recover a lost RunInstances response via unique tags; never launch another batch.
            if cluster.state['phase'] in ('provisioning', 'launch_requested'):
                instances = cluster.discover_instances()
                if len(instances) != args.workers:
                    raise RuntimeError('Partial provisioning; inspect state before retrying. No duplicate launch attempted.')
            cluster.deploy()
        if not started:
            cluster.start()
            started = True
        else:
            print('Resuming existing experiment ' + cluster.experiment(), flush=True)
        status = wait_for_experiment(cluster, args.timeout * 2 * args.repetitions + 3600)
        cluster.fetch()
        fetched = True
        print('Results saved: ' + cluster.state['fetched'][cluster.experiment()]['directory'], flush=True)
        if status['status'] != 'success':
            raise RuntimeError('Experiment failed; downloaded diagnostics include the error.')
    finally:
        # A lost SSH reply can leave a successfully launched remote job alive.
        started = started or bool(cluster.state.get('latest_experiment'))
        # Do not destroy the only copy of completed/running results if the laptop
        # loses connectivity or download fails. A rerun resumes rather than rents again.
        if not args.keep_cluster and (fetched or not started) and cluster.state.get('phase') not in (None, 'planned', 'terminated'):
            print('Cleaning up this experiment cluster...', flush=True)
            cluster.destroy()
        elif cluster.state.get('phase') not in (None, 'planned', 'terminated'):
            print('Cluster retained. Rerun to resume, or use cluster.py --name %s destroy --yes.' % args.name, flush=True)


def choose_cluster_name(base):
    candidates = []
    for path in (HERE / 'clusters').glob(base + '*/state.json'):
        value = read_json(path)
        if value.get('phase') != 'terminated':
            candidates.append(value['name'])
    if len(candidates) > 1:
        raise RuntimeError('Multiple retained clusters match this run; inspect/destroy them before renting more: ' + repr(candidates))
    if candidates:
        return candidates[0]
    if (HERE / 'clusters' / base / 'state.json').exists():
        return base + '-' + time.strftime('%Y%m%dT%H%M%SZ', time.gmtime())
    return base


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--workers', type=int, choices=[4, 8, 16], nargs='+', default=[4])
    parser.add_argument('--input', default=str(HERE.parent / 'data/prepared/input.txt'))
    parser.add_argument('--repeat', type=int, default=1)
    parser.add_argument('--repetitions', type=int, default=3)
    parser.add_argument('--ssh-key', required=True)
    parser.add_argument('--algorithm-dir', required=True)
    parser.add_argument('--implementation', choices=['baseline', 'student'], required=True)
    parser.add_argument('--oracle')
    parser.add_argument('--cpu-credits', choices=['unlimited'], default='unlimited')
    parser.add_argument('--region')
    parser.add_argument('--profile')
    parser.add_argument('--subnet-id')
    parser.add_argument('--ssh-cidr')
    parser.add_argument('--ami')
    parser.add_argument('--instance-type', default='t8i.small')
    parser.add_argument('--disk-gib', type=int, default=100)
    parser.add_argument('--timeout', type=int, default=7200)
    parser.add_argument('--name-prefix', default='wc')
    parser.add_argument('--keep-cluster', action='store_true')
    args = parser.parse_args()
    if min(args.repeat, args.repetitions, args.disk_gib, args.timeout) < 1:
        parser.error('repeat, repetitions, disk-gib and timeout must be positive')
    if len(set(args.workers)) != len(args.workers):
        parser.error('each worker count may only appear once')
    import re
    if not re.fullmatch(r'[A-Za-z0-9][A-Za-z0-9_-]{0,39}', args.name_prefix):
        parser.error('use a simple name prefix')
    s3 = args.input.startswith('s3://')
    if (not s3 and not Path(args.input).is_file()) or not Path(args.ssh_key).is_file():
        parser.error('input file or SSH private key does not exist')
    args.region = args.region or default_region(args.profile)
    # Derive once up front so unreadable/passphrase-protected keys fail before renting.
    subprocess.run(['ssh-keygen', '-y', '-P', '', '-f', args.ssh_key], check=True,
                   capture_output=True, timeout=10)
    args.ssh_cidr = args.ssh_cidr or public_cidr()
    if s3:
        sys.path.insert(0, str(HERE.parent))
        from data.s3_dataset import inspect
        args.input, args.s3_manifest = inspect(args.input, args.region, args.profile)
        source_hash = args.s3_manifest['dataset']['input_sha256']
    else:
        if not args.oracle or not Path(args.oracle).is_file():
            parser.error('local input requires its independently prepared --oracle')
        source_hash = sha256(args.input)
    args.input_mode = 's3-direct' if s3 else 'local-scp'
    placement = args.subnet_id
    ami = args.ami
    completed = []
    for count in args.workers:
        values = vars(args).copy()
        values.update(workers=count, name=args.name_prefix + str(count), yes=True,
                      ami=ami, experiment=None, retain_intermediates=False, subnet_id=placement)
        values['name'] = choose_cluster_name(values['name'])
        options = argparse.Namespace(**values)
        cluster = Cluster(options)
        if cluster.state:
            config = cluster.state['config']
            if (config['workers'] != count or config.get('ssh_key_path') != str(Path(args.ssh_key).resolve())
                    or config['instance_type'] != args.instance_type or config['disk_gib'] != args.disk_gib
                    or config.get('implementation') != args.implementation
                    or config.get('input_mode', 'local-scp') != args.input_mode
                    or config.get('algorithm_sha256') != {p.name: sha256(p) for p in sorted(Path(args.algorithm_dir).glob('*.py'))}):
                raise RuntimeError('Existing cluster configuration differs; choose another --name-prefix')
            old = cluster.state.get('experiments', {}).get(cluster.state.get('latest_experiment'), {})
            if old and (old['source_sha256'] != source_hash or old['repeat'] != args.repeat or old['repetitions'] != args.repetitions):
                raise RuntimeError('Existing experiment uses different input/repetitions; choose another --name-prefix')
            options.subnet_id = config['subnet_id']
        else:
            options.subnet_id = placement or discover_subnet(cluster, max(args.workers))
        print('=== %s | %d workers | key %s ===' % (options.name, count, args.ssh_key), flush=True)
        lock = (cluster.directory / '.workflow.lock').open('w')
        try:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
            execute(cluster, options)
            name = cluster.experiment()
            completed.append(Path(cluster.state['fetched'][name]['directory']) / 'results' / name)
        finally:
            lock.close()
            if cluster.state.get('config'):
                placement = cluster.state['config']['subnet_id']
                ami = cluster.state['config']['ami']
    if len(completed) > 1:
        destination = HERE / 'comparisons' / time.strftime('%Y%m%dT%H%M%SZ', time.gmtime())
        summarize(completed, destination)
        print('Scaling report: ' + str(destination / 'scaling.csv'))


if __name__ == '__main__':
    try:
        main()
    except (Exception, KeyboardInterrupt) as exc:
        print('ERROR: ' + str(exc), file=sys.stderr)
        if isinstance(exc, subprocess.CalledProcessError) and exc.stderr:
            print(exc.stderr.decode() if isinstance(exc.stderr, bytes) else exc.stderr, file=sys.stderr)
        sys.exit(1)
