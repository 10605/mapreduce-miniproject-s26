#!/usr/bin/env python3
"""Local AWS lifecycle CLI. No AWS resources are created by plan/status.

Only `launch` provisions resources. No AWS credentials are copied to EC2.
"""
import argparse
from concurrent.futures import ThreadPoolExecutor
import fcntl
import ipaddress
import json
import os
from pathlib import Path
import re
import shlex
import shutil
import subprocess
import sys
import tarfile
import time
import uuid

from common import BASELINE_COMMIT, REMOTE_ROOT, read_json, write_json, sha256, utc, run, ssh_options

HERE = Path(__file__).resolve().parent
PROJECT = HERE.parent
BASELINE_FILES = ['hz_worker.py', 'reduce_util.py', 'wc_hz.py']
SSM_AMI = '/aws/service/ami-amazon-linux-latest/al2023-ami-kernel-default-x86_64'
BOOTSTRAP = '''#!/bin/bash
set -euxo pipefail
dnf install -y python3.13 openssh-clients tar gzip
touch /var/lib/hazbench-ready
'''


def background_command(command, experiment):
    # Group the AND-list in the foreground. Backgrounding "cd && nohup" instead
    # leaves a subshell holding the SSH output pipe until the entire job exits.
    return ('cd ' + shlex.quote(REMOTE_ROOT) + ' && { nohup env PYTHONHASHSEED=0 PYTHONUTF8=1 '
            + shlex.join(command) + ' > ' + shlex.quote('logs/' + experiment + '.log')
            + ' 2>&1 < /dev/null & echo $!; }')


class Cluster:
    def __init__(self, args):
        self.args = args
        self.directory = HERE / 'clusters' / args.name
        self.directory.mkdir(parents=True, exist_ok=True, mode=0o700)
        self.directory.chmod(0o700)
        self.file = self.directory / 'state.json'
        self.state = read_json(self.file) if self.file.exists() else {}

    def save(self):
        write_json(self.file, self.state)

    def login_key(self):
        return Path(self.state.get('config', {}).get('ssh_key_path', self.directory / 'key'))

    def aws(self, service, operation, **values):
        config = self.state.get('config', {})
        args = ['aws', '--region', config.get('region', self.args.region), '--output', 'json']
        profile = config.get('profile', self.args.profile)
        if profile:
            args += ['--profile', profile]
        args += [service, operation]
        for key, value in values.items():
            args.append('--' + key.replace('_', '-'))
            if isinstance(value, (dict, list)):
                args.append(json.dumps(value))
            else:
                args.append(str(value))
        env = dict(os.environ, AWS_PAGER='', AWS_CLI_AUTO_PROMPT='off')
        proc = subprocess.run(args, check=True, capture_output=True, text=True, env=env)
        return json.loads(proc.stdout) if proc.stdout.strip() else {}

    def plan(self):
        if self.state:
            raise RuntimeError('This cluster name already has state; choose a new name or inspect status.')
        network = ipaddress.ip_network(self.args.ssh_cidr, strict=True)
        if network.version != 4 or network.prefixlen != 32:
            raise ValueError('--ssh-cidr must be your public IPv4 /32')
        key_path = getattr(self.args, 'ssh_key', None)
        if key_path:
            key_path = str(Path(key_path).expanduser().resolve())
            # Derive only the PUBLIC key; never read/log/copy the private material.
            subprocess.run(['ssh-keygen', '-y', '-P', '', '-f', key_path],
                           check=True, capture_output=True, text=True, timeout=10)
        subnet = self.aws('ec2', 'describe-subnets', subnet_ids=[self.args.subnet_id])['Subnets'][0]
        routes = self.aws('ec2', 'describe-route-tables', filters=[
            {'Name': 'association.subnet-id', 'Values': [self.args.subnet_id]}])['RouteTables']
        if not routes:
            routes = self.aws('ec2', 'describe-route-tables', filters=[
                {'Name': 'vpc-id', 'Values': [subnet['VpcId']]},
                {'Name': 'association.main', 'Values': ['true']}])['RouteTables']
        if not any(route.get('DestinationCidrBlock') == '0.0.0.0/0' and
                   route.get('GatewayId', '').startswith('igw-') and route.get('State') == 'active'
                   for table in routes for route in table['Routes']):
            raise ValueError('Subnet needs an active default route to an Internet Gateway (public subnet).')
        if subnet['AvailableIpAddressCount'] < self.args.workers:
            raise ValueError('Subnet has too few available IPv4 addresses')
        ami = self.args.ami or self.aws('ssm', 'get-parameter', name=SSM_AMI)['Parameter']['Value']
        image = self.aws('ec2', 'describe-images', image_ids=[ami])['Images'][0]
        if image['Architecture'] != 'x86_64' or image['State'] != 'available' or image.get('OwnerId') != '137112412989':
            raise ValueError('Use an available Amazon-owned x86_64 AL2023 AMI')
        if not image.get('Name', '').startswith('al2023-ami-'):
            raise ValueError('AMI must be Amazon Linux 2023')
        minimum_disk = next((m['Ebs']['VolumeSize'] for m in image['BlockDeviceMappings']
                             if m['DeviceName'] == image['RootDeviceName']), 8)
        if self.args.disk_gib < minimum_disk:
            raise ValueError('disk-gib is smaller than the AMI root volume')
        info = self.aws('ec2', 'describe-instance-types', instance_types=[self.args.instance_type])['InstanceTypes'][0]
        if 'x86_64' not in info['ProcessorInfo']['SupportedArchitectures']:
            raise ValueError('instance type must support x86_64')
        if info.get('BurstablePerformanceSupported') and self.args.cpu_credits != 'unlimited':
            raise ValueError('Course T instances must use the fixed unlimited credit mode')
        if not re.match(r'^[acdhimrzt][0-9]', self.args.instance_type):
            raise ValueError('This harness supports Standard On-Demand instance families, including T8i')
        offerings = self.aws('ec2', 'describe-instance-type-offerings', location_type='availability-zone', filters=[
            {'Name': 'location', 'Values': [subnet['AvailabilityZone']]},
            {'Name': 'instance-type', 'Values': [self.args.instance_type]}])['InstanceTypeOfferings']
        if not offerings:
            raise ValueError('instance type is not offered in the selected subnet AZ')
        identity = self.aws('sts', 'get-caller-identity')
        quota = None
        try:
            quota = self.aws('service-quotas', 'get-service-quota', service_code='ec2',
                             quota_code='L-1216C47A')['Quota']['Value']
        except subprocess.CalledProcessError:
            pass  # Optional read permission; launch will still enforce actual quotas.
        cpu = info['VCpuInfo']['DefaultVCpus']
        if quota is not None and cpu * self.args.workers > quota:
            raise ValueError('Requested vCPUs exceed the regional Standard On-Demand quota')
        self.state = {'schema': 1, 'name': self.args.name, 'id': uuid.uuid4().hex,
            'phase': 'planned', 'created_utc': utc(), 'account_id': identity['Account'],
            'config': {'region': self.args.region, 'profile': self.args.profile,
                       'workers': self.args.workers, 'subnet_id': self.args.subnet_id,
                       'vpc_id': subnet['VpcId'], 'availability_zone': subnet['AvailabilityZone'],
                       'ssh_cidr': str(network), 'ami': ami, 'ami_name': image['Name'],
                       'root_device_name': image['RootDeviceName'], 'instance_type': self.args.instance_type,
                       'vcpus_per_worker': cpu, 'memory_mib_per_worker': info['MemoryInfo']['SizeInMiB'],
                       'disk_gib': self.args.disk_gib, 'quota_standard_vcpus': quota,
                       'ebs_type': 'gp3', 'ebs_iops': 3000, 'ebs_throughput_mib_s': 125,
                       'requested_vcpus': cpu * self.args.workers,
                       'cpu_credits': self.args.cpu_credits if info.get('BurstablePerformanceSupported') else None,
                       'implementation': self.args.implementation,
                       'input_mode': getattr(self.args, 'input_mode', 'local-scp'),
                       'algorithm_sha256': {p.name: sha256(p) for p in sorted(Path(self.args.algorithm_dir).glob('*.py'))}}}
        if key_path:
            self.state['config']['ssh_key_path'] = key_path
        self.save()
        print(json.dumps(self.state, indent=2))
        print('PLAN ONLY. No billable resources created. Inspect state.json, then run launch.')

    def discover_instances(self):
        if 'config' not in self.state:
            raise RuntimeError('Run plan first')
        response = self.aws('ec2', 'describe-instances', filters=[
            {'Name': 'tag:HazbenchCluster', 'Values': [self.state['id']]},
            {'Name': 'instance-state-name', 'Values': ['pending', 'running', 'stopping', 'stopped', 'shutting-down', 'terminated']}])
        instances = [i for r in response['Reservations'] for i in r['Instances']]
        self.state['instance_ids'] = sorted({i['InstanceId'] for i in instances} | set(self.state.get('instance_ids', [])))
        self.state['instances'] = instances
        self.save()
        return instances

    def launch(self):
        if self.state.get('phase') != 'planned':
            raise RuntimeError('launch only accepts a fresh plan; use status/deploy/destroy after an interrupted launch')
        config = self.state['config']
        # Persist all names BEFORE cloud mutation so destroy can recover interrupted setup.
        name = 'hazbench-' + self.state['id'][:16]
        self.state.update(phase='provisioning', key_name=name, group_name=name, client_token=self.state['id'])
        self.save()
        key = self.login_key()
        public = self.directory / 'login_key.pub'
        if config.get('ssh_key_path'):
            public.write_text(subprocess.check_output(['ssh-keygen', '-y', '-P', '', '-f', str(key)], text=True))
        else:
            run(['ssh-keygen', '-q', '-t', 'ed25519', '-N', '', '-f', key])
            shutil.copyfile(str(key) + '.pub', public)
        self.aws('ec2', 'import-key-pair', key_name=name, public_key_material='fileb://' + str(public),
                 tag_specifications=[{'ResourceType': 'key-pair', 'Tags': [{'Key': 'HazbenchCluster', 'Value': self.state['id']}]}])
        response = self.aws('ec2', 'create-security-group', group_name=name,
                            description='10-605 isolated WordCount benchmark SSH', vpc_id=config['vpc_id'])
        self.state['security_group_id'] = response['GroupId']
        self.save()
        group = response['GroupId']
        self.aws('ec2', 'create-tags', resources=[group], tags=[{'Key': 'HazbenchCluster', 'Value': self.state['id']}])
        self.aws('ec2', 'authorize-security-group-ingress', group_id=group, ip_permissions=[
            {'IpProtocol': 'tcp', 'FromPort': 22, 'ToPort': 22, 'IpRanges': [{'CidrIp': config['ssh_cidr']}]},
            {'IpProtocol': 'tcp', 'FromPort': 22, 'ToPort': 22, 'UserIdGroupPairs': [{'GroupId': group}]}])
        bootstrap = self.directory / 'bootstrap.sh'
        bootstrap.write_text(BOOTSTRAP)
        tags = [{'Key': 'Name', 'Value': self.args.name}, {'Key': 'HazbenchCluster', 'Value': self.state['id']}]
        request = {'ImageId': config['ami'], 'InstanceType': config['instance_type'],
            'MinCount': config['workers'], 'MaxCount': config['workers'], 'KeyName': name,
            'ClientToken': self.state['client_token'],
            'NetworkInterfaces': [{'DeviceIndex': 0, 'SubnetId': config['subnet_id'], 'Groups': [group],
                                   'AssociatePublicIpAddress': True, 'DeleteOnTermination': True}],
            'BlockDeviceMappings': [{'DeviceName': config['root_device_name'], 'Ebs': {
                'VolumeSize': config['disk_gib'], 'VolumeType': 'gp3', 'Iops': 3000, 'Throughput': 125,
                'Encrypted': True, 'DeleteOnTermination': True}}],
            'MetadataOptions': {'HttpTokens': 'required', 'HttpEndpoint': 'enabled'},
            'TagSpecifications': [{'ResourceType': 'instance', 'Tags': tags}, {'ResourceType': 'volume', 'Tags': tags}]}
        request_file = self.directory / 'run-instances.json'
        if config.get('cpu_credits'):
            request['CreditSpecification'] = {'CpuCredits': config['cpu_credits']}
        write_json(request_file, request)
        self.state['phase'] = 'launch_requested'
        self.save()
        response = self.aws('ec2', 'run-instances', cli_input_json='file://' + str(request_file), user_data='file://' + str(bootstrap))
        self.state['instance_ids'] = [i['InstanceId'] for i in response['Instances']]
        self.state['phase'] = 'launched'
        self.save()
        print('Launched: ' + ' '.join(self.state['instance_ids']))
        print('Instances are now billable. Next: deploy. After fetching results: destroy --yes.')

    def public_ssh(self, host, command, **kwargs):
        # Per-cluster TOFU file; never alters the user's global known_hosts.
        options = ssh_options(self.login_key(), self.directory / 'known_hosts')
        options[options.index('StrictHostKeyChecking=yes')] = 'StrictHostKeyChecking=accept-new'
        return run(['ssh'] + options + ['ec2-user@' + host, command], **kwargs)

    def public_scp(self, host, source, destination, download=False):
        options = ssh_options(self.login_key(), self.directory / 'known_hosts')
        options[options.index('StrictHostKeyChecking=yes')] = 'StrictHostKeyChecking=accept-new'
        args = ['ec2-user@' + host + ':' + str(source), destination] if download else [source, 'ec2-user@' + host + ':' + str(destination)]
        return run(['scp', '-O'] + options + args, timeout=7200)

    def wait_for_instances(self, ids, timeout=1200, interval=10):
        """Use ordinary EC2 API operations, not the CLI's global waiter model.

        Some CLI v1/botocore installations ship inconsistent waiter/service
        models and crash building ANY `ec2 wait` command. Describe operations
        still work. Require both instance and system health checks to pass.
        """
        deadline = time.monotonic() + timeout
        previous = None
        last = {}
        while time.monotonic() < deadline:
            try:
                response = self.aws('ec2', 'describe-instances', instance_ids=ids)
            except subprocess.CalledProcessError as exc:
                # RunInstances IDs can precede their visibility in DescribeInstances.
                # This is EC2 eventual consistency, not proof provisioning failed.
                message = exc.stderr.decode() if isinstance(exc.stderr, bytes) else (exc.stderr or '')
                if 'InvalidInstanceID.NotFound' not in message:
                    raise
                print('Waiting for EC2 instance IDs to propagate...', flush=True)
                time.sleep(min(interval, max(0, deadline - time.monotonic())))
                continue
            instances = {i['InstanceId']: i for r in response['Reservations'] for i in r['Instances']}
            states = {instance_id: instances.get(instance_id, {}).get('State', {}).get('Name', 'not-yet-visible')
                      for instance_id in ids}
            bad = {key: value for key, value in states.items()
                   if value in ('stopping', 'stopped', 'shutting-down', 'terminated')}
            if bad:
                reasons = {key: instances[key].get('StateReason', {}) for key in bad}
                raise RuntimeError('Instances left the startup path: %s; reasons=%s' % (bad, reasons))
            healthy = set()
            checks = {}
            if all(value == 'running' for value in states.values()):
                try:
                    status = self.aws('ec2', 'describe-instance-status', instance_ids=ids)
                except subprocess.CalledProcessError as exc:
                    message = exc.stderr.decode() if isinstance(exc.stderr, bytes) else (exc.stderr or '')
                    if 'InvalidInstanceID.NotFound' not in message:
                        raise
                    print('Waiting for EC2 health status to propagate...', flush=True)
                    time.sleep(min(interval, max(0, deadline - time.monotonic())))
                    continue
                for item in status['InstanceStatuses']:
                    key = item['InstanceId']
                    checks[key] = {'system': item.get('SystemStatus', {}).get('Status', 'unknown'),
                                   'instance': item.get('InstanceStatus', {}).get('Status', 'unknown')}
                    if (key in states and item.get('InstanceState', {}).get('Name') == 'running'
                            and checks[key] == {'system': 'ok', 'instance': 'ok'}):
                        healthy.add(key)
            last = {'states': states, 'health_checks': checks}
            progress = (sum(value == 'running' for value in states.values()), len(healthy))
            if progress != previous:
                print('EC2 readiness: %d/%d running, %d/%d healthy' %
                      (progress[0], len(ids), progress[1], len(ids)), flush=True)
                previous = progress
            if len(healthy) == len(ids):
                return
            time.sleep(min(interval, max(0, deadline - time.monotonic())))
        raise TimeoutError('EC2 readiness exceeded %ss; last observed status: %s' % (timeout, last))

    def nodes(self, wait=False):
        if wait:
            ids = self.state.get('instance_ids') or [i['InstanceId'] for i in self.discover_instances()]
            if not ids:
                raise RuntimeError('No instances discovered; inspect status or destroy partial resources')
            self.wait_for_instances(ids)
        instances = sorted(self.discover_instances(), key=lambda i: i['InstanceId'])
        if len(instances) != self.state['config']['workers'] or any(i['State']['Name'] != 'running' for i in instances):
            raise RuntimeError('Expected all planned workers running; use status')
        return [{'instance_id': i['InstanceId'], 'private_ip': i['PrivateIpAddress'],
                 'public_ip': i['PublicIpAddress'], 'launch_time': i['LaunchTime']} for i in instances]

    def deploy(self):
        print('Waiting for EC2 status checks, cloud-init and SSH...', flush=True)
        nodes = self.nodes(wait=True)

        def ready(node):
            deadline = time.monotonic() + 1200
            while time.monotonic() < deadline:
                try:
                    self.public_ssh(node['public_ip'], 'test -f /var/lib/hazbench-ready',
                                    timeout=20, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
                    return
                except (subprocess.SubprocessError, OSError):
                    time.sleep(5)
            raise RuntimeError('SSH/bootstrap timeout on ' + node['instance_id'] + '; inspect EC2 cloud-init logs')

        with ThreadPoolExecutor(max_workers=16) as pool:
            list(pool.map(ready, nodes))
        package = self.directory / 'package'
        if package.exists():
            shutil.rmtree(package)
        (package / 'baseline').mkdir(parents=True)
        algorithm = Path(self.args.algorithm_dir)
        current_hashes = {p.name: sha256(p) for p in sorted(algorithm.glob('*.py'))}
        if self.state['config'].get('algorithm_sha256', current_hashes) != current_hashes:
            raise ValueError('Algorithm changed after planning; finish/clean up this cluster before deploying another revision')
        for filename in BASELINE_FILES:
            if not (algorithm / filename).is_file():
                raise ValueError('Missing algorithm file: ' + filename)
        for path in sorted(algorithm.glob('*.py')):
            shutil.copyfile(path, package / 'baseline' / path.name)
        for filename in ['common.py', 'worker.py', 'dataset.py', 'benchmark.py', 'stage_input.py']:
            shutil.copyfile(HERE / filename, package / filename)
        cluster = {'id': self.state['id'], 'name': self.args.name, 'config': self.state['config'],
                   'baseline_commit': BASELINE_COMMIT, 'nodes': nodes}
        write_json(package / 'cluster.json', cluster)
        (package / '.scp-probe').write_text(self.state['id'])
        known = []
        for node in nodes:
            key = self.public_ssh(node['public_ip'], 'cat /etc/ssh/ssh_host_ed25519_key.pub',
                                  capture_output=True, text=True).stdout.split()
            known.append(node['private_ip'] + ' ' + ' '.join(key[:2]))
        (package / 'peer_known_hosts').write_text('\n'.join(known) + '\n')
        # Personal SSH private key stays on the laptop. Only an ephemeral cluster peer
        # key is distributed for the inherited Worker's SCP operations.
        peer = self.directory / 'peer_key'
        if not peer.exists():
            run(['ssh-keygen', '-q', '-t', 'ed25519', '-N', '', '-f', peer])
        shutil.copyfile(peer, package / 'peer_key')
        peer_public = Path(str(peer) + '.pub').read_text().strip()
        (package / 'peer_key').chmod(0o600)
        archive = self.directory / 'package.tar.gz'
        with tarfile.open(archive, 'w:gz') as tf:
            for path in sorted(package.iterdir()):
                tf.add(path, arcname=path.name)

        def upload(node):
            host = node['public_ip']
            self.public_ssh(host, 'set -e; umask 077; mkdir -p ' + REMOTE_ROOT +
                            '; flock -n ' + REMOTE_ROOT + '/benchmark.lock true')
            self.public_scp(host, archive, REMOTE_ROOT + '/package.tar.gz')
            self.public_ssh(host, 'set -e; cd ' + REMOTE_ROOT + '; tar xzf package.tar.gz; chmod 600 peer_key; rm package.tar.gz')
            self.public_ssh(host, 'set -e; mkdir -p ~/.ssh; chmod 700 ~/.ssh; '
                'grep -qxF ' + shlex.quote(peer_public) + ' ~/.ssh/authorized_keys || '
                'printf "%s\\n" ' + shlex.quote(peer_public) + ' >> ~/.ssh/authorized_keys; chmod 600 ~/.ssh/authorized_keys')

        with ThreadPoolExecutor(max_workers=8) as pool:
            list(pool.map(upload, nodes))
        self.state.update(phase='deployed', nodes=nodes, package_sha256=sha256(archive))
        self.save()
        self.preflight()
        print('Deployed and SSH mesh preflight passed on %d workers.' % len(nodes))

    def preflight(self):
        nodes = self.nodes()
        # Every source tests every destination, sequential per source; exercise self-SCP too.
        script = """import json,subprocess,sys,tempfile,os
sys.path.insert(0, __REMOTE_ROOT__)
from common import ssh_options
root=__REMOTE_ROOT__+'/'
c=json.load(open(root+'cluster.json'))
for n in c['nodes']:
 fd,path=tempfile.mkstemp(prefix='hazbench-probe-')
 os.close(fd)
 try:
  subprocess.run(['scp','-O']+ssh_options(root+'peer_key',root+'peer_known_hosts')+['ec2-user@'+n['private_ip']+':'+root+'.scp-probe',path],check=True,timeout=30)
  assert open(path).read()==c['id'], 'SCP probe contents differ'
 finally:
  os.unlink(path)
print('mesh-ok')
""".replace('__REMOTE_ROOT__', repr(REMOTE_ROOT))
        def check(node):
            self.public_ssh(node['public_ip'], 'python3.13 -c ' + shlex.quote(script), timeout=60 * len(nodes))
        with ThreadPoolExecutor(max_workers=8) as pool:
            list(pool.map(check, nodes))

    def start(self):
        if self.state.get('phase') != 'deployed':
            raise RuntimeError('Run deploy first')
        experiment = self.args.experiment or time.strftime('wc-%Y%m%dT%H%M%SZ', time.gmtime())
        if not re.fullmatch(r'[A-Za-z0-9][A-Za-z0-9_-]{0,79}', experiment):
            raise ValueError('Use a simple experiment name (letters, digits, _ and -; max 80 characters)')
        is_s3 = self.args.input.startswith('s3://')
        source = Path(self.args.input).resolve() if not is_s3 else None
        if not is_s3 and not source.is_file():
            raise ValueError('Input is not a local file')
        nodes = self.nodes()
        driver = nodes[0]['public_ip']
        self.public_ssh(driver, 'set -e; mkdir -p ' + REMOTE_ROOT + '/sources ' + REMOTE_ROOT + '/logs; '
                        'flock -n ' + REMOTE_ROOT + '/benchmark.lock true; test ! -e ' + REMOTE_ROOT + '/results/' + experiment)
        if is_s3:
            sys.path.insert(0, str(PROJECT))
            from data.s3_dataset import signed_plan, inspect
            uri, manifest = inspect(self.args.input, self.state['config']['region'], self.state['config'].get('profile'))
            digest = manifest['dataset']['input_sha256']
            plan = signed_plan(uri, manifest, len(nodes), self.state['config']['region'], self.state['config'].get('profile'))
            private_plan = self.directory / 's3-plan.private.json'
            write_json(private_plan, plan)
            private_plan.chmod(0o600)
            remote_plan = REMOTE_ROOT + '/sources/' + digest + '-s3-plan.json'
            try:
                self.public_scp(driver, private_plan, remote_plan)
                self.public_ssh(driver, 'chmod 600 ' + remote_plan)
            finally:
                private_plan.unlink(missing_ok=True)
            remote_source = 's3-direct'
        else:
            digest = sha256(source)
            remote_source = REMOTE_ROOT + '/sources/' + digest + '.txt'
            self.public_scp(driver, source, remote_source)
            remote_hash = self.public_ssh(driver, 'sha256sum ' + remote_source, capture_output=True, text=True).stdout.split()[0]
            if remote_hash != digest:
                raise RuntimeError('source upload checksum mismatch')
        if self.state['config'].get('cpu_credits'):
            credits = self.aws('ec2', 'describe-instance-credit-specifications',
                               instance_ids=self.state['instance_ids'])['InstanceCreditSpecifications']
            if (len(credits) != len(nodes) or
                    any(i['CpuCredits'] != self.state['config']['cpu_credits'] for i in credits)):
                raise RuntimeError('Actual EC2 credit mode differs from course configuration')
            self.state['credit_specifications_verified'] = credits
            self.save()
        command = ['python3.13', REMOTE_ROOT + '/benchmark.py', '--source', remote_source,
                   '--experiment', experiment, '--repeat', str(self.args.repeat),
                   '--repetitions', str(self.args.repetitions), '--timeout', str(self.args.timeout)]
        if is_s3:
            command += ['--s3-plan', remote_plan]
        elif getattr(self.args, 'oracle', None):
            oracle = Path(self.args.oracle).resolve()
            remote_oracle = REMOTE_ROOT + '/sources/' + digest + '.oracle.sqlite'
            self.public_scp(driver, oracle, remote_oracle)
            remote_oracle_sha = self.public_ssh(driver, 'sha256sum ' + remote_oracle,
                                               capture_output=True, text=True).stdout.split()[0]
            if remote_oracle_sha != sha256(oracle):
                raise RuntimeError('Oracle upload checksum mismatch')
            command += ['--oracle', remote_oracle]
        if self.args.retain_intermediates:
            command.append('--retain-intermediates')
        # Detached process survives laptop disconnect; explicit redirection avoids SSH holding pipes open.
        launch = background_command(command, experiment)
        self.state.setdefault('experiments', {})[experiment] = {'pid': None, 'source_sha256': digest,
            'repeat': self.args.repeat, 'repetitions': self.args.repetitions, 'started_utc': utc(), 'launch_status': 'requested'}
        self.state['latest_experiment'] = experiment
        self.save()
        pid = self.public_ssh(driver, launch, capture_output=True, text=True).stdout.strip()
        self.state['experiments'][experiment].update(pid=pid, launch_status='submitted')
        self.save()
        print('Started detached experiment %s, coordinator PID %s. Use status/logs/fetch.' % (experiment, pid))

    def experiment(self):
        name = getattr(self.args, 'experiment', None) or self.state.get('latest_experiment')
        if not name or not re.fullmatch(r'[A-Za-z0-9][A-Za-z0-9_-]{0,79}', name):
            raise ValueError('Specify --experiment or start an experiment first')
        return name

    def status(self):
        instances = self.discover_instances()
        print(json.dumps({'phase': self.state.get('phase'), 'region': self.state['config']['region'],
            'instances': [{'id': i['InstanceId'], 'state': i['State']['Name'], 'public_ip': i.get('PublicIpAddress'),
                           'launch_time': i['LaunchTime']} for i in instances]}, indent=2))
        if (getattr(self.args, 'experiment', None) or self.state.get('latest_experiment')) and self.state.get('nodes'):
            name = self.experiment()
            self.public_ssh(self.state['nodes'][0]['public_ip'],
                'if test -f ' + REMOTE_ROOT + '/results/' + name + '/status.json; then cat ' +
                REMOTE_ROOT + '/results/' + name + '/status.json; else echo "Experiment has not written status yet; check logs."; fi', timeout=30)

    def logs(self):
        name = self.experiment()
        self.public_ssh(self.nodes()[0]['public_ip'], 'tail -n 80 ' + REMOTE_ROOT + '/logs/' + name + '.log', timeout=30)

    def fetch(self):
        name = self.experiment()
        driver = self.nodes()[0]['public_ip']
        status = json.loads(self.public_ssh(driver, 'cat ' + REMOTE_ROOT + '/results/' + name + '/status.json',
                                           capture_output=True, text=True).stdout)
        if status['status'] not in ('success', 'failed'):
            raise RuntimeError('Experiment is still running; use status/logs, then fetch after it finishes')
        remote = REMOTE_ROOT + '/' + name + '-results.tar.gz'
        # SQLite reference and input shards are reproducible and can be huge; retain the
        # manifest + exact expected.jsonl instead. All raw worker outputs/metrics are kept.
        self.public_ssh(driver, 'cd ' + REMOTE_ROOT +
            ' && tar --exclude="*.sqlite" --exclude="shard-*.txt" -czf ' + remote +
            ' results/' + name + ' logs/' + name + '.log', timeout=7200)
        digest = self.public_ssh(driver, 'sha256sum ' + remote, capture_output=True, text=True).stdout.split()[0]
        local = self.directory / (name + '-results.tar.gz')
        self.public_scp(driver, remote, local, download=True)
        if sha256(local) != digest:
            raise RuntimeError('Downloaded archive checksum mismatch; do not rely on this copy')
        destination = self.directory / 'downloaded' / name
        destination.mkdir(parents=True, exist_ok=True)
        with tarfile.open(local) as tf:
            for member in tf.getmembers():
                path = (destination / member.name).resolve()
                if not path.is_relative_to(destination.resolve()) or member.issym() or member.islnk():
                    raise RuntimeError('Unsafe archive member')
            tf.extractall(destination)
        record = {'archive': str(local), 'sha256': digest, 'downloaded_utc': utc(), 'directory': str(destination)}
        from credit_metrics import collect
        metrics = collect(self.state['config'], self.state['instance_ids'],
                          self.state['experiments'][name]['started_utc'], utc())
        write_json(destination / 'results' / name / 'cpu-credit-metrics.json', metrics)
        self.state.setdefault('fetched', {})[name] = record
        self.save()
        print(json.dumps(record, indent=2))

    def destroy(self):
        if not self.args.yes:
            raise ValueError('Use destroy --yes to terminate ONLY this named cluster')
        config = self.state['config']
        instances = self.discover_instances()
        ids = [i['InstanceId'] for i in instances if i['State']['Name'] != 'terminated']
        if ids:
            self.aws('ec2', 'terminate-instances', instance_ids=ids)
            deadline = time.monotonic() + 900
            while time.monotonic() < deadline:
                current = self.discover_instances()
                if all(i['State']['Name'] == 'terminated' for i in current):
                    break
                time.sleep(10)
            else:
                raise RuntimeError('Termination not confirmed; run status and destroy again')
        if self.state.get('group_name'):
            groups = self.aws('ec2', 'describe-security-groups', filters=[
                {'Name': 'group-name', 'Values': [self.state['group_name']]},
                {'Name': 'vpc-id', 'Values': [config['vpc_id']]}])['SecurityGroups']
            for group in groups:
                # Remove the self reference before deleting the group.
                if group['IpPermissions']:
                    self.aws('ec2', 'revoke-security-group-ingress', group_id=group['GroupId'], ip_permissions=group['IpPermissions'])
                self.aws('ec2', 'delete-security-group', group_id=group['GroupId'])
        if self.state.get('key_name'):
            self.aws('ec2', 'delete-key-pair', key_name=self.state['key_name'])
        self.state.update(phase='terminated', terminated_utc=utc())
        self.save()
        print('Confirmed terminated. Root EBS uses DeleteOnTermination; fetched local results are retained.')


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--name', required=True, help='local cluster name, e.g. wc4')
    parser.add_argument('--region', default='us-east-1')
    parser.add_argument('--profile', default=None)
    sub = parser.add_subparsers(dest='command', required=True)
    plan = sub.add_parser('plan', help='AWS read-only preflight; saves local plan, creates no cloud resources')
    plan.add_argument('--workers', type=int, choices=[4, 8, 16], required=True)
    plan.add_argument('--subnet-id', required=True)
    plan.add_argument('--ssh-cidr', required=True)
    plan.add_argument('--instance-type', default='t8i.small')
    plan.add_argument('--cpu-credits', choices=['unlimited'], default='unlimited')
    plan.add_argument('--algorithm-dir', default=str(PROJECT / 'baseline'))
    plan.add_argument('--implementation', choices=['baseline', 'student'], default='baseline')
    plan.add_argument('--disk-gib', type=int, default=100)
    plan.add_argument('--ami', help='reuse the resolved AL2023 AMI ID for subsequent clusters')
    plan.add_argument('--ssh-key', default=os.environ.get('WC_SSH_KEY'), help='existing local private key; never uploaded')
    for name in ['launch', 'deploy', 'preflight']:
        sub.add_parser(name)
    start = sub.add_parser('start')
    start.add_argument('--input', required=True, help='local UTF-8 plain text corpus')
    start.add_argument('--experiment')
    start.add_argument('--repeat', type=int, default=1, help='synthetic corpus replication factor')
    start.add_argument('--repetitions', type=int, default=3)
    start.add_argument('--timeout', type=int, default=7200)
    start.add_argument('--retain-intermediates', action='store_true')
    for name in ['status', 'logs', 'fetch']:
        cmd = sub.add_parser(name)
        cmd.add_argument('--experiment')
    sub.add_parser('destroy').add_argument('--yes', action='store_true')
    args = parser.parse_args()
    if not re.fullmatch(r'[A-Za-z0-9][A-Za-z0-9_-]{0,59}', args.name):
        parser.error('Use a simple cluster name, max 60 characters')
    for key in ['repeat', 'repetitions', 'timeout', 'disk_gib']:
        if hasattr(args, key) and getattr(args, key) < 1:
            parser.error(key + ' must be positive')
    cluster = Cluster(args)
    lock = (cluster.directory / '.local.lock').open('w')
    try:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        getattr(cluster, args.command)()
    except Exception as exc:
        print('ERROR: ' + str(exc), file=sys.stderr)
        if isinstance(exc, subprocess.CalledProcessError) and exc.stderr:
            print(exc.stderr, file=sys.stderr)
        print('State retained in ' + str(cluster.directory) + '. If launch already ran, inspect status/destroy; do not blindly relaunch.', file=sys.stderr)
        sys.exit(1)
    finally:
        lock.close()


if __name__ == '__main__':
    main()
