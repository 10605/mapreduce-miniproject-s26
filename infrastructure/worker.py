"""Run UNCHANGED upstream SeqWorker methods with deployment/measurement adapters."""
import argparse
import os
from pathlib import Path
import platform
import resource
import shlex
import signal
import socket
import subprocess
import sys
import threading
import time
import traceback
import unicodedata

from common import read_json, write_json, sha256, ssh_options, utc

ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(ROOT / 'baseline'))
import hz_worker
from wc_hz import WordCount


class ConfiguredWordCount(WordCount):
    """Only adapt cluster identity and SSH options; algorithm methods are inherited."""
    def __init__(self, cluster, index):
        self.workers = [n['private_ip'] for n in cluster['nodes']]
        self.internal2external = {socket.gethostname(): self.workers[index]}
        self.cloud_username = 'ec2-user'
        self.keypair_file = str(ROOT / 'peer_key')

    def ssh_args(self):
        return shlex.join(ssh_options(self.keypair_file, ROOT / 'peer_known_hosts') + ['-l', self.cloud_username])

    def scp_args(self):
        # Legacy SCP protocol gives consistent semantics across OpenSSH versions.
        return shlex.join(['-O'] + ssh_options(self.keypair_file, ROOT / 'peer_known_hosts'))

    def _shard_bufname_by_recipient(self, src, worker_idx):
        # SCP resolves relative remote paths against $HOME, not the mapper's cwd.
        # Every node uses the same absolute per-run directory.
        return str(Path.cwd() / super()._shard_bufname_by_recipient(src, worker_idx))


def system_snapshot():
    """Node-wide counters; never claim these are task-only counters."""
    if not Path('/proc/stat').exists():
        return {'available': False}
    cpu = list(map(int, Path('/proc/stat').read_text().splitlines()[0].split()[1:9]))
    memory = {}
    for line in Path('/proc/meminfo').read_text().splitlines():
        key, val = line.split(':', 1)
        if key in ('MemTotal', 'MemAvailable'):
            memory[key + '_kib'] = int(val.split()[0])
    networks = {}
    for line in Path('/proc/net/dev').read_text().splitlines()[2:]:
        name, data = line.split(':', 1)
        if name.strip() != 'lo':
            values = list(map(int, data.split()))
            networks[name.strip()] = {'rx_bytes': values[0], 'tx_bytes': values[8]}
    disks = {}
    for line in Path('/proc/diskstats').read_text().splitlines():
        values = line.split()
        name = values[2]
        # Exclude partitions and virtual loop devices; retain whole NVMe/xvd disks.
        if (name.startswith('nvme') and name.endswith('n1')) or (name.startswith('xvd') and name[-1].isalpha()):
            disks[name] = {'read_bytes': int(values[5]) * 512,
                           'write_bytes': int(values[9]) * 512}
    return {'available': True, 'monotonic_seconds': time.monotonic(),
            'cpu_jiffies': cpu, 'memory': memory, 'network': networks, 'disk': disks}


def node_delta(before, after):
    if not before.get('available') or not after.get('available'):
        return None
    ticks = [b - a for a, b in zip(before['cpu_jiffies'], after['cpu_jiffies'])]
    total = sum(ticks)
    result = {'scope': 'whole_node_including_SSH_OS_and_controller',
              'cpu_busy_percent': 100 * (total - ticks[3] - ticks[4]) / total if total else None,
              'cpu_iowait_percent': 100 * ticks[4] / total if total else None}
    for category in ('network', 'disk'):
        result[category] = {device: {k: val - before[category].get(device, {}).get(k, val)
                           for k, val in values.items()}
                           for device, values in after[category].items()}
    return result


def usage():
    own, child = resource.getrusage(resource.RUSAGE_SELF), resource.getrusage(resource.RUSAGE_CHILDREN)
    scale = 1024 if sys.platform == 'darwin' else 1
    return {'user': own.ru_utime + child.ru_utime, 'system': own.ru_stime + child.ru_stime,
            'worker_peak_rss_kib': own.ru_maxrss / scale,
            'largest_child_peak_rss_kib': child.ru_maxrss / scale,
            'major_faults': own.ru_majflt + child.ru_majflt,
            'voluntary_context_switches': own.ru_nvcsw + child.ru_nvcsw,
            'involuntary_context_switches': own.ru_nivcsw + child.ru_nivcsw}


def measured_phase(cluster, index, phase, workdir, timeout, interval=1.0, copy_hook=None):
    """copy_hook is only for offline tests; production uses real subprocesses."""
    workdir = Path(workdir).resolve()
    workdir.mkdir(parents=True, exist_ok=True)
    previous = Path.cwd()
    stop = threading.Event()
    records = []
    child_calls = []
    status = {'status': 'running', 'phase': phase, 'index': index, 'started_utc': utc()}
    before, before_usage = system_snapshot(), usage()
    records.append(before)
    start = time.monotonic()
    deadline = start + timeout

    def sampler():
        while not stop.wait(interval):
            records.append(system_snapshot())

    def checked_command(command, **kwargs):
        t0 = time.monotonic()
        item = {'command': command, 'kind': 'scp' if command.startswith('scp ') else
                ('merge' if ' -m ' in command else 'sort')}
        child_calls.append(item)
        proc = None
        try:
            if copy_hook and command.startswith('scp '):
                copy_hook(command)
            else:
                proc = subprocess.Popen(command, start_new_session=True, **kwargs)
                rc = proc.wait(timeout=max(0.01, deadline - time.monotonic()))
                if rc:
                    raise subprocess.CalledProcessError(rc, command)
            item['exit_code'] = 0
        except BaseException:
            # A subprocess deadline can expire just before SIGALRM. Cancel the
            # second timer before cleanup/traceback to avoid masking the failure.
            signal.alarm(0)
            if proc and proc.poll() is None:
                os.killpg(proc.pid, signal.SIGKILL)
                proc.wait()
            item['exit_code'] = proc.returncode if proc else -1
            raise
        finally:
            item['wall_seconds'] = time.monotonic() - t0

    def timed_out(signum, frame):
        raise TimeoutError('worker phase exceeded timeout')

    # Alarm also bounds the Python partition/reduce loops, not just SSH/sort calls.
    old_handler = signal.signal(signal.SIGALRM, timed_out)
    old_check_call = hz_worker.check_call
    hz_worker.check_call = checked_command
    thread = threading.Thread(target=sampler, daemon=True)
    thread.start()
    signal.alarm(timeout)
    try:
        os.chdir(workdir)
        worker = ConfiguredWordCount(cluster, index)
        if phase == 'map_sort':
            worker.do_map_and_shuffle('input.txt')
        elif phase == 'gather_reduce':
            worker.do_gather_reduce('input.txt', 'counts.txt')
        else:
            raise ValueError(phase)
        status['status'] = 'success'
    except BaseException as exc:
        signal.alarm(0)
        status.update(status='failed', error=repr(exc), traceback=traceback.format_exc())
    finally:
        signal.alarm(0)
        signal.signal(signal.SIGALRM, old_handler)
        hz_worker.check_call = old_check_call
        os.chdir(previous)
        stop.set()
        thread.join()
        end = system_snapshot()
        records.append(end)
        elapsed = time.monotonic() - start
        after_usage = usage()
        user = after_usage['user'] - before_usage['user']
        system = after_usage['system'] - before_usage['system']
        status.update(finished_utc=utc(), wall_seconds=elapsed,
                      user_cpu_seconds=user, system_cpu_seconds=system,
                      cpu_percent_one_core=100 * (user + system) / elapsed,
                      cpu_percent_node_capacity=100 * (user + system) / elapsed / (os.cpu_count() or 1),
                      worker_peak_rss_kib=after_usage['worker_peak_rss_kib'],
                      largest_child_peak_rss_kib=after_usage['largest_child_peak_rss_kib'],
                      major_faults=after_usage['major_faults'] - before_usage['major_faults'],
                      node_counters=node_delta(before, end), subprocesses=child_calls)
        status['subprocess_wall_seconds_by_kind'] = {kind: sum(c['wall_seconds'] for c in child_calls if c['kind'] == kind)
                                                    for kind in ('sort', 'merge', 'scp')}
        status['python_and_wrapper_wall_seconds'] = max(0, elapsed - sum(c['wall_seconds'] for c in child_calls))
        available = [s['memory']['MemAvailable_kib'] for s in records if s.get('available')]
        status['node_mem_available_kib_min_sampled'] = min(available) if available else None
        status['files'] = {p.name: {'bytes': p.stat().st_size} for p in workdir.glob('*.tsv')}
        for name in ('input.txt', 'counts.txt'):
            p = workdir / name
            if p.exists():
                status['files'][name] = {'bytes': p.stat().st_size}
                if name == 'counts.txt':
                    status['files'][name]['sha256'] = sha256(p)
        write_json(workdir / (phase + '.json'), status)
        write_json(workdir / (phase + '_samples.json'), records)
    return status


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('phase', choices=['map_sort', 'gather_reduce', 'inventory'])
    parser.add_argument('--index', type=int, default=0)
    parser.add_argument('--workdir')
    parser.add_argument('--timeout', type=int, default=7200)
    args = parser.parse_args()
    if args.phase == 'inventory':
        stat = os.statvfs(ROOT)
        value = {'hostname': socket.gethostname(), 'platform': platform.platform(),
                 'python': sys.version, 'unicode_version': unicodedata.unidata_version, 'logical_cpus': os.cpu_count(),
                 'sort_version': subprocess.check_output(['sort', '--version'], text=True).splitlines()[0],
                 'free_disk_bytes': stat.f_bavail * stat.f_frsize,
                 'system': system_snapshot(), 'baseline_sha256': {p.name: sha256(p) for p in (ROOT / 'baseline').glob('*.py')}}
        print(__import__('json').dumps(value))
        return
    result = measured_phase(read_json(ROOT / 'cluster.json'), args.index, args.phase,
                            args.workdir, args.timeout)
    if result['status'] != 'success':
        print(result['traceback'], file=sys.stderr)
        sys.exit(1)


if __name__ == '__main__':
    main()
