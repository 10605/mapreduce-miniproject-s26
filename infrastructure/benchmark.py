"""Remote coordinator, launched detached by cluster.py on worker 0."""
import argparse
from concurrent.futures import ThreadPoolExecutor, as_completed
import csv
import fcntl
import json
import os
from pathlib import Path
import shlex
import shutil
import statistics
import subprocess
import sys
import time
import traceback

from common import BASELINE_COMMIT, read_json, write_json, run, ssh_options, utc, hash_sources, sha256
from dataset import prepare, validate
from stage_input import stage

ROOT = Path(__file__).resolve().parent


class Transport:
    def __init__(self, cluster):
        self.cluster = cluster
        self.options = ssh_options(ROOT / 'peer_key', ROOT / 'peer_known_hosts')

    def command(self, index, command, timeout=7500, **kwargs):
        args = ['bash', '-lc', command] if index == 0 else ['ssh'] + self.options + [
            'ec2-user@' + self.cluster['nodes'][index]['private_ip'], command]
        return run(args, timeout=timeout, **kwargs)

    def put(self, index, source, destination):
        if index == 0:
            shutil.copyfile(source, destination)
        else:
            run(['scp', '-O'] + self.options + [str(source),
                'ec2-user@' + self.cluster['nodes'][index]['private_ip'] + ':' + str(destination)], timeout=7200)

    def get(self, index, source, destination):
        if index == 0:
            shutil.copyfile(source, destination)
        else:
            run(['scp', '-O'] + self.options + ['ec2-user@' + self.cluster['nodes'][index]['private_ip'] + ':' + str(source),
                str(destination)], timeout=7200, stdout=subprocess.DEVNULL)


def parallel(count, task):
    failures = []
    results = [None] * count
    with ThreadPoolExecutor(max_workers=count) as pool:
        tasks = {pool.submit(task, i): i for i in range(count)}
        for future in as_completed(tasks):
            i = tasks[future]
            try:
                results[i] = future.result()
            except Exception as exc:
                failures.append({'worker': i, 'error': repr(exc)})
    return results, failures


def launch_phase(transport, count, phase, workdir, output, timeout):
    start = time.monotonic()

    def invoke(index):
        with (output / ('w%02d-%s.log' % (index, phase))).open('wb') as fp:
            transport.command(index, shlex.join(['python3.13', str(ROOT / 'worker.py'), phase,
                '--index', str(index), '--workdir', str(workdir), '--timeout', str(timeout)]),
                timeout=timeout + 120, stdout=fp, stderr=subprocess.STDOUT)

    _, errors = parallel(count, invoke)
    barrier_seconds = time.monotonic() - start
    return {'barrier_wall_seconds': barrier_seconds, 'workers': [], 'errors': errors}


def collect_phase(transport, count, phase, workdir, output, result):
    def collect(i):
        for suffix in ('.json', '_samples.json'):
            target = output / ('w%02d-%s%s' % (i, phase, suffix))
            try:
                transport.get(i, workdir / (phase + suffix), target)
                if suffix == '.json':
                    metric = read_json(target)
                    if metric['status'] != 'success':
                        raise RuntimeError(metric.get('error'))
            except Exception as exc:
                raise RuntimeError('worker %s phase %s collection: %r' % (i, phase, exc)) from exc
        return read_json(output / ('w%02d-%s.json' % (i, phase)))
    metrics, errors = parallel(count, collect)
    result['workers'] = [m for m in metrics if m is not None]
    result['errors'].extend(errors)


def run_one(cluster, transport, dataset_dir, manifest, output, run_id, timeout, retain, s3_plan=None):
    output.mkdir(parents=True)
    count = len(cluster['nodes'])
    workdir = ROOT / 'jobs' / run_id
    result = {'run_id': run_id, 'workers': count, 'status': 'running', 'started_utc': utc(),
              'input_mode': 's3-direct' if s3_plan else 'local-scp', 'input_staging': [None] * count,
              'cache_policy': 'OS-default; no cache flushing; new intermediate files per run',
              'errors': []}
    write_json(output / 'result.json', result)
    distribution_start = time.monotonic()

    def distribute(index):
        transport.command(index, 'mkdir -p ' + shlex.quote(str(workdir)))
        if s3_plan:
            local_plan = dataset_dir / ('download-w%02d.private.json' % index)
            write_json(local_plan, dict(s3_plan['nodes'][index]))
            local_plan.chmod(0o600)
            try:
                transport.put(index, local_plan, workdir / '.input-download.json')
                transport.command(index, 'chmod 600 ' + shlex.quote(str(workdir / '.input-download.json')))
            finally:
                local_plan.unlink(missing_ok=True)
            downloaded = transport.command(index, shlex.join(['python3.13', str(ROOT / 'stage_input.py'),
                '--plan', str(workdir / '.input-download.json'), '--target', str(workdir / 'input.txt')]),
                timeout=7200, capture_output=True, text=True)
            result['input_staging'][index] = json.loads(downloaded.stdout)
        else:
            transport.put(index, dataset_dir / ('shard-%02d.txt' % index), workdir / 'input.txt')
        # Verify bytes actually delivered, before starting the timed job.
        text = transport.command(index, 'sha256sum ' + shlex.quote(str(workdir / 'input.txt')),
                                 capture_output=True, text=True).stdout
        if text.split()[0] != manifest['shards'][index]['sha256']:
            raise RuntimeError('input shard checksum mismatch')

    try:
        _, errors = parallel(count, distribute)
        result['distribute_input_seconds'] = time.monotonic() - distribution_start
        if errors:
            result['errors'].extend(errors)
            raise RuntimeError('input distribution failed')
        write_json(output / 'result.json', result)
        print('Input staged on %d workers in %.3f seconds (%s)' %
              (count, result['distribute_input_seconds'], result['input_mode']), flush=True)
        compute_start = time.monotonic()
        result['map_sort'] = launch_phase(transport, count, 'map_sort', workdir, output, timeout)
        if result['map_sort']['errors']:
            collect_phase(transport, count, 'map_sort', workdir, output, result['map_sort'])
            raise RuntimeError('map/sort failed; reduce was not started')
        print('Map/sort completed in %.3f seconds; starting gather/reduce' % result['map_sort']['barrier_wall_seconds'], flush=True)
        result['gather_reduce'] = launch_phase(transport, count, 'gather_reduce', workdir, output, timeout)
        result['job_wall_seconds'] = time.monotonic() - compute_start
        print('MapReduce job completed in %.3f seconds; collecting outputs and validating' % result['job_wall_seconds'], flush=True)
        # Collect metrics after BOTH computation barriers so SCP collection does
        # not add an extra transfer stage between map and reduce.
        for phase in ('map_sort', 'gather_reduce'):
            collect_phase(transport, count, phase, workdir, output, result[phase])
        if result['map_sort']['errors'] or result['gather_reduce']['errors']:
            raise RuntimeError('gather/reduce failed')
        fetch_start = time.monotonic()
        counts_dir = output / 'worker_outputs'
        counts_dir.mkdir()

        def collect(index):
            target = counts_dir / ('w%02d-counts.txt' % index)
            transport.get(index, workdir / 'counts.txt', target)
            metric = next(m for m in result['gather_reduce']['workers'] if m['index'] == index)
            if sha256(target) != metric['files']['counts.txt']['sha256']:
                raise RuntimeError('result transfer checksum mismatch')

        _, errors = parallel(count, collect)
        result['collect_results_seconds'] = time.monotonic() - fetch_start
        if errors:
            result['errors'].extend(errors)
            raise RuntimeError('result collection failed')
        result['pipeline_wall_seconds'] = time.monotonic() - distribution_start
        result['validation'] = validate(dataset_dir / 'expected.sqlite', sorted(counts_dir.glob('*.txt')), output)
        if not result['validation']['correct']:
            raise RuntimeError('WordCount failed exact correctness validation')
        phase_metrics = result['map_sort']['workers'] + result['gather_reduce']['workers']
        result['user_cpu_seconds_sum'] = sum(m['user_cpu_seconds'] for m in phase_metrics)
        result['system_cpu_seconds_sum'] = sum(m['system_cpu_seconds'] for m in phase_metrics)
        result['worker_peak_rss_kib_max'] = max(m['worker_peak_rss_kib'] for m in phase_metrics)
        result['largest_child_peak_rss_kib_max'] = max(m['largest_child_peak_rss_kib'] for m in phase_metrics)
        result['input_mib_per_second'] = manifest['input_bytes'] / 1024**2 / result['job_wall_seconds']
        result['tokens_per_second'] = manifest['tokens'] / result['job_wall_seconds']
        result['mapper_partition_bytes'] = sum(v['bytes'] for m in result['map_sort']['workers']
            for name, v in m['files'].items() if name.startswith('mapout-input.txt-for-'))
        result['worker_cpu_node_capacity_percent_mean'] = statistics.mean(m['cpu_percent_node_capacity'] for m in phase_metrics)
        result['status'] = 'success'
    except Exception as exc:
        result.update(status='failed', error=repr(exc), traceback=traceback.format_exc())
    finally:
        if not retain and result['status'] == 'success':
            # All output/checksums/metrics are now on the coordinator. Only this run is removed.
            _, cleanup_errors = parallel(count, lambda i: transport.command(i, 'rm -rf -- ' + shlex.quote(str(workdir))))
            result['cleanup_errors'] = cleanup_errors
        result['finished_utc'] = utc()
        write_json(output / 'result.json', result)
    return result


CSV_FIELDS = ['run_id', 'workers', 'status', 'job_wall_seconds', 'pipeline_wall_seconds',
              'distribute_input_seconds', 'collect_results_seconds', 'input_mib_per_second',
              'tokens_per_second', 'user_cpu_seconds_sum', 'system_cpu_seconds_sum',
              'worker_peak_rss_kib_max', 'largest_child_peak_rss_kib_max', 'mapper_partition_bytes']


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--source', required=True)
    parser.add_argument('--oracle')
    parser.add_argument('--s3-plan')
    parser.add_argument('--experiment', required=True)
    parser.add_argument('--repeat', type=int, default=1)
    parser.add_argument('--repetitions', type=int, default=3)
    parser.add_argument('--timeout', type=int, default=7200, help='per worker, per phase')
    parser.add_argument('--retain-intermediates', action='store_true')
    args = parser.parse_args()
    if args.repeat < 1 or args.repetitions < 1 or args.timeout < 1:
        parser.error('repeat, repetitions, timeout must be positive')
    output = ROOT / 'results' / args.experiment
    output.mkdir(parents=True, exist_ok=False)
    status = {'status': 'running', 'started_utc': utc(), 'args': vars(args)}
    write_json(output / 'status.json', status)
    # Prevent concurrent benchmarks on the same cluster, including different experiment names.
    lock = (ROOT / 'benchmark.lock').open('w')
    try:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        cluster = read_json(ROOT / 'cluster.json')
        count = len(cluster['nodes'])
        transport = Transport(cluster)
        write_json(output / 'provenance.json', {'baseline_commit': BASELINE_COMMIT, 'source_sha256': hash_sources(ROOT),
            'cluster': cluster, 'controller_runs_on_worker': 0, 'pythonhashseed': os.environ.get('PYTHONHASHSEED'),
            'measurement_note': 'Course harness; implementation and algorithm hashes are recorded in cluster.config. Both phases and their shuffle I/O are timed.'})

        def inventory(i):
            p = transport.command(i, shlex.join(['python3.13', str(ROOT / 'worker.py'), 'inventory']),
                                  capture_output=True, text=True)
            value = json.loads(p.stdout)
            if value['free_disk_bytes'] < 10 * 1024**3:
                raise RuntimeError('worker %s has less than 10 GiB free' % i)
            return value

        inventories, errors = parallel(count, inventory)
        write_json(output / 'inventory.json', inventories)
        if errors:
            raise RuntimeError('preflight inventory failed: ' + repr(errors))
        if len({json.dumps(v['baseline_sha256'], sort_keys=True) for v in inventories}) != 1:
            raise RuntimeError('workers have inconsistent baseline code')
        if len({v['python'] for v in inventories}) != 1:
            raise RuntimeError('workers have inconsistent Python versions')
        if any(not v['python'].startswith('3.13.') or v['unicode_version'] != '15.1.0' for v in inventories):
            raise RuntimeError('Course oracle requires Python 3.13 / Unicode 15.1.0 on every worker')
        dataset_dir = output / 'dataset'
        preparation_start = time.monotonic()
        s3_plan = None
        if args.s3_plan:
            s3_plan = read_json(args.s3_plan)
            Path(args.s3_plan).unlink()
            if s3_plan['workers'] != count:
                raise ValueError('S3 input plan worker count differs')
            dataset_dir.mkdir()
            item = s3_plan['oracle']
            stage({'objects': [item], 'sha256': item['sha256'], 'bytes': item['bytes']}, dataset_dir / 'expected.sqlite')
            m = s3_plan['dataset']
            manifest = {'source_sha256': m['input_sha256'], 'expanded_normalized_sha256': m['input_sha256'],
                'input_bytes': m['input_bytes'], 'tokens': m['tokens'], 'vocabulary': m['vocabulary'],
                'workers': count, 'repeat': 1, 'expected_sha256': m['expected_sha256'],
                'sharding': '16 canonical contiguous byte ranges, whole LF records, consecutive groups per worker',
                's3_source_uri': s3_plan['source_uri'],
                'shards': [{'file': 'shard-%02d.txt' % i, 'bytes': node['bytes'], 'sha256': node['sha256']}
                           for i, node in enumerate(s3_plan['nodes'])]}
            write_json(dataset_dir / 'manifest.json', manifest)
            write_json(dataset_dir / 's3-manifest.json', s3_plan['public_manifest'])
        else:
            manifest = prepare(args.source, dataset_dir, count, args.repeat, oracle=args.oracle)
        status['dataset_preparation_seconds'] = time.monotonic() - preparation_start
        status['dataset'] = {'input_bytes': manifest['input_bytes'], 'tokens': manifest['tokens']}
        write_json(output / 'status.json', status)
        print('Dataset prepared: %d bytes, %d tokens, %d workers' %
              (manifest['input_bytes'], manifest['tokens'], count), flush=True)
        runs = []
        for repetition in range(args.repetitions):
            name = args.experiment + '-r%02d' % (repetition + 1)
            status['current_repetition'] = repetition + 1
            write_json(output / 'status.json', status)
            print('Starting ' + name, flush=True)
            result = run_one(cluster, transport, dataset_dir, manifest, output / name,
                             name, args.timeout, args.retain_intermediates, s3_plan=s3_plan)
            runs.append(result)
            print('%s: %s, job wall seconds=%s' % (name, result['status'], result.get('job_wall_seconds')), flush=True)
            with (output / 'summary.csv').open('w', newline='') as fp:
                writer = csv.DictWriter(fp, fieldnames=CSV_FIELDS, extrasaction='ignore')
                writer.writeheader()
                writer.writerows(runs)
            if result['status'] != 'success':
                raise RuntimeError('failed repetition; see ' + name + '/result.json')
        times = [r['job_wall_seconds'] for r in runs]
        write_json(output / 'aggregate.json', {'workers': count, 'successful_repetitions': len(times),
            'job_wall_seconds_median': statistics.median(times), 'job_wall_seconds_min': min(times),
            'job_wall_seconds_max': max(times), 'job_wall_seconds_stdev': statistics.stdev(times) if len(times) > 1 else None,
            'input_sha256': manifest['expanded_normalized_sha256'], 'input_bytes': manifest['input_bytes']})
        status['status'] = 'success'
    except Exception as exc:
        status.update(status='failed', error=repr(exc), traceback=traceback.format_exc())
    finally:
        status['finished_utc'] = utc()
        write_json(output / 'status.json', status)
        lock.close()
    print(json.dumps(status), flush=True)
    if status['status'] != 'success':
        sys.exit(1)


if __name__ == '__main__':
    main()
