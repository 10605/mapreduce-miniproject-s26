"""Compare only successful, compatible baseline experiments; no invented AWS data."""
import argparse
import csv
import json
from pathlib import Path
import statistics

from common import read_json, write_json


def summarize(directories, destination):
    experiments = []
    fingerprints = []
    for directory in map(Path, directories):
        if read_json(directory / 'status.json')['status'] != 'success':
            raise ValueError('Experiment is incomplete/failed: ' + str(directory))
        aggregate = read_json(directory / 'aggregate.json')
        provenance = read_json(directory / 'provenance.json')
        inventories = read_json(directory / 'inventory.json')
        config = provenance['cluster']['config']
        comparable = {k: config[k] for k in ['region', 'availability_zone', 'ami', 'instance_type', 'disk_gib']}
        fingerprints.append(json.dumps({'config': comparable, 'source': provenance['source_sha256'],
            'python': inventories[0]['python'], 'input': aggregate['input_sha256']}, sort_keys=True))
        experiments.append((directory, aggregate))
    if not experiments or len(set(fingerprints)) != 1:
        raise ValueError('Need compatible experiments: same input, source, Python, AMI, instance, disk, region and AZ')
    if len({a['workers'] for _, a in experiments}) != len(experiments):
        raise ValueError('Provide exactly one experiment per worker count')
    reference = next((a['job_wall_seconds_median'] for _, a in experiments if a['workers'] == 4), None)
    rows = []
    for directory, aggregate in sorted(experiments, key=lambda x: x[1]['workers']):
        count = aggregate['workers']
        seconds = aggregate['job_wall_seconds_median']
        runs = [read_json(p) for p in sorted(directory.glob('*-r*/result.json'))]
        if not runs or any(r['status'] != 'success' or not r.get('validation', {}).get('correct') for r in runs):
            raise ValueError('Missing or invalid run results: ' + str(directory))
        row = {'workers': count, 'repetitions': len(runs), 'job_seconds_median': seconds,
               'job_seconds_min': aggregate['job_wall_seconds_min'], 'job_seconds_max': aggregate['job_wall_seconds_max'],
               'input_mib_per_second_median': statistics.median(r['input_mib_per_second'] for r in runs),
               'user_cpu_seconds_sum_median': statistics.median(r['user_cpu_seconds_sum'] for r in runs),
               'system_cpu_seconds_sum_median': statistics.median(r['system_cpu_seconds_sum'] for r in runs),
               'worker_peak_rss_kib_max': max(r['worker_peak_rss_kib_max'] for r in runs),
               'largest_child_peak_rss_kib_max': max(r['largest_child_peak_rss_kib_max'] for r in runs),
               'speedup_relative_to_4': reference / seconds if reference else None,
               'efficiency_relative_to_4': reference / seconds / (count / 4) if reference else None}
        rows.append(row)
    destination = Path(destination)
    destination.mkdir(parents=True, exist_ok=True)
    with (destination / 'scaling.csv').open('w', newline='') as fp:
        writer = csv.DictWriter(fp, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)
    write_json(destination / 'scaling.json', {'experiments': [str(d) for d, _ in experiments], 'rows': rows})
    print(json.dumps(rows, indent=2))
    return rows


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('experiments', nargs='+', help='directories containing aggregate.json')
    parser.add_argument('--output', default='comparison')
    args = parser.parse_args()
    summarize(args.experiments, args.output)
