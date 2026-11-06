#!/usr/bin/env python3
"""Create CSV/JSON/HTML from exact-correct runs, separated by code/data/hardware."""
import csv
import hashlib
import html
import json
from pathlib import Path
import statistics
import sys
import time

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from project import course, frozen_baseline
from common import read_json, write_json


def collect():
    groups = []
    for p in sorted((ROOT / 'infrastructure/clusters').glob('*/downloaded/*/results/*/aggregate.json')):
        directory = p.parent
        provenance = read_json(directory / 'provenance.json')
        cfg = provenance['cluster']['config']
        aggregate = read_json(p)
        runs = [read_json(r) for r in sorted(directory.glob('*-r*/result.json'))]
        if len(runs) != aggregate['successful_repetitions'] or not runs or any(r['status'] != 'success' or not r['validation']['correct'] for r in runs):
            continue
        code = hashlib.sha256(json.dumps(cfg['algorithm_sha256'], sort_keys=True).encode()).hexdigest()
        values = [r['job_wall_seconds'] for r in runs]
        groups.append({'backend': cfg['implementation'], 'code_sha256': code,
            'repetitions': len(runs), 'input_mode': cfg.get('input_mode', 'local-scp'),
            'workers': aggregate['workers'], 'instance_type': cfg['instance_type'],
            'cpu_credits': cfg.get('cpu_credits'), 'disk_gib_requested': cfg['disk_gib'],
            'region': cfg['region'], 'input_sha256': aggregate['input_sha256'],
            'input_bytes': aggregate['input_bytes'], 'output_sha256': runs[0]['validation']['canonical_output_sha256'],
            'seconds': values, 'seconds_median': statistics.median(values),
            'input_staging_seconds': [r['distribute_input_seconds'] for r in runs],
            'pipeline_seconds': [r['pipeline_wall_seconds'] for r in runs],
            'cpu_seconds_median': statistics.median(r['user_cpu_seconds_sum'] + r['system_cpu_seconds_sum'] for r in runs),
            'worker_peak_rss_mib': max(r['worker_peak_rss_kib_max'] for r in runs) / 1024,
            'largest_child_peak_rss_mib': max(r['largest_child_peak_rss_kib_max'] for r in runs) / 1024,
            'throughput_mib_s': aggregate['input_bytes'] / 2**20 / statistics.median(values),
            'timing_scope': 'preloaded-local-text -> local-output, including map/sort/shuffle/reduce',
            'source': str(directory.relative_to(ROOT)), 'correct': True})
    for p in sorted((ROOT / 'cloud/results').glob('*/*n/aggregate.json')):
        d = p.parent
        a = read_json(p)
        provenance = read_json(d.parent / 'provenance.json')
        runs = [read_json(r) for r in sorted(d.glob('r*/result.json'))]
        if len(runs) != a['repetitions'] or not runs or any(r['status'] != 'success' or not r['validation']['correct'] for r in runs):
            continue
        cfg = provenance['course_config']
        values = [r['query_wall_seconds'] for r in runs]
        groups.append({'backend': 'polars', 'code_sha256': provenance['query_sha256'],
            'repetitions': len(runs), 'input_mode': 's3-query',
            'workers': a['workers'], 'instance_type': cfg['instance_type'], 'cpu_credits': cfg['cpu_credits'],
            'disk_gib_requested': cfg['disk_gib'], 'region': cfg['region'],
            'input_sha256': a['source_sha256'], 'input_bytes': provenance['dataset']['input_bytes'],
            'output_sha256': a['expected_sha256'], 'seconds': values,
            'seconds_median': statistics.median(values), 'cpu_seconds_median': None,
            'input_staging_seconds': [], 'pipeline_seconds': values,
            'worker_peak_rss_mib': None, 'largest_child_peak_rss_mib': None,
            'throughput_mib_s': provenance['dataset']['input_bytes'] / 2**20 / statistics.median(values),
            'timing_scope': 'S3-text -> S3-Parquet, including native aggregation/shuffle',
            'source': str(d.relative_to(ROOT)), 'correct': True})
    return groups


def compatible(a, b):
    keys = ['workers', 'instance_type', 'cpu_credits', 'disk_gib_requested', 'region', 'input_sha256', 'output_sha256']
    return all(a[k] == b[k] for k in keys)


def render(groups, output):
    output.mkdir(parents=True, exist_ok=False)
    for row in groups:
        baselines = [b for b in groups if b['backend'] == 'baseline' and compatible(row, b)]
        # Latest successful source path, not the fastest rerun. Keep every row.
        reference = sorted(baselines, key=lambda b: b['source'])[-1] if baselines else None
        row['time_ratio_to_baseline'] = reference['seconds_median'] / row['seconds_median'] if reference else None
        row['comparison_label'] = ('same MapReduce timing boundary' if row['backend'] != 'polars'
                                   else 'observed workflow time ratio; storage/output boundaries differ')
        cohort_keys = ['backend', 'code_sha256', 'instance_type', 'cpu_credits', 'disk_gib_requested',
                       'region', 'input_sha256', 'output_sha256']
        references = [b for b in groups if b['workers'] == 4 and all(row[k] == b[k] for k in cohort_keys)]
        reference4 = sorted(references, key=lambda b: b['source'])[-1] if references else None
        row['speedup_relative_to_4'] = reference4['seconds_median'] / row['seconds_median'] if reference4 else None
        row['parallel_efficiency_relative_to_4'] = row['speedup_relative_to_4'] / (row['workers'] / 4) if reference4 else None
    write_json(output / 'summary.json', groups)
    if groups:
        with (output / 'results.csv').open('w', newline='') as fp:
            writer = csv.DictWriter(fp, fieldnames=list(groups[0]))
            writer.writeheader()
            writer.writerows(groups)
    columns = ['backend', 'workers', 'repetitions', 'input_mode', 'code_sha256', 'seconds', 'seconds_median', 'input_staging_seconds', 'pipeline_seconds', 'time_ratio_to_baseline',
               'speedup_relative_to_4', 'parallel_efficiency_relative_to_4',
               'cpu_seconds_median', 'worker_peak_rss_mib', 'throughput_mib_s', 'comparison_label']
    rows = ''.join('<tr>' + ''.join('<td>' + html.escape(str(r[k]) if r[k] is not None else 'unavailable') + '</td>'
                                  for k in columns) + '</tr>' for r in groups)
    page = '<!doctype html><meta charset="utf-8"><title>WordCount results</title><style>body{font:15px system-ui;margin:32px}table{border-collapse:collapse}td,th{border:1px solid #ccc;padding:8px;max-width:220px;overflow-wrap:anywhere}</style>'
    page += '<h1>WordCount benchmark</h1><p>Actual repetition count is shown per row, including single-trial runs. Exact-correct outputs only. CPU totals are not wall time. Cloud CPU/RSS unavailable.</p>'
    page += '<p>EC2 runtime includes map, sort, shuffle and reduce; setup, input staging, result collection and validation are excluded. Cloud runtime includes S3 input/output. Cross-backend ratios do not isolate the compute engine.</p>'
    page += '<table><tr>' + ''.join('<th>' + x + '</th>' for x in columns) + '</tr>' + rows + '</table>'
    if not groups:
        page += '<p>No successful complete 3-run experiments yet. No estimated scores are generated.</p>'
    (output / 'report.html').write_text(page)


if __name__ == '__main__':
    course()
    frozen_baseline()
    output = ROOT / 'reports' / time.strftime('%Y%m%dT%H%M%SZ', time.gmtime())
    render(collect(), output)
    print(output / 'report.html')
