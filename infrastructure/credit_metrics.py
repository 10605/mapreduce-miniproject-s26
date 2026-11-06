"""Best-effort, whole-instance credit telemetry; never counted as task CPU time."""
from concurrent.futures import ThreadPoolExecutor
from preflight import aws


def collect(c, instance_ids, start, end):
    metrics = ['CPUCreditUsage', 'CPUCreditBalance', 'CPUSurplusCreditBalance', 'CPUSurplusCreditsCharged']

    def one(item):
        instance_id, metric = item
        record = {'instance_id': instance_id, 'metric': metric}
        try:
            record['response'] = aws(c, 'cloudwatch', 'get-metric-statistics', namespace='AWS/EC2',
                metric_name=metric, dimensions=[{'Name': 'InstanceId', 'Value': instance_id}],
                start_time=start, end_time=end, period=300, statistics=['Average', 'Sum', 'Maximum'])
            if not record['response'].get('Datapoints'):
                record['unavailable'] = 'No published datapoints in this window; not zero credits or zero cost.'
        except Exception as exc:
            record['unavailable'] = repr(exc)
        return record

    with ThreadPoolExecutor(max_workers=8) as pool:
        results = list(pool.map(one, [(i, m) for i in instance_ids for m in metrics]))
    return {'scope': 'whole-instance experiment window, five-minute AWS samples; not per-query CPU',
            'start': start, 'end': end, 'samples': results,
            'note': 'Publication delay/permissions may leave metrics unavailable; use raw samples, do not infer missing values as zero.'}
