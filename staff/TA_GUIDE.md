# TA setup and release checklist

The new student project is isolated from the previous Brown/WikiText experiments. No previous result is a measurement for this new 10 GB / T8i configuration.

## Dataset decision

Recommended: [`wikimedia/wikipedia`, `20231101.en`](https://huggingface.co/datasets/wikimedia/wikipedia), pinned to `b04c8d1ceb2f5cd4588862100d08de323dccfbaa`. The dataset card reports 6,407,814 English articles, ~20.2 GB of dataset records and ~11.63 GB of compressed downloads. Those figures include dataset fields; **the course size is the exported UTF-8 `text` bytes**, not Parquet download size. `data/source.json` locks the 41 source files, sizes and SHA-256 hashes.

The exporter scans sorted source files and their original row order, keeps complete unique article IDs, and stops after reaching **10,000,000,000 text bytes**. The last complete article may exceed the target slightly. It never repeats articles to inflate volume. `manifest.json` records actual bytes and corpus/output hashes. Article IDs, titles and URLs are saved for attribution alongside the source revision and CC BY-SA/GFDL license identifiers. Preserve those records when distributing the derived text. See the [dataset card](https://huggingface.co/datasets/wikimedia/wikipedia) for source/license details.

Alternative: [`HuggingFaceFW/fineweb`](https://huggingface.co/datasets/HuggingFaceFW/fineweb), with a fixed revision and deterministic 10 GB text subset. Its `sample-10BT` means **10 billion tokens**, not 10 GB. Wikipedia is the closer continuation of the existing WikiText workload, so this release implements Wikipedia.

## Prepare once, publish once

Use an isolated **Python 3.13** environment with `pyarrow==21.0.0` for TA preparation. Oracle creation checks Unicode 15.1.0; EC2 installs/runs `python3.13` rather than AL2023 system Python 3.9, so Unicode word definitions agree. Algorithm files are copied unchanged from professor revision e6735dda97c848edc219020480669a2aa49dc333. Python 3.13 satisfies its Python 3.12+ requirement; EC2 also installs the pinned tqdm dependency. See [AL2023 Python versions](https://docs.aws.amazon.com/linux/al2023/ug/python.html). Do not install dependencies into an environment running another benchmark.

```bash
python3.13 -m venv .venv
.venv/bin/python -m pip install -r requirements.txt
.venv/bin/python data/prepare.py --oracle-workers 4
```

This downloads verified source Parquet files as needed and builds `data/prepared/`: input text, independent Python Counter/SQLite oracle, sorted expected output, article attribution and manifest. It streams documents and uses SQLite rather than retaining the whole corpus/vocabulary in RAM. Allow disk space for source cache (~11.6 GB at most), 10 GB text, oracle/indexes and temporary results. Preparation is outside benchmark timing. Existing output directories are never overwritten. Interrupted `.part` downloads can be retried; an incomplete export requires a new output directory or explicit TA cleanup.

Choose an existing private course bucket and a **new versioned prefix**. Publish with `project.py publish --s3-uri ... --set-default`; this saves a shared S3 URI in `course.json`. The publisher creates 16 canonical contiguous shards and layouts for 4/8/16 workers; students download only their assigned shards directly from S3. Both input modes use the same whole-line byte partitions. Do not use an old Brown/WikiText prefix or make an entire bucket public. Give students the needed read access using the course's existing AWS setup. AWS runs use the bundled `data/course-publication.json` metadata and download only on EC2. Optional `project.py fetch` now rebuilds from the original pinned Hugging Face files, including extraction/reference generation, and verifies the course input, expected-output and attribution hashes. It does not read the course S3 bucket.

```bash
python3 project.py publish --s3-uri s3://YOUR-COURSE-BUCKET/wikipedia-en-10gb-v1/HASH/v1/ --set-default
```

Publish the actual manifest/input hash with the release. Local fixtures are explicitly labeled and rejected by the official benchmark entry point. The full 10 GB text and independent reference were generated on 2026-11-05. See the versioned S3 URI in course.json and the single-trial report for the actual verified run; do not infer results for unmeasured scales.

## Hardware and credits

Default configuration: `t8i.small`, **2 vCPU / 2 GiB**, 4/8/16 nodes, three repetitions, us-east-1. `--instance-type` selects another x86_64 AWS model (including `t2.micro`: 1 vCPU / 1 GiB), and `--repetitions 1` records a single trial. Cloud receives the same selected model and count, with no automatic substitution. Single-trial reports are explicitly labeled. `course.json` is the shared configuration for both backends. AL2023 x86_64 and 100 GiB gp3 (3000 IOPS / 125 MiB/s) are used by direct EC2. The first worker is the coordinator; there is no extra paid coordinator instance.

T8i is burstable. Direct EC2 explicitly launches `CpuCredits=unlimited` and verifies actual mode before starting the job. For Cloud, read-only preflight requires the account's T8i default to be unlimited, because the pinned Cloud SDK has no credit-mode constructor parameter. After Cloud starts, the runner looks up its EC2 instances by `PolarsClusterId` and verifies actual instance type, node count and credit mode. No instance-family fallback is allowed. Unlimited avoids standard-mode credit throttling but can incur surplus CPU charges; the harness saves whole-instance five-minute CPU credit/usage/surplus CloudWatch samples when available. These are separate from task CPU time; missing samples/permissions are explicitly unavailable. See [AWS credit modes](https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/burstable-performance-instances-unlimited-mode-concepts.html).

```bash
python3 project.py preflight              # AWS reads only, creates no compute
export WC_SSH_KEY=/path/to/your/key.pem
python3 project.py run --backend baseline --workers 4
```

Set WC_SSH_KEY to your own local private-key path. The key must not be bundled or uploaded. AWS CLI credentials/API permissions are separately required. Read-only AWS inspection on 2026-11-05 confirmed the T8i small specification; real capacity and quotas must be checked when launching. 16 nodes need 32 standard On-Demand vCPUs plus any other active account usage.

## Optional Polars Cloud

Configure the existing authorized AWS-connected workspace ID and a writable bucket under `course.json.polars`. For a TA-only trial, `cloud/runner.py --workspace UUID --bucket NAME` overrides those values without embedding a teacher workspace in the student configuration. Do not distribute the TA's SDK tokens or private keys. Students using their own accounts need their own configured workspace. Sign in with the official Python SDK into the ignored project `.auth/` directory. The pin is the previously used `polars==1.44.1`, `polars-cloud==0.11.0`.

```bash
export POLARS_CLOUD_CONFIG_DIR="$PWD/.auth"
.venv/bin/python -c 'import polars_cloud as pc; pc.authenticate()'
.venv/bin/python project.py run --backend polars --workers 4
```

Cloud receives the exact same text, split into byte-preserving plain TXT files, not pre-counted or converted input. The runner forces `.distributed(min_workers=P,max_workers=P,single_worker_ops="forbid")`, verifies stage worker counts, downloads all output and uses the fixed validation code in `cloud/validation.py`. Only the query in `src/polars_query.py` is student-editable. It requests `instance_type="t8i.small"` and the same per-node storage request; managed OS/data volume details are separately recorded and are not claimed to match AL2023's volume layout.

**Verified on 2026-11-05: 16 × t8i.small completed the full 10 GB dataset in one Cloud query.** Query wall time was 32.577413 s (server start-to-end 32.248977 s); both stages used 16 workers, failed stages were zero, and all counts matched the independent reference exactly. Cloud and AWS metadata confirmed 16 actual t8i.small instances and unlimited credit mode. See `cloud/results/polars-t8i-small-16-20261105/` in the TA checkout. This demonstrates feasibility for this query at 16 nodes; 4/8-node runs and repeated-trial stability have not been measured. It is not a measured memory-headroom guarantee: process CPU/RSS is unavailable from this SDK. Do not compare T8i MapReduce with larger Cloud nodes.

The runner stops identified contexts in `finally`, checks fresh server state and waits for EC2 termination before moving to the next node count. If start is rejected without returning a compute ID, the service might temporarily create partial resources; inspect the Cloud portal/AWS tagged instances before retrying. No automatic arbitrary workspace-wide termination is performed.

## Metrics, grading and release

Baseline/student direct-EC2 timing boundaries are identical: map/sort + gather/shuffle/reduce after input placement, before result collection/validation. All requested rounds include the first round; no cache-clearing claims are made. Cloud's S3-text-to-S3-Parquet query boundary is separately labeled. A cross-backend time ratio is an end-to-end implementation comparison, not an isolated engine speedup. CPU totals are not wall time; per-process peak RSS values are not a simultaneous sum over the node. Cloud CPU/RSS is missing when the SDK cannot expose it.

Reports keep code/data/hardware identity per row and preserve all successful runs. Only matching dataset/output hashes, instance model/count, requested disk size, credit mode and region get a baseline ratio. No old 540 MB/c6i result is imported. Missing 10 GB measurements remain absent.

Before release: run `python3 project.py check` for both backends, the tests, and a real AWS baseline sanity on the prepared data. Collect fresh 4/8/16 measurements before publishing a scaling study. Verify cleanup and quota release. Rebenchmark top three student code submissions in a fresh TA checkout using the held canonical infrastructure/config/data, plus changed-input correctness cases; do not trust a student-modified report or oracle. Students submit `src/` and a short explanation interpreting the auto-generated report.

Package a clean starter ZIP using `python3 staff/make_release.py`. It excludes environments, prepared data, credentials, results, logs and caches. The archive includes a provided-file checksum manifest for routine accidental-change detection; this is not a security signature. Distribution is ready only after the actual data S3 URI and baseline measurements have been supplied.

## S3 direct input and timing

Example: `python3 project.py run --backend baseline --input s3://BUCKET/PREFIX/ --workers 16 --instance-type t8i.small --repetitions 1`. A manifest prefix is required for parallel staging; arbitrary raw S3 objects should first be prepared/published. No EC2 instance profile is required. Deployment creates or reuses a free S3 Gateway Endpoint on the selected subnet route table. AWS credentials additionally need ec2:DescribeRouteTables, ec2:DescribeVpcEndpoints, ec2:CreateVpcEndpoint, ec2:DeleteVpcEndpoints and ec2:CreateTags. The course publication uses unsigned regional HTTPS URLs plus bundled metadata; custom private publications still use temporary read-only URLs generated by the local AWS CLI. Custom restricted prefixes may supply `--manifest /path/to/publication-record.json` (the same uri/required_region/manifest format as data/course-publication.json). URLs are not printed, recorded in reports, or packaged with results; staging plans are deleted after use. The course bucket permits reads only through us-east-1 VPC endpoints, including across student accounts. Direct laptop GETs, including owner-signed and presigned requests, are denied. Other private buckets retain their own policies. Each object and combined node input is SHA-256 checked before the timed job.

Input-staging wall time covers parallel download and checksum verification, separate from map/sort/shuffle/reduce job time. Pipeline wall time also includes output collection. Preserve those fields when discussing the benefit of S3; no local-vs-S3 speedup is claimed without running both. The `baseline/` algorithm remains byte-identical to its pinned professor revision; the input partitioning adapter now uses deterministic whole-line contiguous ranges, identically in local/S3 modes. Input-upload and TA oracle preparation times are separate.

## Verified single trial: 2026-11-05

The requested 16-node t8i.small S3-input trial ran exactly one MapReduce repetition. It processed 10,000,003,056 bytes / 1,611,413,213 tokens / 6,816,165 keys. Input staging including checksums: 14.720971 s; map/sort: 282.434612 s; gather/reduce: 395.622821 s; job: 678.057495 s; exact validation: PASS. See `reports/requested-16-t8i-small-20261105/` in the TA checkout for raw metrics and cleanup evidence. The later Polars trial above uses exactly the same input/output hashes; its S3 read/write query boundary differs from the MapReduce job boundary. No T2 performance result is implied.

Two deployment attempts were terminated before computation (EC2 ID propagation and a migrated probe path). Both issues now have regression coverage; all 48 provisioned instances across the attempts and successful trial were confirmed terminated. The SSH background launcher and timestamped-cluster resume behavior also have regression tests.

## Python merge-sort baseline update (2026-11-07)

The starter now pins professor commit `e6735dda97c848edc219020480669a2aa49dc333`. Both baseline and initial src include `merge_sort.py`; the optimized worker is now named `Worker` (previously `SeqWorker`). Keep Python 3.13 / Unicode 15.1 so the existing oracle remains valid. `tqdm==4.67.1` is installed locally and on EC2.

The trusted worker adapter sets both TMPDIR and Python's cached tempfile directory to `<job>/.spill` before each phase, restores them afterward, rejects tmpfs/ramfs, and records the path and filesystem. On EC2 the job directory is on the EBS volume. Sorting now runs inside the Python worker; its CPU and RSS include sorting. Child-process RSS mainly describes SCP, so do not compare that field alone to old GNU-sort peaks.

The earlier 2026-11-05 measurements above belong to old commit `5346bc41d5b099c40725669e9182548702a78562` with GNU sort. Retain those raw results and labels; they are not timings of this new baseline. The 10 GB data, tokenization and reference counts are unchanged. New validation results are recorded separately after the full AWS run.

### Verified full-data trial: 2026-11-07

The Python merge-sort baseline passed all 22 tests and both four-worker local checks, followed by one exact-correct full 10 GB trial on 16 × t8i.micro. Dataset/output hashes match the old GNU-sort trial. All 16 workers ran the pinned upstream files; all 32 phase records report EBS-backed xfs spill directories.

| Metric | New Python baseline |
|---|---:|
| Input staging (separate) | 16.995 s |
| Map/sort | 432.979 s |
| Gather/reduce | 450.102 s |
| Job total | 883.082 s |
| Task CPU total | 10480.132 CPU-s |
| Worker peak RSS | 31.06 MiB |
| Largest child peak RSS | 21.60 MiB |
| Lowest sampled whole-node available memory | 462.74 MiB |

This single trial took about 26.17% longer than the old 699.938 s GNU-sort run, with lower observed memory pressure. No repeated-run or 4/8-node scaling claim is made. Both versions' raw results remain in the TA checkout; new experiment: `wc-20261107T211755Z`. Output SHA-256: `8671d9dbf177294378ab70d33ca216b050e437d742ef8a519123faf988163662`. All 16 new instances were individually confirmed terminated.

## Regional S3 access restriction (2026-11-13)

`staff/s3_access.py` builds the read-only regional policy; it never changes AWS on import. The policy uses `aws:SourceVpcArn`, not `aws:RequestedRegion`. A whole-bucket Deny on GetObject/GetObjectVersion rejects absent or non-us-east-1 VPC context, including owner-signed URLs. Uploads, listing by authorized owners and bucket-policy administration are not denied. The old unconditional public-read/list statements are replaced with regional GetObject access. This enforces a regional endpoint path, not the identity of an EC2 process or subsequent redistribution.

`infrastructure/s3_gateway.py` records endpoint IDs/routing provenance in cluster state. A usable existing endpoint is left unchanged and retained. Endpoints created by this cluster are tagged and deleted on cleanup; ambiguous/lost create replies can be recovered by the cluster tag and client token. Shared endpoints with restrictive endpoint policies may need administrator configuration; deployment never broadens them. Do not apply this policy until the isolated-prefix test and a real four-worker experiment pass. Retain policy backups and restore the original policy if final download checks fail.

Polars Cloud must also have an S3 Gateway Endpoint in its us-east-1 workspace VPC. Use a separate student-owned output bucket so downloaded query results remain accessible locally. The TA workspace already has its own CloudFormation-managed S3 endpoint; never delete or modify it as part of a MapReduce experiment.
