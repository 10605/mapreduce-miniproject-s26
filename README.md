# WordCount optimization miniproject

Make the supplied MapReduce WordCount faster, while producing exactly the same word counts. Compare the original code with your version on **4, 8 and 16 AWS machines**. Submit your `src/` folder and a short explanation of your changes and measured results.

## Edit only these algorithm files

You may also add helper `.py` files inside `src/`.

| File | What you can improve |
|---|---|
| `src/hz_worker.py` | map/shuffle/sort/reduce workflow and concurrency |
| `src/merge_sort.py` | external sorting, batch size and merge fan-in |
| `src/reduce_util.py` | partitioning, grouping and intermediate data |
| `src/wc_hz.py` | WordCount implementation |
| `src/polars_query.py` | optional Polars Cloud query |

Keep the `WordCount` class, `do_map_and_shuffle(src)` and `do_gather_reduce(src, dst)` entry points, tokenization rules and output format (one Python `(word, integer_count)` tuple per key). Do not change the input, use reference counts in your algorithm, or skip timed work.  For the optimized runs you submit, do not change the default AWS instances that run the code.

The other folders are provided: `baseline/` holds the original implementation; `infrastructure/` handles AWS and reports; `data/` handles datasets; `cloud/` handles Polars Cloud; `tests/` checks correctness. Leave these and `course.json` unchanged, except for the dataset location when using your own S3 bucket.

## Dataset

We use about **10 GB of English Wikipedia text** from [wikimedia/wikipedia (`20231101.en`)](https://huggingface.co/datasets/wikimedia/wikipedia): 3.43 million complete articles, without repeating articles. The TA provides the fixed dataset and reference counts through the S3 location in `course.json`. Preserve the supplied attribution and license records.

**Recommended: use the public [course S3 bucket](https://10605-f26-testing.s3.us-east-1.amazonaws.com/).** The dataset prefix is already configured in `course.json`. Each machine downloads its assigned shards directly from S3 when you run an experiment; no local download or re-upload is needed. Use your own AWS credentials for the supplied CLI and EC2 runs.

**Optional: use your own S3 bucket.** After setup below, create a bucket in `us-east-1` and use credentials with write access to it, then run:

```bash
python project.py fetch                     # download and verify the course data
python project.py publish --input data/prepared \
  --s3-uri s3://YOUR-BUCKET/wordcount-v1/ --set-default
```

`--set-default` makes later runs use your bucket. Keep the dataset unchanged. If you only want a local copy, run just `fetch`.

## Setup and run

Install Python 3.13 and the AWS CLI, then run these commands from this project folder. Configure your own AWS account and SSH key; the key alone does not provide AWS API access.

```bash
python3.13 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
aws configure
export WC_SSH_KEY=/path/to/your/key.pem

python project.py check                    # local correctness check; no AWS cost
python project.py run --backend baseline   # original code
python project.py run --backend student    # your src/ code
python project.py report                   # HTML, CSV and JSON in reports/
```

Runs create **paid AWS resources**. By default, they use `t8i.small`, test 4/8/16 machines with three repetitions each, fetch results and terminate their clusters. To start with one smaller experiment:

```bash
python project.py run --backend student --workers 4 --repetitions 1
```

Select another machine with `--instance-type t2.micro`. Select local input with `--input data/prepared` (or a UTF-8 text file), or another published S3 dataset with `--input s3://YOUR-BUCKET/wordcount-v1/`. Use the same dataset, machine type and worker counts for comparisons.

Reports record exact correctness, job time, input-staging time, throughput, CPU and memory. Job time covers map/shuffle/sort/reduce; setup and validation are separate. Explain the changes using these measurements. The TA will rebenchmark the top three submissions (or others if it seems necessary to check the results).

## Optional: Polars Cloud

After the TA configures a workspace and output bucket in `course.json`, sign in and run:

```bash
export POLARS_CLOUD_CONFIG_DIR="$PWD/.auth"
python -c 'import polars_cloud as pc; pc.authenticate()'
python project.py run --backend polars --workers 4 --repetitions 1
```

Cloud must use the same selected machine type and count. Its query time includes S3 input/output; CPU and memory metrics may be unavailable. Consult the TA for supported configurations.

For interrupted-run recovery, explicit cleanup and TA setup, see [the TA guide](staff/TA_GUIDE.md).

## Contact TA (Spring 26 only)

If you have questions or run into issues, email the TA at [yutengz@andrew.cmu.edu](mailto:yutengz@andrew.cmu.edu). The TA will do their best to reply within 1–2 hours to help keep your project running smoothly.
