"""Exercise real worker phases; replace only remote SCP with a local file copy."""
import argparse
from pathlib import Path
import shlex
import shutil
import sys

p = argparse.ArgumentParser()
p.add_argument('--runtime', required=True)
p.add_argument('--cluster', required=True)
p.add_argument('--index', type=int, required=True)
p.add_argument('--phase', required=True)
p.add_argument('--workdir', required=True)
a = p.parse_args()
sys.path.insert(0, a.runtime)
from common import read_json
from worker import measured_phase
c = read_json(a.cluster)


def copy(command):
    remote, destination = shlex.split(command)[-2:]
    host, source = remote.split(':', 1)
    index = [n['private_ip'] for n in c['nodes']].index(host.split('@')[-1])
    assert Path(source).is_absolute(), source
    source = Path(c['test_nodes'][index]) / Path(source).name
    shutil.copyfile(source, destination)


result = measured_phase(c, a.index, a.phase, a.workdir, 60, interval=0.05, copy_hook=copy)
raise SystemExit(0 if result['status'] == 'success' else 1)
