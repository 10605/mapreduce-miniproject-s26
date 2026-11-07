"""Exercise real worker phases; replace only remote SCP with a local file copy."""
import argparse
from pathlib import Path
import shlex
import shutil
import sys
import tempfile

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


# Prime tempfile's cache to ensure the production adapter overrides it.
old_tempdir = tempfile.gettempdir()
import merge_sort as ms
original_init = ms._SpillFileIndex.__init__
def checked_init(self):
    original_init(self)
    assert Path(self.dir).parent == Path(a.workdir).resolve() / '.spill', self.dir
ms._SpillFileIndex.__init__ = checked_init
result = measured_phase(c, a.index, a.phase, a.workdir, 60, interval=0.05, copy_hook=copy)
assert tempfile.gettempdir() == old_tempdir
assert result['temporary_directory'] == str(Path(a.workdir).resolve() / '.spill')
raise SystemExit(0 if result['status'] == 'success' else 1)
