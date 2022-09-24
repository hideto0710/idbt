import pathlib
import subprocess

from subprocess import Popen
from typing import Optional


class DBTRPCServer:
    def __init__(self):
        self.process: Optional[Popen] = None

    def start(self):
        crr_dir = '/Users/hidetoinamura/Develop/mudazukai/idbt'
        pj_dir = '/Users/hidetoinamura/Develop/mudazukai/v-moni/transform'
        logs_dir = '{}/.idbt/logs'.format(crr_dir)
        pathlib.Path(logs_dir).mkdir(parents=True, exist_ok=True)
        cmd = ['dbt', 'rpc']
        with open('{}/test.txt'.format(logs_dir), 'wb') as out:
            self.process = subprocess.Popen(
                cmd,
                cwd=pj_dir,
                stdout=out,
                stderr=out,
                universal_newlines=True,
            )
