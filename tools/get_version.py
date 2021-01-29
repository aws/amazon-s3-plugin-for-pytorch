import os
import subprocess
from pathlib import Path

project_root = Path(__file__).parent

def get_sha():
    try:
        return subprocess.check_output(['git', 'rev-parse', 'HEAD'], cwd=project_root).decode('ascii').strip()
    except Exception:
        return 'Unknown'

def get_version():
    sha = get_sha()
    version = open('version.txt', 'r').read().strip()
    if sha != 'Unknown':
        version += '+' + sha[:7]
    return version

