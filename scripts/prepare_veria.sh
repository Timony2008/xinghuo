#!/usr/bin/env bash
set -euo pipefail
project_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$project_root"
if [[ ! -x .venv-veria/bin/python ]]; then
    uv venv --python 3.14 .venv-veria
fi
.venv-veria/bin/python -c 'import sys; assert sys.version_info[:2] == (3, 14), "Existing .venv-veria must use Python 3.14"'
uv pip install --python .venv-veria/bin/python -r requirements-veria.lock
uv pip install --python .venv-veria/bin/python -e .
docker info >/dev/null
docker build -f Dockerfile.veria-tools -t xinghuo-veria-tools:local .
printf '%s\n' 'Prepared environment only. No model or benchmark was invoked.'
