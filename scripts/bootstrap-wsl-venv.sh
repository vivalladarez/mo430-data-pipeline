#!/usr/bin/env bash
# Uma vez no WSL (na raiz do clone): bash scripts/bootstrap-wsl-venv.sh
# Cria ~/.venvs/mo430-data-pipeline, instala requirements e normaliza LF dos .sh em scripts/.

set -e

_scripts="$(cd "$(dirname "${BASH_SOURCE[0]:-$0}")" && pwd)"
_repo="$(cd "${_scripts}/.." && pwd)"

# LF em scripts sob /mnt/c (evita sed -i e erro de permissoes no OneDrive)
python3 -c "
from pathlib import Path
d = Path('${_scripts}')
for n in ('wsl-env.sh', 'bootstrap-wsl-venv.sh'):
    p = d / n
    if p.is_file():
        p.write_bytes(p.read_bytes().replace(b'\r\n', b'\n').replace(b'\r', b''))
"

MO430_VENV="${MO430_VENV:-${HOME}/.venvs/mo430-data-pipeline}"
mkdir -p "$(dirname "${MO430_VENV}")"
rm -rf "${MO430_VENV}"
python3 -m venv "${MO430_VENV}"
# shellcheck source=/dev/null
source "${MO430_VENV}/bin/activate"
cd "${_repo}"
python -m pip install --upgrade pip
pip install -r requirements.txt
echo "MO430: venv pronto em ${MO430_VENV}"
echo "MO430: use: cd ${_repo} && source scripts/wsl-env.sh"
