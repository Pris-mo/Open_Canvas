#!/usr/bin/env sh
set -eu

REPO_ROOT="${1:-$(cd "$(dirname "$0")/.." && pwd)}"
VENV_DIR="${VENV_DIR:-$REPO_ROOT/.venv}"
VENV_PY="${VENV_PY:-$VENV_DIR/bin/python}"

echo "Repo: $REPO_ROOT"
echo "Venv python: $VENV_PY"

# Create venv if missing (only if VENV_PY doesn't exist)
if [ ! -x "$VENV_PY" ]; then
  echo "Venv not found at $VENV_PY, creating..."
  python3 -m venv "$VENV_DIR"
  VENV_PY="$VENV_DIR/bin/python"
fi

"$VENV_PY" -c "import sys; print('Using:', sys.executable)"

echo "Upgrading pip tooling..."
"$VENV_PY" -m pip install --upgrade pip setuptools wheel

echo "Editable install of Open_Canvas..."
"$VENV_PY" -m pip install -e "$REPO_ROOT"

echo "Installing requirements..."
"$VENV_PY" -m pip install -r "$REPO_ROOT/requirements.txt"

echo "Freeze (top lines):"
"$VENV_PY" -m pip freeze | head -n 30 || true

echo "OK: deps installed into venv."
