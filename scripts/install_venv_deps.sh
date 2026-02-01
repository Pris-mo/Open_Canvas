#!/usr/bin/env sh
set -eu

REPO_ROOT="${1:-$(cd "$(dirname "$0")/.." && pwd)}"
VENV_DIR="${VENV_DIR:-$REPO_ROOT/.venv}"
VENV_PY="${VENV_PY:-$VENV_DIR/bin/python}"
PIP_DEFAULT_TIMEOUT="${PIP_DEFAULT_TIMEOUT:-120}"
PIP_INDEX_URL="${PIP_INDEX_URL:-https://pypi.org/simple}"
INSTALL_SYS_DEPS="${INSTALL_SYS_DEPS:-0}"

echo "Repo: $REPO_ROOT"
echo "Venv python: $VENV_PY"
echo "Pip timeout: $PIP_DEFAULT_TIMEOUT"
echo "Pip index: $PIP_INDEX_URL"
echo "Install system deps: $INSTALL_SYS_DEPS"

if [ "$INSTALL_SYS_DEPS" = "1" ]; then
  if command -v apt-get >/dev/null 2>&1; then
    echo "Installing system deps (ffmpeg, tesseract-ocr)..."
    apt-get update
    apt-get install -y ffmpeg tesseract-ocr
  else
    echo "WARN: apt-get not found; skipping system deps install."
  fi
fi

# Create venv if missing (only if VENV_PY doesn't exist)
if [ ! -x "$VENV_PY" ]; then
  echo "Venv not found at $VENV_PY, creating..."
  "$(command -v python3)" -m venv "$VENV_DIR"
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
