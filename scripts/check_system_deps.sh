#!/usr/bin/env sh
set -eu

need_cmd() {
  if ! command -v "$1" >/dev/null 2>&1; then
    echo "MISSING: $1"
    return 1
  else
    echo "OK: $1 -> $(command -v "$1")"
  fi
}

fail=0
need_cmd git || fail=1
need_cmd gs || fail=1            # ghostscript (ocrmypdf)
need_cmd tesseract || fail=1     # pytesseract/easyocr workflows
need_cmd pdftoppm || fail=1      # poppler-utils (pdf2image)
need_cmd pdfinfo || fail=1       # poppler-utils
# add more only if your code actually uses them

if [ "$fail" -eq 1 ]; then
  echo ""
  echo "One or more OS dependencies are missing."
  echo "If you're in Docker, install via apt-get (see Dockerfile)."
  exit 1
fi

echo "OK: system deps look present."