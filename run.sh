#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")"
source .venv/bin/activate
python -m pip install -r requirements.txt
python ingest_and_export.py
