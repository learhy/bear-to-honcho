#!/bin/bash
# Incremental sync of Bear notes into Honcho
# Runs via Hermes cron every 10 minutes

set -e

PYTHON=/usr/bin/python3
SCRIPT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd -P)/bear_to_honcho.py"
LOG="$HOME/.bear-rag/honcho_ingestion.log"

echo "$(date): Starting incremental sync..." >> "$LOG"
$PYTHON "$SCRIPT" --incremental >> "$LOG" 2>&1
echo "$(date): Sync complete" >> "$LOG"