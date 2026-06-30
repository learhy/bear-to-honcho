#!/bin/bash
# Incremental sync of Bear notes into Honcho
# Runs via Hermes cron every 10 minutes

set -e

PYTHON=/usr/bin/python3
SCRIPT=/home/dan.rohan/software/bear-to-honcho/bear_to_honcho.py
LOG=/home/dan.rohan/.bear-rag/honcho_ingestion.log

echo "$(date): Starting incremental sync..." >> "$LOG"
$PYTHON "$SCRIPT" --incremental >> "$LOG" 2>&1
echo "$(date): Sync complete" >> "$LOG"