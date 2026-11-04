# Bear-to-Honcho

Ingests [Bear](https://bear.app) notes into a self-hosted [Honcho](https://github.com/plastic-labs/honcho) memory system, enabling an AI agent to access years of meeting notes, 1:1s, and project context through Honcho's dialectic reasoning and semantic search.

## How It Works

```
Bear.app (MacBook)
    │ rsync every 10m
    ▼
~/.bear-rag/bear_fts.db (FTS5 mirror)
    │ bear_to_honcho.py
    ▼
Honcho API (localhost:8000)
    │ deriver processes messages
    ▼
Conclusions + embeddings in PostgreSQL
    │
    ▼
Hermes agent memory tools (honcho_search, honcho_reasoning, etc.)
```

The script reads notes from the Bear FTS5 mirror, classifies them by type (1:1, meeting, planning, personal) and mentioned people, then pushes them into person-scoped Honcho sessions. Honcho's deriver extracts atomic facts from each message, building a per-person knowledge representation over time.

## Prerequisites

- **Bear FTS5 mirror** at `~/.bear-rag/bear_fts.db` (built by `fts5_mirror.py`)
- **Honcho server** running on `http://localhost:8000` with workspace `hermes_work_assistant`
- **Python 3.11+** with `requests` installed

## Usage

### Initial load (all notes)

```bash
python3 bear_to_honcho.py --initial
```

Processes all non-archived notes under 25,000 chars (~4,000 notes). Takes 2-3 minutes with batch pauses.

### Dry run (see what would be ingested)

```bash
python3 bear_to_honcho.py --dry-run --max-notes 100
```

### Incremental sync (new/modified notes only)

```bash
python3 bear_to_honcho.py --incremental
```

### Status check

```bash
python3 bear_to_honcho.py --status
```

## Configuration

Environment variables (all have defaults):

| Variable | Default | Description |
|----------|---------|-------------|
| `BEAR_FTS_DB` | `~/.bear-rag/bear_fts.db` | Path to Bear FTS5 mirror |
| `HONCHO_URL` | `http://localhost:8000` | Honcho API URL |
| `HONCHO_WORKSPACE` | `hermes_work_assistant` | Honcho workspace ID |
| `HONCHO_USER_PEER` | `dan-rohan` | Peer ID for message attribution |
| `HONCHO_STATE_FILE` | `~/.bear-rag/honcho_ingestion_state.json` | State file path |
| `BEAR_HONCHO_PEOPLE` | (unset) | Comma-separated people to track; overrides the file below |
| `BEAR_HONCHO_PEOPLE_FILE` | `~/.bear-rag/tracked_people.txt` | One name per line. Kept out of the repo. First match picks the session. |

## Cron Setup

The `sync.sh` script is designed for Hermes cron:

```bash
# Symlink for Hermes cron
mkdir -p ~/.hermes/scripts
ln -sf ~/software/bear-to-honcho/sync.sh ~/.hermes/scripts/bear-to-honcho.sh

# Create cron job (every 10 minutes)
hermes -p work-assistant cron create \
  --name "bear-to-honcho-sync" \
  --schedule "every 10m" \
  --script bear-to-honcho.sh \
  --deliver local \
  --no-agent
```

## How Notes Are Classified

| Type | Detection | Example |
|------|-----------|---------|
| 1:1 | Tags contain `1:1s` or title contains `1:1` | "Alex/Dan 1:1" |
| meeting | Tags contain `meeting-notes` or `meeting` | "Roadmap Review" |
| planning | Tags contain `planning` or `diary` | "Q4 Planning" |
| personal | Tags contain `personal` or `family` | "Birthday party" |
| general | Default | Feature requests, random notes |

Notes are routed to person-scoped sessions (`bear-notes-alex`, `bear-notes-sam`, etc.) based on which tracked people are mentioned. Notes mentioning no tracked people go to `bear-notes-general`.

## Limitations

- Notes over 25,000 chars are skipped (451 of ~4,477 notes). Chunking support is a future enhancement.
- All messages are attributed to the user peer. Speaker attribution from transcripts is not supported.
- The deriver processes messages asynchronously. After the initial load, it may take hours to fully extract all observations depending on LLM speed.