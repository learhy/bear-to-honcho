#!/usr/bin/env python3
"""Ingest Bear notes into Honcho as messages for deriver processing.

Reads notes from the Bear FTS5 mirror, classifies them by type and person,
and pushes them into Honcho sessions via the REST API.

Usage:
    python3 bear_to_honcho.py --initial          # Full initial load
    python3 bear_to_honcho.py --incremental      # Only notes modified since last sync
    python3 bear_to_honcho.py --dry-run          # Print what would be ingested, no API calls
    python3 bear_to_honcho.py --status           # Print ingestion state
"""

from __future__ import annotations

import argparse
import json
import os
import sqlite3
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional

import requests

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

BEAR_FTS_DB = os.path.expanduser(os.environ.get("BEAR_FTS_DB", "~/.bear-rag/bear_fts.db"))
HONCHO_URL = os.environ.get("HONCHO_URL", "http://localhost:8000")
HONCHO_WORKSPACE = os.environ.get("HONCHO_WORKSPACE", "hermes_work_assistant")
HONCHO_USER_PEER = os.environ.get("HONCHO_USER_PEER", "dan-rohan")
STATE_FILE = os.path.expanduser(
    os.environ.get("HONCHO_STATE_FILE", "~/.bear-rag/honcho_ingestion_state.json")
)
MAX_MESSAGE_SIZE = 25000  # Honcho message limit
BATCH_SIZE = 100  # Honcho batch message limit
BATCH_PAUSE_SECONDS = 2  # Pause between batches to avoid flooding the deriver queue

# People to track. Kept out of the repo: one name per line in
# ~/.bear-rag/tracked_people.txt (override the path with BEAR_HONCHO_PEOPLE_FILE),
# or a comma-separated BEAR_HONCHO_PEOPLE env var. Order matters: the first
# match decides a note's session.
def load_tracked_people() -> list[str]:
    env = os.environ.get("BEAR_HONCHO_PEOPLE", "").strip()
    if env:
        return [p.strip() for p in env.split(",") if p.strip()]
    path = os.path.expanduser(
        os.environ.get("BEAR_HONCHO_PEOPLE_FILE", "~/.bear-rag/tracked_people.txt")
    )
    try:
        with open(path, encoding="utf-8") as f:
            return [ln.strip() for ln in f if ln.strip() and not ln.lstrip().startswith("#")]
    except FileNotFoundError:
        return []


TRACKED_PEOPLE: list[str] = load_tracked_people()


# ---------------------------------------------------------------------------
# Session naming
# ---------------------------------------------------------------------------


def session_name_for_person(person: str) -> str:
    """Create a Honcho session ID for a person's Bear notes."""
    slug = person.lower().replace(" ", "-").replace(".", "")
    return f"bear-notes-{slug}"


# ---------------------------------------------------------------------------
# State management
# ---------------------------------------------------------------------------


def load_state() -> dict[str, Any]:
    """Load ingestion state from disk."""
    path = Path(STATE_FILE)
    if path.exists():
        with open(path) as f:
            return json.load(f)
    return {"last_sync": None, "ingested_note_ids": [], "stats": {}}


def save_state(state: dict[str, Any]) -> None:
    """Save ingestion state to disk."""
    path = Path(STATE_FILE)
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w") as f:
        json.dump(state, f, indent=2)


# ---------------------------------------------------------------------------
# Honcho API helpers
# ---------------------------------------------------------------------------


def honcho_headers() -> dict[str, str]:
    """Headers for Honcho API calls (no auth for self-hosted)."""
    return {"Content-Type": "application/json"}


def honcho_post(path: str, body: dict[str, Any], dry_run: bool = False) -> Optional[Any]:
    """POST to Honcho API. Returns parsed JSON or None."""
    if dry_run:
        print(f"  [DRY RUN] POST {path}")
        return None
    url = f"{HONCHO_URL}{path}"
    r = requests.post(url, json=body, headers=honcho_headers(), timeout=30)
    if r.status_code not in (200, 201):
        print(f"  [ERROR] POST {path} -> {r.status_code}: {r.text[:200]}", file=sys.stderr)
        return None
    return r.json()


def ensure_workspace(dry_run: bool = False) -> bool:
    """Ensure the Honcho workspace exists."""
    r = requests.get(
        f"{HONCHO_URL}/v3/workspaces/{HONCHO_WORKSPACE}",
        headers=honcho_headers(),
        timeout=10,
    )
    if r.status_code == 200:
        return True
    honcho_post("/v3/workspaces", {"name": HONCHO_WORKSPACE}, dry_run=dry_run)
    return True


def ensure_peer(peer_id: str, dry_run: bool = False) -> bool:
    """Create a peer if it doesn't exist. Peer IDs must match ^[a-zA-Z0-9_-]+$. """
    # Check existing
    r = requests.get(
        f"{HONCHO_URL}/v3/workspaces/{HONCHO_WORKSPACE}/peers",
        params={"page": 1, "size": 200},
        headers=honcho_headers(),
        timeout=10,
    )
    if r.status_code == 200:
        existing = r.json()
        items = existing.get("items", existing) if isinstance(existing, dict) else existing
        if any(p.get("id") == peer_id for p in items):
            return True
    # Create
    honcho_post(
        f"/v3/workspaces/{HONCHO_WORKSPACE}/peers",
        {"id": peer_id},
        dry_run=dry_run,
    )
    return True


def ensure_session(
    session_id: str, peer_ids: list[str], dry_run: bool = False
) -> bool:
    """Ensure a Honcho session exists with the given peers.

    Idempotent — safe to call if session already exists.
    """
    base = f"/v3/workspaces/{HONCHO_WORKSPACE}/sessions/{session_id}"

    # Check if session exists
    r = requests.get(f"{HONCHO_URL}{base}/peers", headers=honcho_headers(), timeout=10)
    if r.status_code != 200:
        # Create session
        honcho_post(
            f"/v3/workspaces/{HONCHO_WORKSPACE}/sessions",
            {"id": session_id},
            dry_run=dry_run,
        )

    # Ensure each peer exists globally, then add to session
    for peer_id in peer_ids:
        ensure_peer(peer_id, dry_run=dry_run)

    # Add peers to session (body is {peer_id: SessionPeerConfig})
    peer_body: dict[str, Any] = {pid: {} for pid in peer_ids}
    if not dry_run:
        r = requests.post(
            f"{HONCHO_URL}{base}/peers",
            json=peer_body,
            headers=honcho_headers(),
            timeout=10,
        )
        if r.status_code not in (200, 201):
            # Peers may already be in session — that's fine
            pass
    return True


def push_messages(
    session_id: str,
    peer_id: str,
    messages: list[str],
    dry_run: bool = False,
) -> int:
    """Push a batch of messages to a Honcho session.

    Returns the number of messages successfully pushed.
    """
    base = f"/v3/workspaces/{HONCHO_WORKSPACE}/sessions/{session_id}"

    batch = [{"content": content, "peer_id": peer_id} for content in messages[:BATCH_SIZE]]

    if not batch:
        return 0

    if dry_run:
        print(f"  [DRY RUN] Would push {len(batch)} messages to {session_id}")
        return len(batch)

    result = honcho_post(f"{base}/messages", {"messages": batch}, dry_run=False)

    if result is None:
        return 0

    return len(result) if isinstance(result, list) else len(batch)


# ---------------------------------------------------------------------------
# Bear FTS5 note reader
# ---------------------------------------------------------------------------


def get_notes(
    since_timestamp: Optional[float] = None,
    limit: Optional[int] = None,
) -> list[dict[str, Any]]:
    """Read notes from the Bear FTS5 mirror.

    Args:
        since_timestamp: Only return notes modified after this Unix timestamp.
        limit: Maximum number of notes to return.

    Returns:
        List of dicts with keys: note_id, title, content, tags, created, modified, char_count
    """
    conn = sqlite3.connect(BEAR_FTS_DB)
    conn.row_factory = sqlite3.Row

    query = (
        "SELECT note_id, title, content, tags, created, modified, char_count "
        "FROM notes WHERE archived = 0"
    )
    params: list[Any] = []

    if since_timestamp is not None:
        query += " AND modified > ?"
        params.append(since_timestamp)

    query += " ORDER BY modified ASC"

    if limit is not None:
        query += " LIMIT ?"
        params.append(limit)

    rows = conn.execute(query, params).fetchall()
    conn.close()

    return [dict(r) for r in rows]


# ---------------------------------------------------------------------------
# Note classification
# ---------------------------------------------------------------------------


def classify_note(note: dict[str, Any]) -> dict[str, Any]:
    """Classify a note by type and extract mentioned people.

    Returns:
        Dict with keys: type, people, session_target
    """
    title = note.get("title", "").lower()
    content = note.get("content", "").lower()
    tags = note.get("tags", "").lower()
    text = f"{title} {content}"

    # Find mentioned people
    people = [person for person in TRACKED_PEOPLE if person.lower() in text]

    # Classify by tags and title patterns
    note_type = "general"

    if "1:1" in tags or "1:1s" in tags:
        note_type = "1:1"
    elif "meeting-notes" in tags or "meeting" in tags:
        note_type = "meeting"
    elif "planning" in tags or "diary" in tags:
        note_type = "planning"
    elif "personal" in tags or "family" in tags:
        note_type = "personal"
    elif "1:1" in title or "one on one" in title:
        note_type = "1:1"

    # Determine session target
    if people:
        session_target = session_name_for_person(people[0])
    else:
        session_target = "bear-notes-general"

    return {
        "type": note_type,
        "people": people,
        "session_target": session_target,
    }


def format_note_for_honcho(note: dict[str, Any], classification: dict[str, Any]) -> str:
    """Format a Bear note as a Honcho message.

    The message includes metadata about the note type, date, and people,
    followed by the note content. This gives the deriver context for
    extracting observations.
    """
    mod_date = datetime.fromtimestamp(
        note["modified"], tz=timezone.utc
    ).strftime("%Y-%m-%d")

    header = (
        f"[Bear Note — {classification['type']}]\n"
        f"Title: {note['title']}\n"
        f"Date: {mod_date}\n"
        f"Tags: {note['tags']}\n"
        f"People: {', '.join(classification['people']) or 'none'}\n\n"
    )

    content = note["content"]

    # Truncate if needed (leave room for header)
    max_content = MAX_MESSAGE_SIZE - len(header) - 50  # safety margin
    if len(content) > max_content:
        content = content[:max_content] + "\n\n[... truncated ...]"

    return header + content


# ---------------------------------------------------------------------------
# Ingestion logic
# ---------------------------------------------------------------------------


def _ingest_notes(
    notes: list[dict[str, Any]],
    state: dict[str, Any],
    dry_run: bool = False,
) -> tuple[int, int]:
    """Core ingestion loop shared by initial and incremental modes.

    Returns (total_pushed, errors).
    """
    if not notes:
        print("No notes to ingest.")
        return 0, 0

    if dry_run:
        session_counts: dict[str, int] = {}
        for note in notes:
            c = classify_note(note)
            session_counts[c["session_target"]] = session_counts.get(c["session_target"], 0) + 1
        print(f"\n[DRY RUN] Would ingest {len(notes)} notes into {len(session_counts)} sessions:")
        for session, count in sorted(session_counts.items(), key=lambda x: -x[1]):
            print(f"  {session}: {count} notes")
        return 0, 0

    ensure_workspace(dry_run=False)

    # Group notes by session target
    session_notes: dict[str, list[dict[str, Any]]] = {}
    for note in notes:
        classification = classify_note(note)
        session = classification["session_target"]
        if session not in session_notes:
            session_notes[session] = []
        note["_classification"] = classification
        session_notes[session].append(note)

    print(f"\nIngesting into {len(session_notes)} sessions...")

    total_pushed = 0
    errors = 0

    for session_id, session_note_list in session_notes.items():
        if not ensure_session(session_id, [HONCHO_USER_PEER], dry_run=False):
            print(f"  [ERROR] Failed to create session {session_id}", file=sys.stderr)
            errors += len(session_note_list)
            continue

        batch: list[str] = []
        batch_note_ids: list[str] = []

        for note in session_note_list:
            formatted = format_note_for_honcho(note, note["_classification"])
            batch.append(formatted)
            batch_note_ids.append(note["note_id"])

            if len(batch) >= BATCH_SIZE:
                pushed = push_messages(session_id, HONCHO_USER_PEER, batch, dry_run=False)
                total_pushed += pushed
                state["ingested_note_ids"].extend(batch_note_ids[:pushed])
                print(f"  {session_id}: pushed {pushed} (total: {total_pushed})")
                batch = []
                batch_note_ids = []
                time.sleep(BATCH_PAUSE_SECONDS)

        # Flush remaining
        if batch:
            pushed = push_messages(session_id, HONCHO_USER_PEER, batch, dry_run=False)
            total_pushed += pushed
            state["ingested_note_ids"].extend(batch_note_ids[:pushed])
            print(f"  {session_id}: pushed {pushed} (total: {total_pushed})")

    return total_pushed, errors


def run_initial_load(
    dry_run: bool = False, max_notes: Optional[int] = None
) -> None:
    """Full initial ingestion of Bear notes into Honcho."""
    state = load_state()
    ingested = set(state.get("ingested_note_ids", []))

    print("Fetching notes from Bear FTS5 mirror...")
    all_notes = get_notes(limit=max_notes)

    to_ingest = []
    skipped_over_limit = 0
    skipped_already = 0

    for note in all_notes:
        if note["note_id"] in ingested:
            skipped_already += 1
            continue
        if note["char_count"] > MAX_MESSAGE_SIZE:
            skipped_over_limit += 1
            continue
        to_ingest.append(note)

    print(f"Total notes: {len(all_notes)}")
    print(f"Already ingested: {skipped_already}")
    print(f"Skipped (over {MAX_MESSAGE_SIZE} chars): {skipped_over_limit}")
    print(f"To ingest: {len(to_ingest)}")

    total_pushed, errors = _ingest_notes(to_ingest, state, dry_run=dry_run)

    state["last_sync"] = datetime.now(timezone.utc).isoformat()
    state["stats"] = {
        "total_pushed": total_pushed,
        "errors": errors,
        "skipped_over_limit": skipped_over_limit,
    }
    save_state(state)

    print(f"\nDone. Pushed {total_pushed} messages to Honcho.")
    print(f"Errors: {errors}")
    print(f"State saved to {STATE_FILE}")


def run_incremental_sync(dry_run: bool = False) -> None:
    """Incrementally sync notes modified since the last ingestion."""
    state = load_state()

    last_sync = state.get("last_sync")
    if not last_sync:
        print("No previous sync found. Run --initial first.")
        return

    dt = datetime.fromisoformat(last_sync.replace("Z", "+00:00"))
    since_ts = dt.timestamp()

    print(f"Last sync: {last_sync}")

    notes = get_notes(since_timestamp=since_ts)

    ingested = set(state.get("ingested_note_ids", []))
    to_ingest = [
        n
        for n in notes
        if n["note_id"] not in ingested and n["char_count"] <= MAX_MESSAGE_SIZE
    ]

    if not to_ingest:
        print("No new or modified notes to ingest.")
        return

    print(f"Found {len(to_ingest)} new/modified notes to ingest.")

    total_pushed, errors = _ingest_notes(to_ingest, state, dry_run=dry_run)

    state["last_sync"] = datetime.now(timezone.utc).isoformat()
    save_state(state)
    print(f"\nIncremental sync done. Pushed {total_pushed} messages.")


# ---------------------------------------------------------------------------
# Status
# ---------------------------------------------------------------------------


def print_status() -> None:
    """Print the current ingestion state."""
    state = load_state()

    print("Bear-to-Honcho Ingestion Status")
    print("=" * 40)
    print(f"Last sync: {state.get('last_sync', 'never')}")
    print(f"Ingested notes: {len(state.get('ingested_note_ids', []))}")

    stats = state.get("stats", {})
    if stats:
        print(f"Last run pushed: {stats.get('total_pushed', '?')}")
        print(f"Last run errors: {stats.get('errors', '?')}")
        print(f"Skipped (over limit): {stats.get('skipped_over_limit', '?')}")

    all_notes = get_notes()
    print(f"\nTotal Bear notes available: {len(all_notes)}")

    ingested = set(state.get("ingested_note_ids", []))
    remaining = sum(
        1
        for n in all_notes
        if n["note_id"] not in ingested and n["char_count"] <= MAX_MESSAGE_SIZE
    )
    print(f"Remaining to ingest: {remaining}")

    session_counts: dict[str, int] = {}
    for n in all_notes:
        if n["note_id"] not in ingested and n["char_count"] <= MAX_MESSAGE_SIZE:
            c = classify_note(n)
            session_counts[c["session_target"]] = session_counts.get(c["session_target"], 0) + 1

    if session_counts:
        print("\nRemaining by session:")
        for session, count in sorted(session_counts.items(), key=lambda x: -x[1]):
            print(f"  {session}: {count}")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Ingest Bear notes into Honcho memory system."
    )
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--initial", action="store_true", help="Full initial load")
    group.add_argument("--incremental", action="store_true", help="Sync only new/modified notes")
    group.add_argument("--status", action="store_true", help="Print ingestion status")
    group.add_argument("--dry-run", action="store_true", help="Print what would be done without API calls")

    parser.add_argument(
        "--max-notes", type=int, default=None, help="Limit number of notes (for testing)"
    )

    args = parser.parse_args()

    if args.status:
        print_status()
    elif args.dry_run:
        run_initial_load(dry_run=True, max_notes=args.max_notes)
    elif args.initial:
        run_initial_load(dry_run=False, max_notes=args.max_notes)
    elif args.incremental:
        run_incremental_sync(dry_run=False)


if __name__ == "__main__":
    main()