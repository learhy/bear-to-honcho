"""Tests for bear_to_honcho ingestion script.

Tests cover:
- Note classification (type, people, session routing)
- Message formatting (header, truncation, metadata)
- State management (load/save)
- Session name generation
- FTS5 note reading
- Honcho API helpers (workspace/peer/session/message push)
"""

import json
import os
import sqlite3
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

# Import the module under test
import sys
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import bear_to_honcho


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def tmp_fts_db():
    """Create a temporary FTS5 mirror with a few test notes."""
    db_path = tempfile.mktemp(suffix=".db")
    conn = sqlite3.connect(db_path)
    conn.execute("""
        CREATE TABLE notes (
            note_id TEXT PRIMARY KEY,
            title TEXT NOT NULL,
            content TEXT NOT NULL DEFAULT '',
            tags TEXT NOT NULL DEFAULT '',
            created REAL NOT NULL,
            modified REAL NOT NULL,
            archived INTEGER NOT NULL DEFAULT 0,
            char_count INTEGER NOT NULL DEFAULT 0,
            bear_url TEXT NOT NULL DEFAULT ''
        )
    """)
    conn.execute("""
        CREATE VIRTUAL TABLE notes_fts USING fts5(
            title, content, tags,
            content='notes',
            content_rowid='rowid',
            tokenize='porter unicode61'
        )
    """)
    # Insert test notes
    test_notes = [
        ("note-1", "Aman 1:1", "Discussed agentic access control with Aman", "hashicorp,1:1s", 1718724564.0, 1718724564.0, 0, 45, ""),
        ("note-2", "Team Meeting", "Roadmap review with Rachana and Pradeep", "hashicorp,meeting-notes", 1718800000.0, 1718800000.0, 0, 50, ""),
        ("note-3", "Q4 Planning", "Planning diary for Q4 Boundary work", "hashicorp,planning,diary", 1718900000.0, 1718900000.0, 0, 40, ""),
        ("note-4", "Iris birthday", "Family note about Iris", "personal,family", 1719000000.0, 1719000000.0, 0, 30, ""),
        ("note-5", "Random thought", "General note with no specific people", "", 1719100000.0, 1719100000.0, 0, 20, ""),
        ("note-6", "Archived note", "This should be skipped", "meetings", 1719200000.0, 1719200000.0, 1, 10, ""),
        ("note-7", "Big note", "x" * 26000, "", 1719300000.0, 1719300000.0, 0, 26000, ""),
    ]
    for note_id, title, content, tags, created, modified, archived, char_count, bear_url in test_notes:
        conn.execute(
            "INSERT INTO notes (note_id, title, content, tags, created, modified, archived, char_count, bear_url) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (note_id, title, content, tags, created, modified, archived, char_count, bear_url),
        )
    conn.commit()
    conn.close()
    yield db_path
    if os.path.exists(db_path):
        os.remove(db_path)


@pytest.fixture
def tmp_state_file():
    """Create a temporary state file path."""
    state_path = tempfile.mktemp(suffix=".json")
    yield state_path
    if os.path.exists(state_path):
        os.remove(state_path)


# ---------------------------------------------------------------------------
# Session naming
# ---------------------------------------------------------------------------


class TestSessionNameForPerson:
    def test_simple_name(self):
        assert bear_to_honcho.session_name_for_person("Aman") == "bear-notes-aman"

    def test_multi_word_name(self):
        assert bear_to_honcho.session_name_for_person("Katie Simmons") == "bear-notes-katie-simmons"

    def test_name_with_dots(self):
        assert bear_to_honcho.session_name_for_person("Yang Li") == "bear-notes-yang-li"

    def test_name_with_special_chars(self):
        result = bear_to_honcho.session_name_for_person("J. Smith")
        assert result == "bear-notes-j-smith"


# ---------------------------------------------------------------------------
# State management
# ---------------------------------------------------------------------------


class TestStateManagement:
    def test_load_state_empty(self, tmp_state_file):
        with patch.object(bear_to_honcho, "STATE_FILE", tmp_state_file):
            state = bear_to_honcho.load_state()
            assert state["last_sync"] is None
            assert state["ingested_note_ids"] == []
            assert state["stats"] == {}

    def test_save_and_load_state(self, tmp_state_file):
        with patch.object(bear_to_honcho, "STATE_FILE", tmp_state_file):
            state = {
                "last_sync": "2026-06-30T12:00:00+00:00",
                "ingested_note_ids": ["note-1", "note-2"],
                "stats": {"total_pushed": 2, "errors": 0},
            }
            bear_to_honcho.save_state(state)
            loaded = bear_to_honcho.load_state()
            assert loaded["last_sync"] == "2026-06-30T12:00:00+00:00"
            assert loaded["ingested_note_ids"] == ["note-1", "note-2"]
            assert loaded["stats"]["total_pushed"] == 2

    def test_save_state_creates_parent_dir(self, tmp_state_file):
        nested_path = os.path.join(os.path.dirname(tmp_state_file), "subdir", "state.json")
        with patch.object(bear_to_honcho, "STATE_FILE", nested_path):
            bear_to_honcho.save_state({"last_sync": None, "ingested_note_ids": [], "stats": {}})
            assert os.path.exists(nested_path)


# ---------------------------------------------------------------------------
# Note reading
# ---------------------------------------------------------------------------


class TestGetNotes:
    def test_get_all_notes(self, tmp_fts_db):
        with patch.object(bear_to_honcho, "BEAR_FTS_DB", tmp_fts_db):
            notes = bear_to_honcho.get_notes()
            assert len(notes) == 6  # 7 total minus 1 archived

    def test_get_notes_with_limit(self, tmp_fts_db):
        with patch.object(bear_to_honcho, "BEAR_FTS_DB", tmp_fts_db):
            notes = bear_to_honcho.get_notes(limit=3)
            assert len(notes) == 3

    def test_get_notes_since_timestamp(self, tmp_fts_db):
        with patch.object(bear_to_honcho, "BEAR_FTS_DB", tmp_fts_db):
            notes = bear_to_honcho.get_notes(since_timestamp=1718850000.0)
            # Notes with modified > 1718850000.0: note-3, note-4, note-5, note-7
            assert len(notes) == 4
            for n in notes:
                assert n["modified"] > 1718850000.0

    def test_notes_ordered_by_modified_asc(self, tmp_fts_db):
        with patch.object(bear_to_honcho, "BEAR_FTS_DB", tmp_fts_db):
            notes = bear_to_honcho.get_notes()
            for i in range(len(notes) - 1):
                assert notes[i]["modified"] <= notes[i + 1]["modified"]

    def test_archived_notes_excluded(self, tmp_fts_db):
        with patch.object(bear_to_honcho, "BEAR_FTS_DB", tmp_fts_db):
            notes = bear_to_honcho.get_notes()
            for n in notes:
                assert n["note_id"] != "note-6"

    def test_note_has_expected_fields(self, tmp_fts_db):
        with patch.object(bear_to_honcho, "BEAR_FTS_DB", tmp_fts_db):
            notes = bear_to_honcho.get_notes(limit=1)
            n = notes[0]
            assert "note_id" in n
            assert "title" in n
            assert "content" in n
            assert "tags" in n
            assert "created" in n
            assert "modified" in n
            assert "char_count" in n


# ---------------------------------------------------------------------------
# Note classification
# ---------------------------------------------------------------------------


class TestClassifyNote:
    def test_1on1_by_tag(self):
        note = {"title": "Weekly sync", "content": "discussion", "tags": "hashicorp,1:1s"}
        c = bear_to_honcho.classify_note(note)
        assert c["type"] == "1:1"

    def test_1on1_by_title(self):
        note = {"title": "1:1 with Aman", "content": "discussion", "tags": ""}
        c = bear_to_honcho.classify_note(note)
        assert c["type"] == "1:1"

    def test_meeting_by_tag(self):
        note = {"title": "Roadmap", "content": "discussion", "tags": "hashicorp,meeting-notes"}
        c = bear_to_honcho.classify_note(note)
        assert c["type"] == "meeting"

    def test_planning_by_tag(self):
        note = {"title": "Q4", "content": "planning", "tags": "planning,diary"}
        c = bear_to_honcho.classify_note(note)
        assert c["type"] == "planning"

    def test_personal_by_tag(self):
        note = {"title": "Iris", "content": "family", "tags": "personal,family"}
        c = bear_to_honcho.classify_note(note)
        assert c["type"] == "personal"

    def test_general_default(self):
        note = {"title": "Random", "content": "stuff", "tags": ""}
        c = bear_to_honcho.classify_note(note)
        assert c["type"] == "general"

    def test_finds_person_aman(self):
        note = {"title": "Sync", "content": "Discussed agentic access control with Aman", "tags": ""}
        c = bear_to_honcho.classify_note(note)
        assert "Aman" in c["people"]

    def test_finds_person_katie_simmons(self):
        note = {"title": "Eng sync", "content": "Katie Simmons said we need to ship", "tags": ""}
        c = bear_to_honcho.classify_note(note)
        assert "Katie Simmons" in c["people"]

    def test_finds_multiple_people(self):
        note = {"title": "Team", "content": "Rachana and Pradeep discussed the roadmap", "tags": ""}
        c = bear_to_honcho.classify_note(note)
        assert "Rachana" in c["people"]
        assert "Pradeep" in c["people"]

    def test_no_people_found(self):
        note = {"title": "Random", "content": "Just a note about nothing", "tags": ""}
        c = bear_to_honcho.classify_note(note)
        assert c["people"] == []

    def test_session_target_with_person(self):
        note = {"title": "Aman sync", "content": "Discussion with Aman", "tags": ""}
        c = bear_to_honcho.classify_note(note)
        assert c["session_target"] == "bear-notes-aman"

    def test_session_target_no_person(self):
        note = {"title": "Random", "content": "Nothing", "tags": ""}
        c = bear_to_honcho.classify_note(note)
        assert c["session_target"] == "bear-notes-general"

    def test_first_person_determines_session(self):
        note = {"title": "Sync", "content": "Rachana and Aman discussed stuff", "tags": ""}
        c = bear_to_honcho.classify_note(note)
        # Rachana appears first in TRACKED_PEOPLE list
        assert c["session_target"] == "bear-notes-rachana"


# ---------------------------------------------------------------------------
# Message formatting
# ---------------------------------------------------------------------------


class TestFormatNoteForHoncho:
    def test_header_contains_type(self):
        note = {
            "title": "Test",
            "content": "Content here",
            "tags": "hashicorp,1:1s",
            "modified": 1718724564.0,
        }
        classification = {"type": "1:1", "people": ["Aman"], "session_target": "bear-notes-aman"}
        msg = bear_to_honcho.format_note_for_honcho(note, classification)
        assert "[Bear Note — 1:1]" in msg

    def test_header_contains_title(self):
        note = {"title": "My Meeting", "content": "x", "tags": "", "modified": 1718724564.0}
        classification = {"type": "meeting", "people": [], "session_target": "bear-notes-general"}
        msg = bear_to_honcho.format_note_for_honcho(note, classification)
        assert "Title: My Meeting" in msg

    def test_header_contains_date(self):
        note = {"title": "T", "content": "x", "tags": "", "modified": 1718724564.0}
        classification = {"type": "general", "people": [], "session_target": "bear-notes-general"}
        msg = bear_to_honcho.format_note_for_honcho(note, classification)
        assert "Date: 2024-06-18" in msg

    def test_header_contains_people(self):
        note = {"title": "T", "content": "x", "tags": "", "modified": 1718724564.0}
        classification = {"type": "1:1", "people": ["Aman", "Pradeep"], "session_target": "bear-notes-aman"}
        msg = bear_to_honcho.format_note_for_honcho(note, classification)
        assert "People: Aman, Pradeep" in msg

    def test_header_contains_none_when_no_people(self):
        note = {"title": "T", "content": "x", "tags": "", "modified": 1718724564.0}
        classification = {"type": "general", "people": [], "session_target": "bear-notes-general"}
        msg = bear_to_honcho.format_note_for_honcho(note, classification)
        assert "People: none" in msg

    def test_content_truncated_when_over_limit(self):
        note = {
            "title": "Big",
            "content": "x" * 26000,
            "tags": "",
            "modified": 1718724564.0,
        }
        classification = {"type": "general", "people": [], "session_target": "bear-notes-general"}
        msg = bear_to_honcho.format_note_for_honcho(note, classification)
        assert len(msg) <= bear_to_honcho.MAX_MESSAGE_SIZE
        assert "[... truncated ...]" in msg

    def test_content_not_truncated_when_under_limit(self):
        note = {
            "title": "Small",
            "content": "short content",
            "tags": "",
            "modified": 1718724564.0,
        }
        classification = {"type": "general", "people": [], "session_target": "bear-notes-general"}
        msg = bear_to_honcho.format_note_for_honcho(note, classification)
        assert "[... truncated ...]" not in msg
        assert "short content" in msg


# ---------------------------------------------------------------------------
# Honcho API helpers (mocked)
# ---------------------------------------------------------------------------


class TestHonchoAPI:
    def test_honcho_headers(self):
        h = bear_to_honcho.honcho_headers()
        assert h["Content-Type"] == "application/json"

    def test_honcho_post_dry_run(self):
        result = bear_to_honcho.honcho_post("/test", {"key": "value"}, dry_run=True)
        assert result is None

    def test_ensure_workspace_exists(self):
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        with patch("bear_to_honcho.requests.get", return_value=mock_resp):
            assert bear_to_honcho.ensure_workspace() is True

    def test_ensure_workspace_creates_if_missing(self):
        mock_resp = MagicMock()
        mock_resp.status_code = 404
        mock_post_resp = MagicMock()
        mock_post_resp.status_code = 201
        with patch("bear_to_honcho.requests.get", return_value=mock_resp):
            with patch("bear_to_honcho.requests.post", return_value=mock_post_resp):
                assert bear_to_honcho.ensure_workspace() is True

    def test_push_messages_dry_run(self):
        count = bear_to_honcho.push_messages("test-session", "dan-rohan", ["msg1", "msg2"], dry_run=True)
        assert count == 2

    def test_push_messages_empty_batch(self):
        count = bear_to_honcho.push_messages("test-session", "dan-rohan", [], dry_run=False)
        assert count == 0

    def test_push_messages_real(self):
        mock_response = MagicMock()
        mock_response.status_code = 201
        mock_response.json.return_value = [{"id": "msg-1"}, {"id": "msg-2"}]
        with patch("bear_to_honcho.requests.post", return_value=mock_response):
            count = bear_to_honcho.push_messages("session", "peer", ["msg1", "msg2"], dry_run=False)
            assert count == 2


# ---------------------------------------------------------------------------
# Integration: dry-run end-to-end
# ---------------------------------------------------------------------------


class TestDryRunIntegration:
    def test_dry_run_with_real_fts(self, tmp_fts_db, tmp_state_file):
        """Test the dry-run path with a real FTS5 DB and mocked Honcho API."""
        with patch.object(bear_to_honcho, "BEAR_FTS_DB", tmp_fts_db), \
             patch.object(bear_to_honcho, "STATE_FILE", tmp_state_file):
            # Dry run should not make any API calls or modify state
            bear_to_honcho.run_initial_load(dry_run=True, max_notes=5)

            # State should still be saved (dry-run path saves state too)
            state = bear_to_honcho.load_state()
            # Dry run pushes 0 messages
            assert state["stats"]["total_pushed"] == 0