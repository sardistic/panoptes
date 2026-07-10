"""Snapshot store: upsert, time-window query, prune — against a temp SQLite file."""
import time

import pytest

from apb.store import snapshots


@pytest.fixture()
def store(tmp_path, monkeypatch):
    monkeypatch.setattr(snapshots, "DB_PATH", tmp_path / "test.sqlite")
    monkeypatch.setattr(snapshots, "_conn", None)
    yield snapshots
    if snapshots._conn is not None:
        snapshots._conn.close()


def _row(call_id="1", ts=None, **kw):
    d = {"metro": "seattle", "call_id": call_id, "type": "fire",
         "summary": "Structure fire", "location": "1st Ave", "sentiment": "high",
         "threat_score": 0.8, "emerging": True, "lat": 47.61, "lon": -122.33,
         "at": "2026-07-01T12:00:00", "ts": ts if ts is not None else time.time()}
    d.update(kw)
    return d


def test_record_upserts_on_metro_call_id(store):
    assert store.record([_row()]) == 1
    store.record([_row()])                       # same key -> update, not duplicate
    assert store.stats()["total"] == 1
    store.record([_row(call_id="2")])
    assert store.stats()["total"] == 2


def test_rows_without_coords_or_id_are_skipped(store):
    assert store.record([_row(lat=None), _row(call_id=None)]) == 0


def test_query_respects_time_window(store):
    store.record([_row(call_id="old", ts=time.time() - 7200),
                  _row(call_id="new", ts=time.time())])
    got = store.query(max_age_hours=1.0)
    assert [d["call_id"] for d in got] == ["new"]
    assert len(store.query(max_age_hours=3.0)) == 2


def test_prune_drops_only_ancient_rows(store):
    store.record([_row(call_id="ancient", ts=time.time() - 40 * 86400),
                  _row(call_id="fresh")])
    # prune keys off event time; the fresh upsert's last_seen keeps it alive
    assert store.prune(max_age_days=30.0) >= 1
    assert [d["call_id"] for d in store.query(max_age_hours=1)] == ["fresh"]


def test_dict_values_are_stringified(store):
    store.record([_row(call_id="d", location={"raw": "1st Ave"})])
    assert isinstance(store.query(max_age_hours=1)[0]["location"], str)


def test_shared_connection_is_serialized_and_configured(store):
    from apb.store import events, sigbuf
    assert events._lock is store.db_lock
    assert sigbuf._lock is store.db_lock
    c = store.conn()
    assert c.execute("PRAGMA busy_timeout").fetchone()[0] == 10_000
    assert c.execute("PRAGMA journal_mode").fetchone()[0].lower() == "wal"
    assert c.execute("PRAGMA user_version").fetchone()[0] == 1
