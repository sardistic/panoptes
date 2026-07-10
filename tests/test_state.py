"""Runtime-state backend selection without requiring PostgreSQL."""
from apb.store import state


def test_sqlite_is_default(monkeypatch):
    monkeypatch.delenv("APB_STATE_DATABASE_URL", raising=False)
    assert state.database_url() is None
    assert not state.is_postgres()


def test_native_postgres_dsn_enables_shared_state(monkeypatch):
    monkeypatch.setenv("APB_STATE_DATABASE_URL", "postgresql://user:pass@db/panoptes")
    assert state.is_postgres()


def test_sqlalchemy_style_dsn_is_rejected(monkeypatch):
    import pytest

    monkeypatch.setenv("APB_STATE_DATABASE_URL",
                       "postgresql+psycopg://user:pass@db/panoptes")
    with pytest.raises(ValueError, match="must use"):
        state.is_postgres()
