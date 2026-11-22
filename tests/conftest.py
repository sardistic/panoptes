"""Shared test defaults."""
import pytest


@pytest.fixture(autouse=True)
def _map_lock_off(monkeypatch):
    """The test client is not the map page; the map-only allowance (APB_ANON_RATE_LIMIT)
    would throttle ordinary API tests. tests/test_platform.py turns it back on."""
    from apb.api import main
    monkeypatch.setattr(main, "_ANON_PER_MIN", 0)
    main._hits.clear()
    main._anon_hits.clear()
