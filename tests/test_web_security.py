"""Static regressions for the dependency-free map UI's trust boundary."""
from pathlib import Path


HTML = (Path(__file__).parents[1] / "web" / "index.html").read_text(encoding="utf-8")


def test_untrusted_markup_has_escaping_and_safe_links():
    assert "const esc =" in HTML
    assert "const safeLink =" in HTML
    assert "${esc(d.summary)}" in HTML
    assert 'rel="noopener noreferrer"' in HTML


def test_hazards_use_one_browser_request():
    assert "fetch(`/live/hazards/all?" in HTML
    assert "fetch(`/live/traffic?" not in HTML


def test_live_updates_use_sse_with_slow_fallback():
    assert "new EventSource(`/live/stream?" in HTML
    assert "setInterval(load,60000)" in HTML
    assert "setInterval(load,15000)" not in HTML
