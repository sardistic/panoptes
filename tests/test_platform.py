"""Keyed API platform: applications, approval, claiming, relays, throttling, map lock."""
from __future__ import annotations

import pytest

ADMIN = "a" * 32
GOOD = {"name": "Ada Researcher", "email": "ada@example.org", "org": "Example Lab",
        "use_case": "Studying how quickly hazard alerts propagate to local news, for a university course.",
        "volume": "500/day", "datasets": ["usgs_quakes", "radio_glossary", "nope"], "agree_terms": True}


@pytest.fixture()
def plat(tmp_path, monkeypatch):
    from apb.alerts import mail
    from apb.api import main, platform
    from apb.store import apikeys, snapshots
    monkeypatch.setattr(snapshots, "DB_PATH", tmp_path / "t.sqlite")
    monkeypatch.setattr(snapshots, "_conn", None)
    monkeypatch.setattr(apikeys, "_ready", False)
    monkeypatch.delenv("APB_STATE_DATABASE_URL", raising=False)
    monkeypatch.setenv("APB_ADMIN_TOKEN", ADMIN)
    for name in ("_buckets", "_daily", "_pending", "_pending_used", "_cache", "_inflight"):
        getattr(platform, name).clear()
    monkeypatch.setattr(platform, "_sem", None)
    monkeypatch.setattr(platform, "_registry", None)
    monkeypatch.setattr(platform, "_tiers", None)
    main._hits.clear()
    main._anon_hits.clear()
    sent = []
    monkeypatch.setattr(mail, "notify_owner", lambda subject, body, wait=False: sent.append((subject, body)))
    mailed = []
    monkeypatch.setattr(mail, "send", lambda to, subject, body, wait=False: mailed.append((to, subject, body)))
    from fastapi.testclient import TestClient
    c = TestClient(main.app)
    c.sent = sent
    c.mailed = mailed
    yield c
    if snapshots._conn is not None:
        snapshots._conn.close()


def _admin(c, method, path, **kw):
    return getattr(c, method)(path, headers={"Authorization": f"Bearer {ADMIN}"}, **kw)


def _approved_key(c, tier="research", **approve):
    a = c.post("/v1/apply", json=GOOD).json()
    assert _admin(c, "post", f"/v1/admin/applications/{a['application']}/approve",
                  json={"tier": tier, **approve}).status_code == 200
    r = c.post("/v1/apply/claim", headers={"X-Claim-Token": a["claim_token"]})
    assert r.status_code == 200
    return r.json()["api_key"], a


def test_apply_validates_notifies_owner_and_limits_per_ip(plat):
    c = plat
    assert c.post("/v1/apply", json={**GOOD, "email": "nope"}).status_code == 400
    assert c.post("/v1/apply", json={**GOOD, "use_case": "data pls"}).status_code == 400
    assert c.post("/v1/apply", json={**GOOD, "agree_terms": False}).status_code == 400
    assert c.post("/v1/apply", json={**GOOD, "website2": "spam"}).status_code == 202   # honeypot, silently
    assert c.sent == []
    r = c.post("/v1/apply", json=GOOD)
    assert r.status_code == 201
    j = r.json()
    assert j["status"] == "pending" and j["claim_token"] and "#claim=" in j["claim_url"]
    subject, body = c.sent[0]
    assert "Ada Researcher" in subject and "ada@example.org" in body and "university course" in body
    assert "usgs_quakes" in body and "nope" not in body                      # unknown datasets dropped
    assert f"/developers/admin/#{j['application']}" in body
    to, subj, text = c.mailed[0]                                              # the applicant's receipt
    assert to == "ada@example.org" and "received" in subj and f"#claim={j['claim_token']}" in text
    st = c.get("/v1/apply/status", headers={"X-Claim-Token": j["claim_token"]}).json()
    assert st["status"] == "pending" and not st["key_issued"]
    assert c.post("/v1/apply/claim", headers={"X-Claim-Token": j["claim_token"]}).status_code == 409
    c.post("/v1/apply", json=GOOD)
    c.post("/v1/apply", json=GOOD)
    assert c.post("/v1/apply", json=GOOD).status_code == 429                 # 3 an hour per address


def test_admin_needs_the_token_and_key_is_shown_once(plat):
    c = plat
    assert c.get("/v1/admin/applications").status_code == 401
    assert c.get("/v1/admin/applications", headers={"Authorization": "Bearer wrong"}).status_code == 401
    key, a = _approved_key(c)
    assert key.startswith("pk_")
    again = c.post("/v1/apply/claim", headers={"X-Claim-Token": a["claim_token"]})
    assert again.status_code == 409
    me = c.get("/v1/me", headers={"X-API-Key": key}).json()
    assert me["tier"] == "research" and "usgs_quakes" in me["datasets"]
    assert "incidents" in me["datasets"]
    keys = _admin(c, "get", "/v1/admin/keys").json()["keys"]
    assert len(keys) == 1 and "key_hash" not in keys[0] and key not in str(keys)
    # lost key: owner revokes and reissues, the old key stops working, the link yields a new one
    assert _admin(c, "post", f"/v1/admin/applications/{a['application']}/reissue").json()["ok"]
    assert c.get("/v1/me", headers={"X-API-Key": key}).status_code == 401
    new = c.post("/v1/apply/claim", headers={"X-Claim-Token": a["claim_token"]}).json()["api_key"]
    assert new != key and c.get("/v1/me", headers={"Authorization": f"Bearer {new}"}).status_code == 200


def test_denied_application_never_gets_a_key(plat):
    c = plat
    a = c.post("/v1/apply", json=GOOD).json()
    _admin(c, "post", f"/v1/admin/applications/{a['application']}/deny", json={"note": "Not a fit"})
    st = c.get("/v1/apply/status", headers={"X-Claim-Token": a["claim_token"]}).json()
    assert st["status"] == "denied" and st["note"] == "Not a fit"
    assert c.post("/v1/apply/claim", headers={"X-Claim-Token": a["claim_token"]}).status_code == 409


def test_relay_caches_strips_and_wraps(plat, monkeypatch):
    from apb.api import platform
    c = plat
    key, _ = _approved_key(c)
    reg = platform.registry()
    reg["datasets"]["radio_glossary"]["strip"] = ["generic"]
    h = {"X-API-Key": key}
    r1 = c.get("/v1/data/radio_glossary", headers=h)
    r2 = c.get("/v1/data/radio_glossary?ignored=1", headers=h)
    assert r1.status_code == 200 and r1.headers["X-Cache"] == "MISS" and r2.headers["X-Cache"] == "HIT"
    j = r1.json()
    assert j["dataset"] == "radio_glossary" and j["attribution"] and "codes" in j["data"]
    assert "generic" not in j["data"]
    assert int(r2.headers["X-RateLimit-Remaining"]) < int(r1.headers["X-RateLimit-Remaining"])
    assert platform._strip({"a": [{"summaries": 1, "b": {"summaries": 2, "c": 3}}]}, {"summaries"}) == \
        {"a": [{"b": {"c": 3}}]}


def test_access_rules(plat):
    c = plat
    assert c.get("/v1/data/usgs_quakes").status_code == 401
    key, _ = _approved_key(c, datasets=["radio_glossary"])                   # owner limits the key
    h = {"X-API-Key": key}
    assert c.get("/v1/data/radio_glossary", headers=h).status_code == 200   # granted
    assert c.get("/v1/data/usgs_quakes", headers=h).status_code == 403      # enabled, not granted
    assert c.get("/v1/data/events", headers=h).status_code == 403
    from apb.api import platform
    platform.registry()["datasets"]["incidents"]["status"] = "review"
    assert c.get("/v1/data/incidents", headers=h).status_code == 404        # under review
    r = c.get("/v1/data/cameras", headers=h)
    assert r.status_code == 404 and "agency terms" in r.json()["error"]
    assert c.get("/v1/data/radio_glossary/123", headers=h).status_code == 404
    cat = c.get("/v1/datasets").json()
    ids = {d["id"]: d for d in cat["datasets"]}
    assert ids["usgs_quakes"]["status"] == "enabled" and ids["incidents"]["status"] == "review"
    assert ids["internet_outages"]["noncommercial"] and not ids["usgs_quakes"]["noncommercial"]
    assert "cameras" in cat["not_available"] and "research" in cat["tiers"]


def test_rate_and_daily_quota(plat, monkeypatch):
    from apb.api import platform
    c = plat
    key, _ = _approved_key(c)
    platform.tiers()["tiers"]["research"].update(per_minute=2, per_day=1000)
    h = {"X-API-Key": key}
    assert c.get("/v1/data/radio_glossary", headers=h).status_code == 200
    assert c.get("/v1/data/radio_glossary", headers=h).status_code == 200
    r = c.get("/v1/data/radio_glossary", headers=h)
    assert r.status_code == 429 and int(r.headers["Retry-After"]) >= 1
    platform._buckets.clear()
    platform.tiers()["tiers"]["research"].update(per_minute=100, per_day=3)
    assert c.get("/v1/data/radio_glossary", headers=h).status_code == 200
    r = c.get("/v1/data/radio_glossary", headers=h)
    assert r.status_code == 429 and "quota" in r.json()["error"]
    platform._flush(force=True)
    assert c.get("/v1/me", headers=h).json()["used_today"] == 3
    kid = _admin(c, "get", "/v1/admin/keys").json()["keys"][0]["id"]
    assert _admin(c, "post", f"/v1/admin/keys/{kid}", json={"revoke": True}).json()["ok"]
    assert c.get("/v1/me", headers=h).status_code == 401


def test_map_routes_throttle_callers_that_are_not_the_map(plat, monkeypatch):
    from apb.api import main, platform
    c = plat
    monkeypatch.setattr(main, "_ANON_PER_MIN", 2)
    for _ in range(2):
        assert c.get("/live/radio/glossary").status_code == 200
    r = c.get("/live/radio/glossary")
    assert r.status_code == 429 and r.json()["docs"] == "/developers/"
    assert c.get("/live/radio/glossary", headers={"Sec-Fetch-Site": "same-origin"}).status_code == 200
    assert c.get("/live/radio/glossary", headers={"Referer": "http://testserver/"}).status_code == 200
    assert c.get("/live/radio/glossary", headers={"Sec-Fetch-Site": "cross-site",
                                                  "Referer": "http://testserver/"}).status_code == 429
    assert c.get("/live/radio/glossary", headers={"x-apb-internal": platform.INTERNAL_TOKEN}).status_code == 200
    assert c.get("/v1/datasets").status_code == 200                          # the API itself is not map-only


def test_client_ip_trusts_cloudflare_only_from_the_tunnel():
    from types import SimpleNamespace
    from apb.api.main import client_ip

    def req(peer, cf):
        return SimpleNamespace(client=SimpleNamespace(host=peer), headers={"cf-connecting-ip": cf} if cf else {})
    assert client_ip(req("172.18.0.5", "203.0.113.9")) == "203.0.113.9"
    assert client_ip(req("8.8.8.8", "203.0.113.9")) == "8.8.8.8"             # a public peer cannot pick
    assert client_ip(req("172.18.0.5", "not-an-ip")) == "172.18.0.5"
    assert client_ip(req("172.18.0.5", None)) == "172.18.0.5"


def test_decisions_are_emailed_with_a_working_link(plat):
    import re
    c = plat
    a = c.post("/v1/apply", json=GOOD).json()
    _admin(c, "post", f"/v1/admin/applications/{a['application']}/approve", json={"tier": "standard", "note": "Welcome"})
    to, subj, text = c.mailed[-1]
    assert to == "ada@example.org" and "ready" in subj and "Welcome" in text and "standard" in text
    emailed = re.search(r"#claim=(\S+)", text).group(1)
    assert emailed != a["claim_token"]
    # both the emailed link and the one shown on the page find the application
    assert c.get("/v1/apply/status", headers={"X-Claim-Token": emailed}).json()["status"] == "approved"
    assert c.get("/v1/apply/status", headers={"X-Claim-Token": a["claim_token"]}).json()["status"] == "approved"
    key = c.post("/v1/apply/claim", headers={"X-Claim-Token": emailed}).json()["api_key"]
    _admin(c, "post", f"/v1/admin/applications/{a['application']}/reissue")
    assert "new Panoptes API key" in c.mailed[-1][1]
    fresh = re.search(r"#claim=(\S+)", c.mailed[-1][2]).group(1)
    assert c.get("/v1/apply/status", headers={"X-Claim-Token": emailed}).status_code == 404   # replaced
    new = c.post("/v1/apply/claim", headers={"X-Claim-Token": fresh}).json()["api_key"]
    assert new != key
    b = c.post("/v1/apply", json={**GOOD, "email": "bob@example.org"}).json()
    n = len(c.mailed)
    _admin(c, "post", f"/v1/admin/applications/{b['application']}/deny", json={"note": "Too vague", "email": False})
    assert len(c.mailed) == n                                                # decided quietly
    _admin(c, "post", f"/v1/admin/applications/{b['application']}/deny", json={"note": "Too vague"})
    assert c.mailed[-1][0] == "bob@example.org" and "not approved" in c.mailed[-1][2]


def test_resend_request_shape(monkeypatch):
    from apb.alerts import mail
    calls = []

    class R:
        status_code = 200
        text = "{}"
    monkeypatch.setattr(mail.httpx, "post", lambda url, **kw: calls.append((url, kw)) or R())
    monkeypatch.setenv("RESEND_API_KEY", "re_test")
    monkeypatch.setenv("APB_OWNER_EMAIL", "owner@example.org")
    monkeypatch.delenv("APB_MAIL_FROM", raising=False)
    monkeypatch.delenv("APB_API_NOTIFY_WEBHOOK", raising=False)
    mail.notify_owner("New key request", "body", wait=True)
    url, kw = calls[0]
    assert url == "https://api.resend.com/emails" and kw["headers"]["Authorization"] == "Bearer re_test"
    assert kw["json"]["to"] == ["owner@example.org"] and kw["json"]["from"] == "Panoptes API <api@panoptes.run>"
    assert kw["json"]["reply_to"] == "api@panoptes.run" and kw["json"]["text"] == "body"
    monkeypatch.delenv("RESEND_API_KEY")
    mail.send("x@example.org", "s", "b", wait=True)                         # unconfigured: logged only
    assert len(calls) == 1
