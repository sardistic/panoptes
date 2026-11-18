"""Turn pasted Censys / Shodan result pages into Rdio Scanner probe targets.

Both sites show more results to a person in a browser than their free APIs return,
so the cheapest way to grow the instance list is: run the search, copy the page,
paste it into a text file, and feed it here.

    python -m apb.discover.rdio_import pasted.txt [more.txt ...]
        -> adds new hosts to data/rdio_candidates.json (private, gitignored)
    python -m apb.discover.rdio_probe --proxy socks5h://warp-egress:1080 \\
        --skip-known --merge-into data/rdio_instances.json
        -> probes only the new ones and merges them into the instance list

Understood layouts (the text as copied from the page):
  Censys hosts:           an address, then "Host", ..., "Matched Fields", ...,
                          "3000 / HTTP" lines for the services that matched
  Censys web properties:  "203.0.113.5", ":", "3000", "Web Property"
  Shodan:                 a "Rdio Scanner" title line, a date, then the address
                          and hostnames (no port shown -> the usual ports are tried)
Anything else that looks like host:port is taken too.
"""
from __future__ import annotations

import argparse
import json
import re
from pathlib import Path

IP = r"(?:\d{1,3}\.){3}\d{1,3}"
HOSTNAME = r"(?:[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?\.)+[a-z]{2,24}"
_IP_LINE = re.compile(rf"^\s*({IP})\s*$")
_HOST_LINE = re.compile(rf"^\s*({HOSTNAME})\s*$", re.I)
_SERVICE = re.compile(r"^\s*(\d{1,5})\s*/\s*HTTP\s*$")
_HOSTPORT = re.compile(rf"\b({IP}|{HOSTNAME}):(\d{{2,5}})\b", re.I)
_PLACE = re.compile(r"^\s*([A-Z][\w .'-]+),\s*([A-Za-z .'-]+)\s*\((US|CA|AU|GB|IE|NZ|FI|IT|CZ)\)\s*$")
_US_STATES = {
    "Alabama": "AL", "Alaska": "AK", "Arizona": "AZ", "Arkansas": "AR", "California": "CA",
    "Colorado": "CO", "Connecticut": "CT", "Delaware": "DE", "Florida": "FL", "Georgia": "GA",
    "Hawaii": "HI", "Idaho": "ID", "Illinois": "IL", "Indiana": "IN", "Iowa": "IA", "Kansas": "KS",
    "Kentucky": "KY", "Louisiana": "LA", "Maine": "ME", "Maryland": "MD", "Massachusetts": "MA",
    "Michigan": "MI", "Minnesota": "MN", "Mississippi": "MS", "Missouri": "MO", "Montana": "MT",
    "Nebraska": "NE", "Nevada": "NV", "New Hampshire": "NH", "New Jersey": "NJ", "New Mexico": "NM",
    "New York": "NY", "North Carolina": "NC", "North Dakota": "ND", "Ohio": "OH", "Oklahoma": "OK",
    "Oregon": "OR", "Pennsylvania": "PA", "Rhode Island": "RI", "South Carolina": "SC",
    "South Dakota": "SD", "Tennessee": "TN", "Texas": "TX", "Utah": "UT", "Vermont": "VT",
    "Virginia": "VA", "Washington": "WA", "West Virginia": "WV", "Wisconsin": "WI", "Wyoming": "WY",
    "District of Columbia": "DC"}
_DATACENTER = re.compile(r"digitalocean|amazon|google|microsoft|oracle|linode|akamai|vultr|ovh|"
                         r"hetzner|contabo|ionos|hurricane|cloudflare|constant company|aws", re.I)
DEFAULT_PORTS = (3000, 443, 80)
_NOT_HOSTS = re.compile(r"\.(png|jpg|js|css|html?)$|^(www\.)?(censys|shodan)\.io$|"
                        r"googleusercontent|amazonaws|linodeusercontent|vultrusercontent|"
                        r"your-server\.de|contaboserver|\.(res|dsl|dynamic)\.|"
                        r"^(c|h|pool|static|syn|bras)-|lightspeed|fios|hsd1", re.I)


def parse(text: str) -> list[dict]:
    """[{host, ports, place, dc}] from one pasted page. Hosts without a port get the
    usual Rdio Scanner ports."""
    lines = text.splitlines()
    found: dict[str, dict] = {}

    def add(host: str, port: int | None = None, place: str | None = None, dc: bool | None = None):
        host = host.strip().lower().rstrip(".")
        if not host or _NOT_HOSTS.search(host) and not _IP_LINE.match(host):
            return None
        e = found.setdefault(host, {"host": host, "ports": set(), "place": None, "dc": False})
        if port and 0 < port < 65536:
            e["ports"].add(port)
        if place and not e["place"]:
            e["place"] = place
        if dc:
            e["dc"] = True
        return e

    i = 0
    current = None
    while i < len(lines):
        line = lines[i].strip()
        nxt = lines[i + 1].strip() if i + 1 < len(lines) else ""
        nxt2 = lines[i + 2].strip() if i + 2 < len(lines) else ""
        m_ip, m_host = _IP_LINE.match(line), _HOST_LINE.match(line)
        if (m_ip or m_host) and nxt == ":" and nxt2.isdigit():          # Censys web property
            add((m_ip or m_host).group(1), int(nxt2))
            current = None
            i += 3
            continue
        if m_ip and nxt == "Host":                                       # Censys host block
            current = add(m_ip.group(1))
            i += 2
            continue
        if m_ip and current is None:                                     # Shodan: bare address
            current = add(m_ip.group(1))
            i += 1
            continue
        if current is not None:
            if m_host and not m_ip and not _NOT_HOSTS.search(m_host.group(1)):
                add(m_host.group(1)).update(place=current.get("place"))  # Shodan hostname
            svc = _SERVICE.match(line)
            if svc and "matched" in current:
                current["ports"].add(int(svc.group(1)))
            if line == "Matched Fields":
                current["matched"] = True
            pl = _PLACE.match(line)
            if pl:
                region = _US_STATES.get(pl.group(2).strip(), pl.group(2).strip())
                current["place"] = (f"{pl.group(1).strip()}, {region}" if pl.group(3) == "US"
                                    else f"{pl.group(1).strip()}, {region}, {pl.group(3)}")
            if _DATACENTER.search(line) and ("(" in line or "LLC" in line or "Inc" in line):
                current["dc"] = True
        for h, p in _HOSTPORT.findall(lines[i]):
            add(h, int(p))
        i += 1
    out = []
    for e in found.values():
        ports = sorted(e["ports"]) or list(DEFAULT_PORTS)
        out.append({"host": e["host"], "ports": ports, "place": e.get("place"), "dc": bool(e["dc"])})
    return out


def merge_candidates(path: Path, new: list[dict]) -> tuple[int, int]:
    """Add parsed hosts to the candidates file. Returns (new hosts, new host:ports)."""
    try:
        cands = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        cands = {"note": "Rdio Scanner candidates (private: operators' addresses)", "hosts": [], "named": []}
    have: dict[str, dict] = {}
    for h in cands.get("hosts", []) + cands.get("multi_instance_operators", []):
        have[h["ip"]] = h
    named = set(cands.get("named", []))
    new_hosts = new_ports = 0
    for e in new:
        if _IP_LINE.match(e["host"]):
            row = have.get(e["host"])
            if row is None:
                row = {"ip": e["host"], "ports": [], "place": e["place"], "dc": e["dc"] or None}
                cands.setdefault("hosts", []).append(row)
                have[e["host"]] = row
                new_hosts += 1
            before = set(row.get("ports") or [])
            row["ports"] = sorted(before | set(e["ports"]))
            new_ports += len(set(row["ports"]) - before)
            if e["place"] and not row.get("place"):
                row["place"] = e["place"]
        else:
            for p in e["ports"]:
                name = e["host"] if p == 443 else f"{e['host']}:{p}"
                if name not in named and e["host"] not in named:
                    named.add(name)
                    new_ports += 1
            if not any(n.split(":")[0] == e["host"] for n in cands.get("named", [])):
                new_hosts += 1
    cands["named"] = sorted(named)
    path.write_text(json.dumps(cands, indent=1), encoding="utf-8")
    return new_hosts, new_ports


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    ap.add_argument("files", nargs="+", help="text files holding pasted result pages")
    ap.add_argument("--candidates", default="data/rdio_candidates.json")
    args = ap.parse_args(argv)
    parsed: list[dict] = []
    for f in args.files:
        parsed += parse(Path(f).read_text(encoding="utf-8", errors="replace"))
    hosts, ports = merge_candidates(Path(args.candidates), parsed)
    print(f"parsed {len(parsed)} hosts; {hosts} new hosts, {ports} new host:port targets "
          f"-> {args.candidates}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
