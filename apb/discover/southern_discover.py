"""Discover Southern Software "Citizen Connect" agencies by brute-forcing the
predictable AgencyID patterns (County+Co+ST, County+Co911+ST, City+PD+ST, ...) against
the public CFS portal, then confirming each resolves to a real tenant.

Complements apb.discover.vendor_dork's `southern` dork target: dorking only finds
agencies Google/DDG has indexed, while these IDs follow a tight convention (seen across
the live tenants: HarnettCoNC, FlorenceCo911SC, PolkCoE911NC, DaphnePDAL, DecaturCoSOGA),
so probing scales past the indexed set — same idea as p2c_discover for P2C.

Validity probe (cheap, 1-2 GETs):
  GET /CADCFS_Public/index.php?AgencyID=<id>   -> sets session; a real tenant renders the
       CFS UI (references fetchesforajax/resttest.php); a bogus id falls back to the
       97KB agency-directory page.
  list-only agencies (county 911 centers) render the directory page too, so they're
  confirmed by a non-empty resttest call list instead.

Merges into data/southern_agencies.json, auto-loaded by apb.ingest.cad.load_southern().

Usage: python -m apb.discover.southern_discover
"""
from __future__ import annotations

import argparse
import json
import time
from pathlib import Path

import httpx

_UA = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
       "(KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36")
_BASE = "https://cc.southernsoftware.com"

# County names by state — Southern Software's footprint skews Southeastern.
COUNTIES = {
    "NC": """Harnett Edgecombe Transylvania Polk Duplin Bertie Guilford Wake Johnston
            Lee Moore Wilson Nash Pitt Craven Onslow Robeson Cumberland Rowan Davidson
            Randolph Catawba Burke Wayne Lenoir Sampson Columbus Brunswick Carteret""",
    "SC": """Abbeville Florence Berkeley Dorchester Anderson Greenwood Laurens Oconee
            Pickens Spartanburg York Lancaster Chester Sumter Orangeburg Aiken Lexington
            Darlington Marlboro Dillon Marion Georgetown Colleton Beaufort""",
    "AL": """Chilton Houston Dale Coffee Geneva Henry Etowah Calhoun Talladega Cullman
            Marshall Blount StClair Walker Tuscaloosa Shelby Elmore Autauga Lee Russell
            Baldwin Escambia Covington Dallas Morgan Limestone""",
    "GA": """Decatur Grady Thomas Mitchell Colquitt Tift Lowndes Worth Dougherty Lee
            Bibb Houston Floyd Bartow Whitfield Walker Catoosa Gordon Carroll Coweta
            Spalding Newton Walton Hall Habersham Effingham""",
    "TN": """Decatur Hardin Henderson McNairy Carroll Gibson Dyer Obion Weakley Tipton
            Lauderdale Hardeman Fayette Maury Giles Lincoln Coffee Franklin Warren
            Bradley McMinn Monroe Blount Sevier Cocke Greene""",
    "VA": """Botetourt Bedford Franklin Henry Pittsylvania Halifax Mecklenburg
            Brunswick Dinwiddie Prince George Sussex Southampton Isle of Wight
            Gloucester Accomack Northampton Tazewell Russell Wise Smyth Wythe""",
    "TX": """Archer Cass Crockett Dewitt Polk Wood Rusk Cherokee Nacogdoches Angelina
            Houston Trinity Walker Liberty Hardin Jasper Newton Tyler Shelby Panola
            Gregg Harrison Upshur Camp Titus Bowie""",
    "MS": """Hinds Harrison Desoto Jackson Rankin Madison Lee Forrest Lamar Lauderdale
            Jones PearlRiver Hancock Marshall Tate Panola Lafayette Pike Adams""",
    "LA": """Bossier Caddo Ouachita Rapides Calcasieu Lafayette Tangipahoa Livingston
            Ascension Terrebonne Lafourche StTammany Iberia Vermilion Acadia""",
    "KY": """Warren Hardin Daviess McCracken Boone Kenton Campbell Madison Pulaski Laurel
            Christian Bullitt Oldham Scott Franklin Bell Whitley Pike Floyd""",
    "AR": """Benton Washington Sebastian Faulkner Saline Craighead Garland Lonoke White
            Pope Jefferson Crittenden Baxter Boone Conway Independence""",
    "FL": """Columbia Suwannee Hamilton Baker Bradford Levy Dixie Gilchrist Taylor
            Jackson Walton Okaloosa SantaRosa Holmes Washington Calhoun Liberty Gadsden""",
}

# Cities with their own PD portal (City+PD+ST).
CITIES = {
    "AL": "Daphne Fairhope Dothan Enterprise Ozark GulfShores Foley Prattville Pelham",
    "NC": "Aberdeen Bessemer CarolinaBeach SouthernPines Pinehurst Sanford Clinton",
    "SC": "Florence Aiken Greenwood Easley Clemson Seneca Hartsville Camden",
    "GA": "Thomasville Tifton Valdosta Cordele Americus Cairo Moultrie",
    "TN": "Lexington Savannah Selmer Paris UnionCity Dyersburg Brownsville",
    "TX": "Lufkin Nacogdoches Livingston Jasper Center Carthage Henderson",
    "MS": "Hattiesburg Gulfport Biloxi Meridian Tupelo Columbus Greenville Natchez",
    "LA": "Bossier Ruston Monroe Houma Hammond Slidell Pineville Bastrop",
    "KY": "Richmond Somerset London Corbin Glasgow Elizabethtown Danville",
    "AR": "Conway Jonesboro Russellville Searcy Cabot Benton Bryant Paragould",
}


def candidates() -> list[str]:
    out: set[str] = set()
    for st, names in COUNTIES.items():
        for c in names.split():
            out |= {f"{c}Co{st}", f"{c}Co911{st}", f"{c}CoE911{st}",
                    f"{c}CoSO{st}", f"{c}CoSheriff{st}"}
    for st, names in CITIES.items():
        for c in names.split():
            out.add(f"{c}PD{st}")
    return sorted(out)


def probe(client: httpx.Client, aid: str) -> bool:
    """True if AgencyID is a real CFS tenant. Sets the session as a side effect."""
    try:
        idx = client.get(f"{_BASE}/CADCFS_Public/index.php",
                         params={"AgencyID": aid}).text
    except httpx.HTTPError:
        return False
    if "fetchesforajax/resttest.php" in idx:
        return True                       # map-enabled tenant — confirmed from the page
    # list-only tenant renders the directory fallback; confirm via a non-empty call list
    try:
        rest = client.get(f"{_BASE}/CADCFS_Public/fetchesforajax/resttest.php",
                          params={"t": int(time.time() * 1000)},
                          headers={"Referer": f"{_BASE}/CADCFS_Public/index.php?AgencyID={aid}",
                                   "X-Requested-With": "XMLHttpRequest"}).text
    except httpx.HTTPError:
        return False
    return "data-calltype=" in rest


def discover(delay: float = 0.2) -> list[str]:
    client = httpx.Client(timeout=15.0, follow_redirects=True,
                          headers={"User-Agent": _UA})
    cands = candidates()
    print(f"[ss] probing {len(cands)} candidate AgencyIDs")
    working: list[str] = []
    for i, aid in enumerate(cands):
        if probe(client, aid):
            working.append(aid)
            print(f"  ✓ {aid}")
        if i % 100 == 0:
            print(f"[ss] {i}/{len(cands)} probed, {len(working)} working")
        time.sleep(delay)
    return working


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--delay", type=float, default=0.2)
    ap.add_argument("--out", default="data/southern_agencies.json")
    args = ap.parse_args()

    working = discover(args.delay)
    out = Path(args.out)
    out.parent.mkdir(parents=True, exist_ok=True)
    existing = json.loads(out.read_text(encoding="utf-8")) if out.exists() else []
    merged = sorted(set(existing) | set(working))
    out.write_text(json.dumps(merged, indent=1), encoding="utf-8")
    print(f"\n[ss] +{len(set(working) - set(existing))} new, {len(merged)} total -> {out}")


if __name__ == "__main__":
    main()
