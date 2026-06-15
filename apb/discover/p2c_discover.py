"""Discover PoliceToCitizen agencies by brute-forcing the predictable subdomain
naming patterns (city+state, city+pd, county+sheriff, ...) against the lightweight
InitialSettings endpoint, then confirming CAD calls actually return data.

CT logs don't help (wildcard cert), so this pattern-probe is how we scale past the
hand-harvested seed. Polite: small delay, validates cheaply (1 GET) before the fuller
incidents check, merges into data/p2c_agencies.json.

Usage: python -m apb.discover.p2c_discover
"""
from __future__ import annotations

import json
import re
import time
from pathlib import Path

import httpx

from apb.ingest.p2c import P2C

_UA = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
       "(KHTML, like Gecko) Chrome/141.0 Safari/537.36")

# (city, state-abbrev) — medium/large US cities; patterns multiply this set.
CITIES = """austin,tx dallas,tx houston,tx sanantonio,tx fortworth,tx elpaso,tx
arlington,tx plano,tx corpuschristi,tx laredo,tx lubbock,tx garland,tx irving,tx
amarillo,tx grandprairie,tx mckinney,tx frisco,tx waco,tx denton,tx midland,tx
abilene,tx beaumont,tx roundrock,tx wichitafalls,tx tyler,tx victoria,tx
miami,fl tampa,fl orlando,fl jacksonville,fl stpetersburg,fl hialeah,fl
tallahassee,fl fortlauderdale,fl portstlucie,fl capecoral,fl pembrokepines,fl
hollywood,fl gainesville,fl miramar,fl coralsprings,fl clearwater,fl palmbay,fl
westpalmbeach,fl pompanobeach,fl lakeland,fl davie,fl boca,fl sunrise,fl ocala,fl
losangeles,ca sandiego,ca sanjose,ca sanfrancisco,ca fresno,ca sacramento,ca
longbeach,ca oakland,ca bakersfield,ca anaheim,ca santaana,ca riverside,ca
stockton,ca chulavista,ca fremont,ca irvine,ca sanbernardino,ca modesto,ca
oxnard,ca fontana,ca morenovalley,ca huntingtonbeach,ca glendale,ca
columbus,oh cleveland,oh cincinnati,oh toledo,oh akron,oh dayton,oh
parma,oh canton,oh youngstown,oh lorain,oh hamilton,oh springfield,oh
charlotte,nc raleigh,nc greensboro,nc durham,nc winstonsalem,nc fayetteville,nc
cary,nc wilmington,nc highpoint,nc greenville,nc asheville,nc concord,nc gastonia,nc
chattanooga,tn knoxville,tn memphis,tn nashville,tn clarksville,tn murfreesboro,tn
columbia,sc charleston,sc northcharleston,sc rockhill,sc greenville,sc myrtlebeach,sc
richmond,va virginiabeach,va norfolk,va chesapeake,va newportnews,va alexandria,va
hampton,va portsmouth,va lynchburg,va roanoke,va suffolk,va
atlanta,ga columbus,ga augusta,ga savannah,ga athens,ga macon,ga roswell,ga albany,ga
phoenix,az tucson,az mesa,az chandler,az glendale,az scottsdale,az gilbert,az tempe,az
denver,co coloradosprings,co aurora,co fortcollins,co lakewood,co pueblo,co
newyork,ny buffalo,ny rochester,ny yonkers,ny syracuse,ny albany,ny
philadelphia,pa pittsburgh,pa allentown,pa erie,pa reading,pa scranton,pa
chicago,il aurora,il rockford,il joliet,il naperville,il springfield,il peoria,il
detroit,mi grandrapids,mi warren,mi annarbor,mi lansing,mi flint,mi dearborn,mi
seattle,wa spokane,wa tacoma,wa vancouver,wa bellevue,wa kent,wa everett,wa
portland,or salem,or eugene,or gresham,or hillsboro,or beaverton,or bend,or
minneapolis,mn stpaul,mn duluth,mn bloomington,mn rochester,mn
kansascity,mo stlouis,mo springfield,mo columbia,mo independence,mo
indianapolis,in fortwayne,in evansville,in southbend,in carmel,in fishers,in
milwaukee,wi madison,wi greenbay,wi kenosha,wi racine,wi appleton,wi
baltimore,md frederick,md rockville,md gaithersburg,md bowie,md
newark,nj jerseycity,nj paterson,nj elizabeth,nj trenton,nj camden,nj
boston,ma worcester,ma springfield,ma lowell,ma cambridge,ma brockton,ma
neworleans,la batonrouge,la shreveport,la lafayette,la lakecharles,la
birmingham,al montgomery,al mobile,al huntsville,al tuscaloosa,al
louisville,ky lexington,ky bowlinggreen,ky owensboro,ky
oklahomacity,ok tulsa,ok norman,ok brokenarrow,ok lawton,ok edmond,ok
jackson,ms gulfport,ms southaven,ms hattiesburg,ms biloxi,ms
littlerock,ar fortsmith,ar fayetteville,ar springdale,ar jonesboro,ar
wichita,ks overlandpark,ks topeka,ks olathe,ks
omaha,ne lincoln,ne bellevue,ne grandisland,ne
desmoines,ia cedarrapids,ia davenport,ia siouxcity,ia waterloo,ia
lasvegas,nv henderson,nv reno,nv northlasvegas,nv sparks,nv
albuquerque,nm lascruces,nm riorancho,nm santafe,nm
saltlakecity,ut provo,ut ogden,ut westjordan,ut
boise,id meridian,id nampa,id idahofalls,id pocatello,id
bridgeport,ct newhaven,ct hartford,ct stamford,ct waterbury,ct
"""

# (county, state) for sheriff patterns
COUNTIES = """wood,oh franklin,oh hamilton,oh montgomery,oh summit,oh lucas,oh
guilford,nc buncombe,nc robeson,nc forsyth,nc wake,nc mecklenburg,nc
marathon,wi dane,wi waukesha,wi brown,wi rock,wi
harris,tx dallas,tx tarrant,tx bexar,tx travis,tx collin,tx denton,tx
miamidade,fl broward,fl palmbeach,fl hillsborough,fl orange,fl duval,fl pinellas,fl
losangeles,ca sandiego,ca orange,ca riverside,ca sanbernardino,ca sacramento,ca
maricopa,az pima,az pinal,az fulton,ga gwinnett,ga cobb,ga dekalb,ga
king,wa pierce,wa snohomish,wa spokane,wa clark,wa multnomah,or washington,or
cook,il dupage,il lake,il will,il kane,il oakland,mi macomb,mi kent,mi
jefferson,ky fayette,ky shelby,tn knox,tn davidson,tn hamilton,tn
charleston,sc greenville,sc richland,sc horry,sc spartanburg,sc
jefferson,al mobile,al madison,al baldwin,al shelby,al
hinds,ms harrison,ms desoto,ms orleans,la eastbatonrouge,la jefferson,la
pulaski,ar benton,ar washington,ar sedgwick,ks johnson,ks
douglas,ne lancaster,ne polk,ia clark,nv washoe,nv
bernalillo,nm saltlake,ut utah,ut ada,id canyon,id
"""


def candidates() -> list[str]:
    out = set()
    for tok in CITIES.split():
        city, st = tok.split(",")
        out |= {f"{city}{st}", city, f"{city}pd", f"{city}police",
                f"{city}{st}pd", f"{city}{st}police", f"{city}pd{st}"}
    for tok in COUNTIES.split():
        county, st = tok.split(",")
        out |= {f"{county}countysheriff", f"{county}county{st}sheriff",
                f"{county}so", f"{county}sheriff", f"{county}countyso",
                f"{county}{st}sheriff"}
    return sorted(out)


def exists(client: httpx.Client, sub: str) -> int | None:
    """Cheap check: does this tenant exist? Returns AgencyId or None."""
    try:
        r = client.get(f"https://{sub}.policetocitizen.com/api/Agency/InitialSettings")
        if r.status_code == 200 and r.headers.get("content-type", "").startswith("application/json"):
            aid = r.json().get("AgencyId")
            return aid if aid and int(aid) > 0 else None
    except Exception:
        return None
    return None


def discover(delay: float = 0.15) -> list[str]:
    client = httpx.Client(timeout=12.0, headers={"User-Agent": _UA,
                          "Accept": "application/json, text/plain, */*"},
                          follow_redirects=True)
    pp = P2C()
    cands = candidates()
    print(f"[p2c] probing {len(cands)} candidate subdomains")
    working: list[str] = []
    for i, sub in enumerate(cands):
        aid = exists(client, sub)
        if aid:
            try:
                n = len(pp.incidents(sub))
            except Exception:
                n = 0
            if n > 0:
                working.append(sub)
                print(f"  ✓ {sub:26} id={aid} calls={n}")
        if i % 100 == 0:
            print(f"[p2c] {i}/{len(cands)} probed, {len(working)} working")
        time.sleep(delay)
    return working


def main():
    working = discover()
    out = Path("data/p2c_agencies.json")
    existing = json.loads(out.read_text(encoding="utf-8")) if out.exists() else []
    merged = sorted(set(existing) | set(working))
    out.write_text(json.dumps(merged, indent=1), encoding="utf-8")
    print(f"\n[p2c] +{len(set(working) - set(existing))} new, {len(merged)} total -> {out}")


if __name__ == "__main__":
    main()
