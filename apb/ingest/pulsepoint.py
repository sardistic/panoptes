"""PulsePoint ingest — live fire/EMS dispatch for ~5,000+ agencies nationwide.

No SDR, no Broadcastify. PulsePoint publishes incident data for public awareness; the
web endpoint returns AES-encrypted JSON (light obfuscation). We decrypt it with the
community-known scheme (MD5 EVP_BytesToKey key derivation, AES-256-CBC, IV supplied in
the response). Credit: Davnit's original algorithm; constants per the public
pulsepointinc/Podskio references.

Be respectful: cache aggressively and poll slowly (this module is wired into APB's
existing 60s cache + rotating poller).
"""
from __future__ import annotations

import base64
import hashlib
import json
import time
from pathlib import Path

import httpx

from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes

HASH_PASSWORD = b"tombrady5rings"
INCIDENTS_URL = "https://api.pulsepoint.org/v1/webapp?resource=incidents&agencyid="
AGENCIES_URL = "https://api.pulsepoint.org/v1/webapp?resource=agencies"

# Subset of the PulsePoint call-type codes -> readable text (classifier reads this).
CALL_TYPES = {
    "ME": "Medical Emergency", "TC": "Traffic Collision", "TCE": "Traffic Collision",
    "TCS": "Traffic Collision Structure", "TCT": "Traffic Collision Train",
    "RTE": "Railroad Emergency", "FIRE": "Fire", "SF": "Structure Fire",
    "RF": "Residential Fire", "CF": "Commercial Fire", "WSF": "Confirmed Structure Fire",
    "VEG": "Vegetation Fire", "WVEG": "Confirmed Vegetation Fire", "VF": "Vehicle Fire",
    "OF": "Outside Fire", "EF": "Extinguished Fire", "AF": "Appliance Fire",
    "GAS": "Gas Leak", "HMR": "Hazmat Response", "HC": "Hazardous Condition",
    "EX": "Explosion", "FA": "Fire Alarm", "OA": "Alarm", "CMA": "Carbon Monoxide",
    "MCI": "Multi Casualty", "LA": "Lift Assist", "PA": "Police Assist",
    "WR": "Water Rescue", "RES": "Rescue", "TR": "Technical Rescue", "AR": "Animal Rescue",
    "EE": "Electrical Emergency", "WD": "Wires Down", "TD": "Tree Down", "EM": "Emergency",
}


def decrypt(payload: dict) -> object:
    """Decrypt a {ct, iv, s} PulsePoint response to its JSON object."""
    ct = base64.b64decode(payload["ct"])
    iv = bytes.fromhex(payload["iv"])
    salt = bytes.fromhex(payload["s"])

    # EVP_BytesToKey (MD5, no iterations) -> 32-byte key
    key, prev = b"", b""
    while len(key) < 32:
        prev = hashlib.md5(prev + HASH_PASSWORD + salt).digest()
        key += prev
    key = key[:32]

    dec = Cipher(algorithms.AES(key), modes.CBC(iv)).decryptor()
    out = dec.update(ct) + dec.finalize()
    out = out[: -out[-1]]                      # strip PKCS7 padding
    text = out.decode("utf-8", "ignore").strip()
    # The plaintext is a JSON-encoded *string* (quoted + escaped). Let json.loads do
    # the unescaping (handles \", \/, \n, \uXXXX correctly), then parse the inner JSON.
    if text[:1] == '"':
        text = json.loads(text)
    return json.loads(text)


class PulsePoint:
    def __init__(self):
        self._client = httpx.Client(timeout=15.0, headers={"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36", "Referer": "https://web.pulsepoint.org/", "Origin": "https://web.pulsepoint.org"})

    def agencies(self, types=("fire", "ems", "law")) -> list[dict]:
        out, seen = [], set()
        for t in types:
            try:
                r = self._client.get(AGENCIES_URL + "&type=" + t)
                r.raise_for_status()
                data = decrypt(r.json())
            except Exception:
                continue
            rows = data.get("agencies", data) if isinstance(data, dict) else data
            for a in rows:
                aid = a.get("agencyid") or a.get("id")
                lat = a.get("latitude") or a.get("Latitude")
                lon = a.get("longitude") or a.get("Longitude")
                if not aid or aid in seen or not lat or not lon:
                    continue
                seen.add(aid)
                out.append({"id": aid,
                            "name": a.get("agencyname") or a.get("name") or aid,
                            "lat": float(lat), "lon": float(lon)})
        return out

    def incidents(self, agency_id: str) -> list[dict]:
        """Active + recent incidents for one agency, decrypted and flattened."""
        r = self._client.get(INCIDENTS_URL + agency_id)
        r.raise_for_status()
        data = decrypt(r.json())
        inc = data.get("incidents", {}) if isinstance(data, dict) else {}
        rows = (inc.get("active") or []) + (inc.get("recent") or [])
        out = []
        for i in rows:
            lat, lon = i.get("Latitude"), i.get("Longitude")
            if not lat or not lon:
                continue
            code = i.get("PulsePointIncidentCallType") or i.get("CallType") or ""
            out.append({
                "call_id": str(i.get("ID") or i.get("IncidentNumber") or len(out)),
                "type_raw": CALL_TYPES.get(code, code),
                "address": i.get("FullDisplayAddress") or i.get("MedicalEmergencyDisplayAddress"),
                "at": i.get("CallReceivedDateTime"),
                "lat": float(lat), "lon": float(lon),
            })
        return out
