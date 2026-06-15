"""Offline type-string learner: map agency-specific CAD incident-type strings to the
APB taxonomy + a threat score using Claude, and cache to data/type_map.json.

Runtime classification (apb.ingest.cad.classify) reads that cache, so we pay the LLM
cost ONCE per distinct string, never per request. Re-run periodically as new feeds /
strings appear.

Usage:
  python -m apb.infer.learn_types            # sample all feeds, map unknowns, save
  python -m apb.infer.learn_types --limit 30 # fewer rows sampled per feed
"""
from __future__ import annotations

import argparse
import json
from pathlib import Path

from apb.common.config import settings
from apb.ingest import cad

TAXONOMY = ["traffic", "medical", "fire", "assault", "robbery", "shots_fired",
            "domestic", "suspicious", "pursuit", "welfare", "other", "noise"]

PROMPT = """You map US public-safety dispatch incident-type strings to a fixed taxonomy.
For each input string return an object {{"string": <input>, "type": <one of {types}>,
"threat": <0.0-1.0>}} where threat is danger-to-life/urgency (routine call ~0.3,
active violence ~0.95). Return ONLY a JSON array, no prose."""


def collect_unknowns(limit_per: int = 50) -> set[str]:
    """Fetch a sample from every feed and gather type strings the keyword rules miss."""
    ing = cad.CadIngest()
    cad._UNKNOWN.clear()
    for metro in list(cad.FEEDS):
        try:
            ing.fetch(metro, limit_per)        # populates cad._UNKNOWN via classify()
        except Exception as e:
            print(f"[learn] {metro} failed: {e}")
    return set(cad._UNKNOWN)


def _provider():
    """Pick the LLM provider from whichever key is set (OpenAI takes precedence if both)."""
    if settings.openai_api_key:
        return "openai"
    if settings.anthropic_api_key:
        return "anthropic"
    raise SystemExit("Set OPENAI_API_KEY or ANTHROPIC_API_KEY to run the learner.")


def _complete(provider: str, system: str, user: str) -> str:
    """One chat completion returning raw text, for either provider."""
    if provider == "openai":
        from openai import OpenAI
        client = OpenAI(api_key=settings.openai_api_key)
        r = client.chat.completions.create(
            model=settings.apb_model_openai, temperature=0,
            messages=[{"role": "system", "content": system},
                      {"role": "user", "content": user}],
        )
        return r.choices[0].message.content or ""
    from anthropic import Anthropic
    client = Anthropic(api_key=settings.anthropic_api_key)
    msg = client.messages.create(
        model=settings.apb_model_light, max_tokens=4000, system=system,
        messages=[{"role": "user", "content": user}],
    )
    return "".join(b.text for b in msg.content if b.type == "text")


def map_strings(strings: list[str], batch: int = 60) -> dict[str, list]:
    provider = _provider()
    system = PROMPT.format(types=", ".join(TAXONOMY))
    print(f"[learn] using provider: {provider}")
    out: dict[str, list] = {}
    for i in range(0, len(strings), batch):
        chunk = strings[i:i + batch]
        text = _complete(provider, system, json.dumps(chunk)).strip()
        if text.startswith("```"):
            text = text.strip("`").split("\n", 1)[-1].rsplit("```", 1)[0]
        try:
            for item in json.loads(text):
                s = str(item["string"]).strip().lower()
                t = item["type"] if item["type"] in TAXONOMY else "other"
                out[s] = [t, float(item["threat"])]
        except (ValueError, KeyError) as e:
            print(f"[learn] batch {i} parse error: {e}")
        print(f"[learn] mapped {min(i + batch, len(strings))}/{len(strings)}")
    return out


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--limit", type=int, default=50)
    ap.add_argument("--out", default="data/type_map.json")
    args = ap.parse_args()

    if not (settings.openai_api_key or settings.anthropic_api_key):
        raise SystemExit("Set OPENAI_API_KEY or ANTHROPIC_API_KEY to run the learner.")

    unknowns = sorted(collect_unknowns(args.limit))
    print(f"[learn] {len(unknowns)} distinct unknown type strings")
    if not unknowns:
        return

    out = Path(args.out)
    existing = {}
    if out.exists():
        existing = json.loads(out.read_text(encoding="utf-8"))
    mapping = map_strings(unknowns)
    existing.update({k: v for k, v in mapping.items()})
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(existing, indent=2), encoding="utf-8")
    print(f"[learn] saved {len(mapping)} new mappings ({len(existing)} total) -> {out}")


if __name__ == "__main__":
    main()
