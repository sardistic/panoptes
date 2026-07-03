"""GDELT query construction — offline. GDELT rejects nested OR groups outright
('keywords too short/common (orclauseid:N)'), so the query must always be a single
flat parenthesized OR list."""
import re

from apb.context.gdelt import _MAX_TERMS, _terms_for


def test_multi_type_terms_flatten_to_single_or_group():
    terms = _terms_for(["fire", "traffic"])
    assert "(" not in terms.replace('"', "")   # no nested groups inside the list
    assert "fire" in terms and "crash" in terms


def test_terms_dedupe_across_types():
    # shots_fired and assault both map "shooting"; it must appear once
    terms = _terms_for(["shots_fired", "assault"])
    assert len(re.findall(r"\bshooting\b", terms)) == 1


def test_unknown_types_fall_back_to_generic():
    assert "police" in _terms_for(["nonexistent_type"])
    assert "police" in _terms_for(None)


def test_term_count_is_bounded():
    all_types = ["shots_fired", "assault", "robbery", "fire", "pursuit",
                 "traffic", "medical", "domestic", "suspicious", "welfare"]
    assert len(_terms_for(all_types).split(" OR ")) <= _MAX_TERMS
