"""Cross-repo SRU parser parity guard (CrispZotLib PLAN 7.4 / audit #2a).

fixtures/parity/parser-records.json is synced byte-for-byte from CrispZotLib
(canonical). CrispLib asserts the SAME golden in test_parser_parity.py. Each
case is raw MARCXML/Dublin-Core plus the agreed parsed-field output.

A failure here means citer's SRU parsers (lib/sru_client.py) drifted from the
agreed output. This guard was added after the parser source was found to have
diverged from CrispLib undetected — the 'Verfasser' author relator was misfiled
as a contributor here (the sync-check never compared parser source, only
endpoints.json + the formatter goldens). Runs fully offline.
"""
import json
import xml.etree.ElementTree as ET
from pathlib import Path

import pytest

from lib.sru_client import parse_marcxml, parse_dublin_core, SRUClient

CASES = json.loads(
    (Path(__file__).parent.parent / "fixtures" / "parity" / "parser-records.json").read_text(
        encoding="utf-8"
    )
)
NS = SRUClient(base_url="x").namespaces


def _parse(case):
    raw = {"data": ET.fromstring(case["xml"]), "id": case["name"], "schema": case["schema"]}
    if case["schema"] == "marcxml":
        return parse_marcxml(raw, NS)
    return parse_dublin_core(raw, NS)


@pytest.mark.parametrize("case", CASES, ids=[c["name"] for c in CASES])
def test_parser_matches_golden(case):
    rec = _parse(case)
    for field, expected in case["expected"].items():
        assert getattr(rec, field) == expected, f"{case['name']}.{field}"
