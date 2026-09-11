# Tests for #70: bulk (multi-input) generation.
from unittest.mock import patch

import app as citer_app

RECORDS = {
    "a": {"title": "Alpha", "cite_type": "web", "url": "https://a.example/"},
    "b": {"title": "Beta", "cite_type": "web", "url": "https://b.example/"},
}


def _post(payload):
    with patch.dict(
        citer_app.input_type_to_resolver,
        {"__bulk__": lambda s: dict(RECORDS[s])},
        clear=False,
    ):
        return citer_app.app.test_client().post("/", json=payload)


def test_bulk_mode_with_explicit_inputs():
    r = _post(
        {
            "user_input": "a\nb",
            "input_type": "__bulk__",
            "template_format": "cite",
            "inputs": ["a", "b"],
        }
    )
    assert r.status_code == 200
    body = r.get_json()
    assert "Alpha" in body and "Beta" in body


def test_bulk_mode_triggered_by_newlines():
    r = _post(
        {
            "user_input": "a\nb",
            "input_type": "__bulk__",
            "template_format": "cite",
        }
    )
    assert r.status_code == 200
    body = r.get_json()
    assert "Alpha" in body and "Beta" in body


def test_bulk_mode_reports_per_item_errors():
    # 'missing' is not in RECORDS -> KeyError -> an HTML comment, not a 500.
    r = _post(
        {
            "user_input": "a\nmissing",
            "input_type": "__bulk__",
            "template_format": "cite",
        }
    )
    assert r.status_code == 200
    body = r.get_json()
    assert "Alpha" in body
    assert "error for missing" in body