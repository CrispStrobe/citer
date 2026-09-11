# Tests for the fixes ported from upstream citer issues (#50, #68).
from datetime import date
from unittest.mock import patch

from curl_cffi import CurlError

import app as citer_app
from lib.generator_en import sfn_cit_ref as en_sfn_cit_ref
from lib.urls import url_data


def test_url_data_returns_cite_web_when_fetch_and_citoid_fail():
    """#50: a connection/HTTP error must still yield a {{cite web}} template."""
    with (
        patch("lib.urls.url_text", side_effect=CurlError("403")),
        patch("lib.urls.citoid_data", side_effect=Exception("citoid down")),
    ):
        d = url_data("https://example.com/x")
    assert d["cite_type"] == "web"
    assert d["url"] == "https://example.com/x"


def test_url_data_uses_citoid_when_available():
    with (
        patch("lib.urls.url_text", side_effect=CurlError("403")),
        patch("lib.urls.citoid_data", return_value={"title": "T", "cite_type": "web"}),
    ):
        d = url_data("https://example.com/x")
    assert d["title"] == "T"


def test_api_honors_date_format():
    """#68: the requested date_format must reach the generator."""
    rec = {
        "title": "A title",
        "date": date(2007, 3, 4),
        "cite_type": "web",
        "url": "https://example.com/",
    }
    with patch.dict(
        citer_app.input_type_to_resolver,
        {"__test_dt__": lambda s: dict(rec)},
        clear=False,
    ):
        client = citer_app.app.test_client()
        r = client.post(
            "/",
            json={
                "user_input": "z",
                "input_type": "__test_dt__",
                "template_format": "cite",
                "date_format": "%Y",
            },
        )
    assert r.status_code == 200
    assert "| date=2007" in r.get_data(as_text=True)


def test_api_defaults_date_format_to_iso():
    rec = {
        "title": "A title",
        "date": date(2007, 3, 4),
        "cite_type": "web",
        "url": "https://example.com/",
    }
    with patch.dict(
        citer_app.input_type_to_resolver,
        {"__test_iso__": lambda s: dict(rec)},
        clear=False,
    ):
        client = citer_app.app.test_client()
        r = client.post(
            "/",
            json={
                "user_input": "z",
                "input_type": "__test_iso__",
                "template_format": "cite",
            },
        )
    assert "| date=2007-03-04" in r.get_data(as_text=True)


def test_optional_ref_name_override():
    """#21: an explicit name replaces the generated <ref name> hash."""
    d = {"title": "T", "cite_type": "web", "url": "https://e.com/", "ref_name": "Smith2020"}
    _, _, ref = en_sfn_cit_ref(d)
    assert ref.startswith('<ref name="Smith2020">')


def test_ref_name_defaults_to_generated_hash():
    d = {"title": "T", "cite_type": "web", "url": "https://e.com/"}
    _, _, ref = en_sfn_cit_ref(d)
    assert ref.startswith('<ref name="')
    assert "Smith2020" not in ref


def test_api_applies_ref_name():
    rec = {"title": "A title", "cite_type": "web", "url": "https://example.com/"}
    with patch.dict(
        citer_app.input_type_to_resolver,
        {"__test_name__": lambda s: dict(rec)},
        clear=False,
    ):
        client = citer_app.app.test_client()
        r = client.post(
            "/",
            json={
                "user_input": "z",
                "input_type": "__test_name__",
                "template_format": "ref",
                "name": "MyRef",
            },
        )
    assert 'name="MyRef"' in r.get_json()
