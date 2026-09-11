# Tests for #55: IETF datatracker URLs → {{cite IETF}}.
from lib.generator_en import sfn_cit_ref
from lib.urls import url_data

IETF_HTML = (
    "<html><head><title>RFC 114: A File Transfer Protocol</title>"
    '<meta name="citation_date" content="1971-04-16">'
    "</head><body></body></html>"
)


def test_ietf_url_is_detected_and_rfc_extracted():
    d = url_data(
        "https://datatracker.ietf.org/doc/html/rfc114",
        check_home=False,
        html=IETF_HTML,
    )
    assert d["cite_type"] == "ietf"
    assert d["rfc"] == "114"


def test_non_ietf_url_is_not_ietf():
    d = url_data(
        "https://example.com/doc/html/rfc114",
        check_home=False,
        html="<html><head><title>X</title></head></html>",
    )
    assert d["cite_type"] != "ietf"


def test_generator_emits_cite_ietf_template():
    d = {
        "cite_type": "ietf",
        "rfc": "114",
        "title": "RFC 114: A File Transfer Protocol",
        "url": "https://datatracker.ietf.org/doc/html/rfc114",
    }
    _, cit, ref = sfn_cit_ref(d)
    assert cit.startswith("* {{cite IETF ")
    assert "rfc=114" in cit
    assert "website=" not in cit
    assert "publisher=" not in cit
    assert 'name="' in ref
