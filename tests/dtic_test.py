# Tests for #29: DTIC technical reports → {{cite techreport}}.
from lib.generator_en import sfn_cit_ref
from lib.urls import url_data

DTIC_HTML = """<html><head>
<title>Some Technical Report - DTIC</title>
<meta name="citation_technical_report_institution" content="Institute for Defense Analyses">
<meta name="citation_id" content="ADA115972">
<meta name="citation_title" content="Some Technical Report">
<meta name="citation_publication_date" content="1982-01-01">
</head><body></body></html>"""


def test_dtic_url_maps_to_techreport_with_institution_and_id():
    d = url_data(
        "https://apps.dtic.mil/sti/citations/ADA115972",
        check_home=False,
        html=DTIC_HTML,
    )
    assert d["cite_type"] == "techreport"
    assert d["publisher"] == "Institute for Defense Analyses"
    assert d["id"] == "ADA115972"


def test_generator_emits_techreport_with_id():
    d = {
        "cite_type": "techreport",
        "title": "Some Technical Report",
        "publisher": "Institute for Defense Analyses",
        "id": "ADA115972",
        "url": "https://apps.dtic.mil/sti/citations/ADA115972",
    }
    _, cit, _ = sfn_cit_ref(d)
    assert cit.startswith("* {{cite techreport ")
    assert "publisher=Institute for Defense Analyses" in cit
    assert "id=ADA115972" in cit


def test_non_dtic_url_not_techreport():
    d = url_data(
        "https://example.com/sti/citations/ADA115972",
        check_home=False,
        html="<html><head><title>X</title></head></html>",
    )
    assert d["cite_type"] != "techreport"
