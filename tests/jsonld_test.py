# Tests for schema.org JSON-LD scraping (upstream #23).
from lib.jsonld import find_json_ld
from lib.urls import url_data

LD_HTML = """
<html><head>
<script type="application/ld+json">
{
  "@context": "https://schema.org",
  "@type": "ScholarlyArticle",
  "headline": "A Study of Things",
  "author": [
    {"@type": "Person", "givenName": "Jane", "familyName": "Doe"},
    {"@type": "Person", "name": "John Smith"}
  ],
  "datePublished": "2020-05-01",
  "publisher": {"@type": "Organization", "name": "ACME Press"},
  "isPartOf": {"@type": "Periodical", "name": "Journal of Things"},
  "pageStart": "11",
  "pageEnd": "29",
  "issn": "1234-5678",
  "sameAs": "https://doi.org/10.1234/thing.2020"
}
</script>
</head><body><p>article body</p></body></html>
"""


def test_find_json_ld_extracts_fields():
    d = find_json_ld(LD_HTML)
    assert d["title"] == "A Study of Things"
    assert d["authors"] == [("Jane", "Doe"), ("John", "Smith")]
    assert d["date"] == "2020-05-01"
    assert d["publisher"] == "ACME Press"
    assert d["journal"] == "Journal of Things"
    assert d["page"] == "11–29"
    assert d["issn"] == "1234-5678"
    assert d["doi"] == "10.1234/thing.2020"


def test_find_json_ld_handles_graph_and_bad_json():
    html = (
        '<script type="application/ld+json">{ not json }</script>'
        '<script type="application/ld+json">'
        '{"@graph":[{"@type":"WebPage","name":"From Graph"}]}</script>'
    )
    d = find_json_ld(html)
    assert d["title"] == "From Graph"


def test_find_json_ld_ignores_non_article_types():
    html = (
        '<script type="application/ld+json">'
        '{"@type":"BreadcrumbList","name":"Home > Here"}</script>'
    )
    assert find_json_ld(html) == {}


def test_url_data_uses_json_ld_fallback_for_journal():
    ld_only = """
    <html><head>
    <script type="application/ld+json">
    {"@type":"Article","name":"X","isPartOf":{"name":"Zeitschrift X"},
     "image":"https://x/y.png"}
    </script></head><body></body></html>
    """
    d = url_data("https://example.com/a", check_home=False, html=ld_only)
    assert d["journal"] == "Zeitschrift X"
