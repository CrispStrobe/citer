# Tests for the names2para author-numbering change (upstream ea5ad3a/dfddae5/6094e7e).
from lib.generator_en import (
    clean_up_title,
    clean_up_website,
    names2para as en_names2para,
    sanitize_names,
    sfn_cit_ref as en_sfn_cit_ref,
)
from lib.generator_fa import names2para as fa_names2para


def test_clean_up_title_normalises_typographic_quotes():
    assert clean_up_title("The \u2018Best\u2019 \u201cTitle\u201d") == 'The \'Best\' "Title"'
    assert clean_up_title(None) is None
    assert clean_up_title("") == ""


def test_clean_up_website_capitalises_leading_the():
    assert clean_up_website("the guardian") == "The guardian"
    assert clean_up_website("The Guardian") == "The Guardian"
    assert clean_up_website("Nature") == "Nature"
    assert clean_up_website(None) is None


def test_sanitize_names_normalises_entries():
    assert sanitize_names([("Jane", "Doe")]) == [("Jane", "Doe")]
    assert sanitize_names([("Org",)]) == [("", "Org")]
    assert sanitize_names([None, ("A", "B")]) == [("A", "B")]
    assert not sanitize_names(None)
    assert not sanitize_names([])


def test_translator_suffix_is_english():
    d = {"cite_type": "book", "title": "T", "translators": [("A", "B")], "url": "https://e.com/"}
    _, cit, _ = en_sfn_cit_ref(d)
    assert "(translator)" in cit
    assert "مترجم" not in cit


def test_en_single_author_uses_last_first():
    assert en_names2para([("Jane", "Doe")], " | ", "first", "last") == " | last=Doe | first=Jane"


def test_en_multiple_authors_use_numbered():
    assert en_names2para(
        [("Jane", "Doe"), ("John", "Smith")], " | ", "first", "last"
    ) == " | last1=Doe | first1=Jane | last2=Smith | first2=John"


def test_en_nofn_single_vs_multi():
    assert en_names2para([("", "Org")], " | ", "first", "last", "author") == " | author=Org"
    assert en_names2para(
        [("", "A"), ("", "B")], " | ", "first", "last", "author"
    ) == " | author1=A | author2=B"


def test_fa_single_author_unnumbered():
    assert fa_names2para([("Jane", "Doe")], "first", "last") == " | last=Doe | first=Jane"


def test_fa_multiple_authors_numbered_with_fa_digits():
    out = fa_names2para([("A", "B"), ("C", "D")], "first", "last")
    assert out == " | last\u06f1=B | first\u06f1=A | last\u06f2=D | first\u06f2=C"
