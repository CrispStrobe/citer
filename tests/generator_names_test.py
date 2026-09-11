# Tests for the names2para author-numbering change (upstream ea5ad3a/dfddae5/6094e7e).
from lib.generator_en import names2para as en_names2para
from lib.generator_fa import names2para as fa_names2para


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
