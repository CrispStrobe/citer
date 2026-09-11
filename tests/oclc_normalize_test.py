# Regression test for upstream citer issue #61: OCLC numbers with leading junk.
from lib.isbn_oclc import normalize_oclc


def test_strips_leading_letters():
    assert normalize_oclc('ocm62134798') == '62134798'
    assert normalize_oclc('on1021182894') == '1021182894'
    assert normalize_oclc('ocn12345') == '12345'


def test_leaves_clean_numbers_alone():
    assert normalize_oclc('62134798') == '62134798'
    assert normalize_oclc(' 137313052 ') == '137313052'


def test_non_numeric_input_is_returned_unchanged():
    # Nothing digit-like to strip to; don't lose the value.
    assert normalize_oclc('abc') == 'abc'