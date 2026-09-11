# Tests for four_digit_num returning the matched string (upstream 2e8f507).
from lib import four_digit_num


def test_returns_first_four_digit_run_as_string():
    assert four_digit_num("Nov.-Dec./1999") == "1999"
    assert four_digit_num("2020-05-01") == "2020"
    assert four_digit_num("published 123456") == "1234"


def test_returns_none_without_four_digits():
    assert four_digit_num("nope") is None
    assert four_digit_num("") is None
