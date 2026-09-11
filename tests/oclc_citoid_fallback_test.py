# Test for the citoid fallback in oclc_data (upstream 9547023).
from unittest.mock import patch

from curl_cffi import CurlError

from lib.isbn_oclc import oclc_data


def test_oclc_falls_back_to_citoid_on_curl_error():
    with (
        patch("lib.isbn_oclc.request", side_effect=CurlError("blocked")),
        patch(
            "lib.isbn_oclc.citoid_data", return_value={"title": "T", "cite_type": "book"}
        ) as citoid,
    ):
        d = oclc_data("ocm62134798")
    # normalize_oclc applied, and the oclc id is re-attached to the citoid data.
    assert d["oclc"] == "62134798"
    assert d["title"] == "T"
    citoid.assert_called_once()
