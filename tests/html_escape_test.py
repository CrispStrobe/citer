"""Security regression: the custom-format citation output is injected into a
<div>, so it must be HTML-escaped like the normal path — otherwise markup in
remote metadata (e.g. a page title) is reflected XSS."""
from lib.html import scr_to_html


def _body(scr, template_format):
    out = scr_to_html(scr, '%Y-%m-%d', ' | ', 'url-doi-isbn', template_format)
    return ''.join(str(x) for x in out)


def test_custom_output_is_escaped():
    malicious = '<img src=x onerror=alert(1)>Title'
    body = _body(('', malicious, ''), 'custom')
    assert '<img src=x onerror' not in body
    assert '&lt;img' in body


def test_normal_output_is_escaped():
    body = _body(('<b>x</b>', '<b>y</b>', '<b>z</b>'), 'wikipedia')
    assert '<b>x</b>' not in body
    assert '&lt;b&gt;' in body
