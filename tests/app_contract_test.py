"""Contract tests for OUR restructured app (dispatch + SRU/IxTheo record mapping
+ the /cite route). Upstream's test_app.py targets upstream's route surface
(root, etc.) which our fork replaced, so these protect our own contract.

Fully offline: no resolver is actually invoked (we assert the dispatch table and
the pure record-to-dict conversion, and mock the resolver for the route test)."""
import app as citer_app
from lib.sru_client import BiblioRecord


def test_dispatch_table_has_our_input_types():
    keys = set(citer_app.input_type_to_resolver)
    # Upstream identifier types plus our SRU/IxTheo additions.
    assert {'', 'url-doi-isbn', 'pmid', 'pmcid', 'oclc', 'sru', 'ixtheo'} <= keys
    assert citer_app.input_type_to_resolver['sru'] is citer_app.sru_search
    assert citer_app.input_type_to_resolver['ixtheo'] is citer_app.ixtheo_search
    for fn in citer_app.input_type_to_resolver.values():
        assert callable(fn)


def test_biblio_record_to_dict_journal_article():
    rec = BiblioRecord(
        id='1', title='Quia ignoro: adoro', authors=['Ströbele, Christian'],
        year='2024', journal_title='Heiliger Dienst', volume='78', issue='3',
        pages='205-213', issn='0017-9620', document_type='Journal Article',
    )
    d = citer_app.biblio_record_to_dict(rec)
    assert d['cite_type'] == 'article-journal'          # typing carried into app format
    assert d['title'] == 'Quia ignoro' and d['subtitle'] == 'adoro'  # split on ':'
    assert d['authors'] == [('Christian', 'Ströbele')]  # "Last, First" -> (first, last)
    assert d['journal'] == 'Heiliger Dienst' and d['volume'] == '78'
    assert d['issn'] == '0017-9620'


def test_biblio_record_to_dict_book_and_chapter_types():
    book = BiblioRecord(id='1', title='Systematische Theologie', document_type='Book', isbn='9783161500000')
    assert citer_app.biblio_record_to_dict(book)['cite_type'] == 'book'
    chap = BiblioRecord(id='2', title='Ein Aufsatz', document_type='Book Chapter', series='Sammelband')
    assert citer_app.biblio_record_to_dict(chap)['cite_type'] == 'chapter'


def test_cite_route_rejects_empty_input():
    client = citer_app.app.test_client()
    resp = client.post('/', json={'user_input': '', 'input_type': ''})
    assert resp.status_code == 400


def test_cite_route_dispatches_by_input_type(monkeypatch):
    # Mock the SRU resolver so no network is touched; assert it is dispatched to.
    # Clear the persistent diskcache first, or a cached result would short-circuit
    # dispatch and the mock would never be called.
    citer_app.rawDataCache.clear()
    called = {}
    def fake(user_input):
        called['q'] = user_input
        return [{'title': 'X', 'cite_type': 'book'}]
    monkeypatch.setitem(citer_app.input_type_to_resolver, 'sru', fake)
    client = citer_app.app.test_client()
    resp = client.post('/', json={'user_input': 'Kant', 'input_type': 'sru'})
    assert resp.status_code == 200
    assert called.get('q') == 'Kant'
