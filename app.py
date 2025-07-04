# -*- coding: utf-8 -*-
"""
Citer Web Application - Main Entry Point
"""

import logging
from collections.abc import Callable
from functools import partial
from html import unescape
from json import JSONDecodeError, dumps, loads
from urllib.parse import parse_qs, unquote, urlparse

from curl_cffi import CurlError

from lib import logger
from lib.archives import archive_org_data, archive_today_data
from lib.commons import (
    ReturnError,
    data_to_sfn_cit_ref,
    isbn_10or13_search,
    uninum2en,
)
from lib.doi import doi_data, doi_search
from lib.googlebooks import google_books_data
from lib.html import (
    ALLOW_ALL_ORIGINS,
    CSS,
    CSS_HEADERS,
    CSS_PATH,
    DEFAULT_SCR,
    JS,
    JS_HEADERS,
    JS_PATH,
    scr_to_html,
)
from lib.isbn_oclc import isbn_data, oclc_data, worldcat_data
from lib.jstor import jstor_data
from lib.ketabir import ketabir_data
from lib.noorlib import noorlib_data
from lib.noormags import noormags_data
from lib.pubmed import pmcid_data, pmid_data
from lib.sru_client import SRUClient
from lib.ixtheo_client import IxTheoSearchHandler
from lib.urls import MAX_RESPONSE_LENGTH, url_data, url_text
from lib.custom_format import custom_format

def biblio_record_to_dict(record) -> dict:
    """
    Converts a BiblioRecord object into a standardized dictionary.
    """
    logger.debug(f"[biblio_record_to_dict] Normalizing record ID: {record.id}")
    doc_type = (record.format or record.document_type or 'book').lower()
    cite_type = 'book'
    if 'journal article' in doc_type or doc_type == 'jour':
        cite_type = 'article-journal'
    elif 'book chapter' in doc_type or doc_type == 'chap':
        cite_type = 'chapter'
    elif 'book' in doc_type or 'thesis' in doc_type or doc_type == 'thes':
        cite_type = 'book'
    logger.debug(f"[biblio_record_to_dict] Determined cite_type: '{cite_type}' from doc_type: '{doc_type}'")

    title_raw = record.title or ""
    title, subtitle = "", ""
    if ':' in title_raw:
        parts = title_raw.split(':', 1)
        title = parts[0].strip()
        subtitle = parts[1].strip() if len(parts) > 1 else ""
    else:
        title = title_raw
    logger.debug(f"[biblio_record_to_dict] Parsed title: '{title}', subtitle: '{subtitle}'")

    def format_name(name_str):
        if not name_str: return "", ""
        if ',' in name_str:
            parts = name_str.split(',', 1)
            return parts[1].strip(), parts[0].strip()
        name_parts = name_str.split()
        return (" ".join(name_parts[:-1]), name_parts[-1]) if len(name_parts) > 1 else ("", name_str)

    result_dict = {
        'title': title, 'subtitle': subtitle,
        'authors': [format_name(author) for author in (record.authors or [])],
        'editors': [format_name(editor) for editor in (record.editors or [])],
        'translators': [format_name(translator) for translator in (record.translators or [])],
        'year': record.year, 'publisher': record.publisher_name,
        'address': record.place_of_publication, 'isbn': record.isbn,
        'issn': record.issn, 'journal': record.journal_title,
        'volume': record.volume, 'issue': record.issue,
        'page': record.pages, 'doi': record.doi,
        'cite_type': cite_type, 'series': record.series,
        'edition': record.edition,
    }
    logger.debug(f"[biblio_record_to_dict] Normalized record dict: {result_dict}")
    return result_dict

def sru_search(user_input: str) -> list:
    logger.debug(f"[sru_search] Initiating SRU search for query: '{user_input}'")
    client = SRUClient(base_url='https://services.dnb.de/sru/dnb', default_schema='RDFxml')
    _, records = client.search(query=user_input, max_records=20)
    logger.debug(f"[sru_search] Found {len(records)} records.")
    return [biblio_record_to_dict(rec) for rec in records] if records else []

def ixtheo_search(user_input: str) -> list:
    logger.debug(f"[ixtheo_search] Initiating IxTheo search for query: '{user_input}'")
    handler = IxTheoSearchHandler()
    _, records = handler.search(query=user_input, max_results=20)
    if not records:
        logger.debug("[ixtheo_search] No initial records found.")
        return []
    
    logger.debug(f"[ixtheo_search] Found {len(records)} initial records. Enhancing with details...")
    enhanced_records = [handler.get_record_with_ris(rec) for rec in records]
    dict_records = [biblio_record_to_dict(rec) for rec in enhanced_records if rec]
    
    dict_records.sort(key=lambda r: (0 if r.get('cite_type') == 'book' else 1 if r.get('cite_type') == 'chapter' else 2))
    logger.debug(f"[ixtheo_search] Returning {len(dict_records)} sorted and normalized records.")
    return dict_records

def google_encrypted_data(url: str, parsed_url) -> dict:
    return google_books_data(parsed_url) if parsed_url.path.startswith('/books') else url_data(url)

def url_doi_isbn_data(user_input: str, /) -> dict:
    en_user_input = unquote(uninum2en(user_input))
    if '.' in en_user_input:
        url = 'http://' + user_input if not user_input.startswith('http') else user_input
        parsed_url = urlparse(url)
        hostname = parsed_url.hostname or ''
        hostname_core = hostname.rpartition('.')[0].removeprefix('www.')
        if (data_func := get_resolver.get(hostname_core)):
            return data_func(url) if data_func is not google_books_data else data_func(parsed_url)
        if (m := doi_search(unescape(en_user_input))):
            try: return doi_data(m[0], True)
            except (JSONDecodeError, CurlError):
                if not user_input.startswith('http'): raise
        return url_data(url)
    if (m := isbn_10or13_search(en_user_input)): return isbn_data(m[0], True)
    raise ValueError('invalid user_input')

def html_data(user_input: dict) -> dict: return url_data(user_input['url'], html=user_input['html'])
def echo(url: str, _: str, /):
    try: url, text = url_text(url)
    except Exception as e: url, text = type(e).__name__, ''
    raise ReturnError(url, '', text)

get_resolver = {
    'archive': archive_today_data, 'books.google': google_books_data, 'books.google.co': google_books_data,
    'books.google.com': google_books_data, 'encrypted.google': google_encrypted_data, 'google': google_encrypted_data,
    'jstor': jstor_data, 'ketab': ketabir_data, 'noorlib': noorlib_data, 'noormags': noormags_data,
    'web-beta.archive': archive_org_data, 'web.archive': archive_org_data, 'worldcat': worldcat_data,
}
input_type_to_resolver = {
    '': url_doi_isbn_data, 'url-doi-isbn': url_doi_isbn_data, 'pmid': pmid_data, 'pmcid': pmcid_data,
    'oclc': oclc_data, 'echo': echo, 'html': html_data, 'sru': sru_search, 'ixtheo': ixtheo_search,
}

http_headers = (('Content-Type', 'text/html; charset=UTF-8'), ALLOW_ALL_ORIGINS)
json_headers = (('Content-Type', 'application/json'), ALLOW_ALL_ORIGINS)
BytesTuple = tuple[bytes]
StartResponse = Callable[[str, list[tuple[str, str]]], Callable]

def css(start_response: StartResponse, _) -> BytesTuple:
    start_response('200 OK', [*CSS_HEADERS]); return (CSS,)
def js(start_response: StartResponse, _) -> BytesTuple:
    start_response('200 OK', [*JS_HEADERS]); return (JS,)
def page_does_not_exist(start_response: StartResponse, _) -> BytesTuple:
    start_response('404 Not Found', [('Content-Type', 'text/plain')]); return (b'404 Not Found',)

def read_body(environ: dict) -> bytes:
    length = int(environ.get('CONTENT_LENGTH') or 0)
    if length > MAX_RESPONSE_LENGTH: return b''
    return environ['wsgi.input'].read(length)

def parse_params(environ: dict) -> tuple:
    body = read_body(environ)
    if body:
        get = loads(body).get
        return (get('dateformat', '%Y-%m-%d'), get('pipeformat', ' | '), get('input_type', ''),
                get('template_format', 'custom'), get('user_input', ''), [*json_headers], dumps)
    
    query_get = parse_qs(environ['QUERY_STRING']).get
    date_format = query_get('dateformat', ('%Y-%m-%d',))[0].strip()
    pipe_format = query_get('pipeformat', [' | '])[0].replace('+', ' ')
    input_type = query_get('input_type', ('',))[0]
    template_format = query_get('template_format', ('custom',))[0]
    
    return (date_format, pipe_format, input_type, template_format,
            query_get('user_input', ('',))[0].strip(), [*http_headers],
            partial(scr_to_html, date_format=date_format, pipe_format=pipe_format,
                    input_type=input_type, template_format=template_format))

def root(start_response: StartResponse, environ: dict) -> BytesTuple:
    (date_format, pipe_format, input_type, template_format,
     user_input, headers, scr_to_resp_body) = parse_params(environ)

    headers.extend([
        ('Cache-Control', 'no-cache, no-store, must-revalidate'),
        ('Pragma', 'no-cache'),
        ('Expires', '0')
    ])

    if not user_input:
        response_body = scr_to_resp_body(DEFAULT_SCR).encode()
        start_response('200 OK', headers)
        return (response_body,)

    logger.debug(f"[root] Handling request for input: '{user_input}', type: '{input_type}', format: '{template_format}'")
    data_func = input_type_to_resolver[input_type]
    try:
        d = data_func(user_input)
    except Exception as e:
        status, scr = '500 Internal Server Error', (type(e).__name__, '', '')
        if isinstance(e, ReturnError): scr = e.args
        else: logger.exception(user_input)
    else:
        try:
            if template_format == 'custom':
                if isinstance(d, list):
                    if not d: scr = ('No results found.', '', '')
                    else:
                        citations = [custom_format(item) for item in d]
                        html_output = '<p>• ' + '<br>• '.join(citations) + '</p>'
                        scr = ('', html_output, '')
                else:
                    scr = ('', f'<p>{custom_format(d)}</p>', '')
            else:
                if isinstance(d, list):
                    if not d: scr = ('No results found.', '', '')
                    else:
                        citations = [data_to_sfn_cit_ref(item, date_format, pipe_format, template_format)[1] for item in d]
                        scr = ('', '• ' + '\n• '.join(citations), '')
                else:
                    scr = data_to_sfn_cit_ref(d, date_format, pipe_format, template_format)
            status = '200 OK'
        except Exception as e:
            logger.exception('Error in formatting block')
            scr = (type(e).__name__, '', '')
            status = '500 Internal Server Error'
    
    response_body = scr_to_resp_body(scr).encode()
    start_response(status, headers)
    return (response_body,)

get_handler: Callable[[str], Callable] = {
    f'/{CSS_PATH}.css': css, f'/{JS_PATH}.js': js,
    '/': root, '/citer.fcgi': root,
}.get

def app(environ: dict, start_response: StartResponse) -> BytesTuple:
    try:
        handler = get_handler(environ.get('PATH_INFO', '/')) or page_does_not_exist
        return handler(start_response, environ)
    except Exception:
        start_response('500 Internal Server Error', [])
        logger.exception('app error, environ:\n%s', environ)
        return (b'Unknown Error',)

if __name__ == '__main__':
    from wsgiref.simple_server import make_server
    httpd = make_server('localhost', 5000, app)
    print('serving on http://localhost:5000')
    try: httpd.serve_forever()
    except KeyboardInterrupt: pass