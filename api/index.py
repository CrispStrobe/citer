# api/index.py
import os
from flask import Flask, request, jsonify, send_from_directory
from urllib.parse import unquote, urlparse
from html import unescape
from json import JSONDecodeError
from cachetools import LRUCache

# **FIXED**: Changed all imports from the 'lib' package to be relative
from .lib import logger
from .lib.commons import (
    ReturnError, data_to_sfn_cit_ref, isbn_10or13_search, uninum2en
)
from .lib.doi import doi_data, doi_search
from .lib.googlebooks import google_books_data
from .lib.isbn_oclc import isbn_data, oclc_data, worldcat_data
from .lib.jstor import jstor_data
from .lib.ketabir import ketabir_data
from .lib.noorlib import noorlib_data
from .lib.noormags import noormags_data
from .lib.pubmed import pmcid_data, pmid_data
from .lib.sru_client import SRUClient
from .lib.ixtheo_client import IxTheoSearchHandler
from .lib.urls import url_data
from .lib.custom_format import custom_format
from .lib.archives import archive_org_data, archive_today_data

# --- Flask App Setup ---
# The path is relative to this file's location, so '../public' correctly points to the root.
app = Flask(__name__, static_folder=os.path.join(os.path.dirname(__file__), '..', 'public'))

# --- Caching ---
rawDataCache = LRUCache(maxsize=100)

# --- Data Normalization & Resolvers ---
# (This section is unchanged and correct)
def biblio_record_to_dict(record) -> dict:
    doc_type = (record.format or record.document_type or 'book').lower()
    cite_type_mapping = {"journal article": "article-journal", "jour": "article-journal", "book chapter": "chapter", "chap": "chapter", "book": "book", "thesis": "book", "thes": "book"}
    cite_type = next((ct for dt, ct in cite_type_mapping.items() if dt in doc_type), 'book')
    title_raw = record.title or ""
    title, subtitle = "", ""
    if ':' in title_raw:
        parts = title_raw.split(':', 1)
        title, subtitle = (parts[0].strip(), parts[1].strip() if len(parts) > 1 else "")
    else:
        title = title_raw
    def format_name(name_str):
        if not name_str: return "", ""
        parts = name_str.split(',', 1)
        return (parts[1].strip(), parts[0].strip()) if len(parts) > 1 else ("", name_str)
    return {'title': title, 'subtitle': subtitle, 'authors': [format_name(author) for author in (record.authors or [])], 'editors': [format_name(editor) for editor in (record.editors or [])], 'translators': [format_name(translator) for translator in (record.translators or [])], 'year': record.year, 'publisher': record.publisher_name, 'address': record.place_of_publication, 'isbn': record.isbn, 'issn': record.issn, 'journal': record.journal_title, 'volume': record.volume, 'issue': record.issue, 'page': record.pages, 'doi': record.doi, 'cite_type': cite_type, 'series': record.series, 'edition': record.edition}

def sru_search(user_input: str) -> list:
    logger.info(f"Initiating SRU search for: '{user_input}'")
    client = SRUClient(base_url='https://services.dnb.de/sru/dnb', default_schema='RDFxml')
    query = f'WOE="{user_input}"'
    _, records = client.search(query=query, max_records=20)
    return [biblio_record_to_dict(rec) for rec in records] if records else []

def ixtheo_search(user_input: str) -> list:
    logger.info(f"Initiating IxTheo search for: '{user_input}'")
    handler = IxTheoSearchHandler()
    _, records = handler.search(query=user_input, max_results=20)
    enhanced_records = [handler.get_record_with_ris(rec) for rec in records if rec]
    dict_records = [biblio_record_to_dict(rec) for rec in enhanced_records if rec]
    dict_records.sort(key=lambda r: (0 if r.get('cite_type') == 'book' else 1 if r.get('cite_type') == 'chapter' else 2))
    return dict_records

def google_encrypted_data(url: str) -> dict:
    parsed_url = urlparse(url)
    return google_books_data(parsed_url) if parsed_url.path.startswith('/books') else url_data(url)

get_resolver = {'archive': archive_today_data, 'books.google': google_books_data, 'encrypted.google': google_encrypted_data, 'google': google_encrypted_data, 'jstor': jstor_data, 'ketab': ketabir_data, 'noorlib': noorlib_data, 'noormags': noormags_data, 'web-beta.archive': archive_org_data, 'web.archive': archive_org_data, 'worldcat': worldcat_data}

def url_doi_isbn_data(user_input: str) -> dict:
    en_user_input = unquote(uninum2en(user_input))
    if '.' in en_user_input or '/' in en_user_input:
        url = 'http://' + user_input if '://' not in user_input else user_input
        parsed_url = urlparse(url)
        hostname = (parsed_url.hostname or '').removeprefix('www.')
        hostname_core = hostname.rpartition('.')[0] or hostname
        if (data_func := get_resolver.get(hostname_core)):
            return data_func(url) if data_func is not google_books_data else data_func(parsed_url)
        if (m := doi_search(unescape(en_user_input))):
            try: return doi_data(m[0], True)
            except (JSONDecodeError, CurlError):
                if not user_input.startswith('http'): raise
        return url_data(url)
    if (m := isbn_10or13_search(en_user_input)):
        return isbn_data(m[0], True)
    logger.info(f"Input '{user_input}' is not an identifier, falling back to IxTheo search.")
    return ixtheo_search(user_input)

input_type_to_resolver = {'': url_doi_isbn_data, 'url-doi-isbn': url_doi_isbn_data, 'pmid': pmid_data, 'pmcid': pmcid_data, 'oclc': oclc_data, 'sru': sru_search, 'ixtheo': ixtheo_search}

# --- Flask Routes ---
@app.route('/', methods=['POST'])
def api_cite():
    params = {}
    try:
        params = request.get_json()
        user_input, input_type, template_format = (params.get('user_input', '').strip(), params.get('input_type', ''), params.get('template_format', 'custom'))
        if not user_input: return jsonify("Please provide a search query."), 400
        cache_key = f"{input_type}:{user_input}"
        raw_data = None
        if cache_key in rawDataCache:
            logger.info(f"Cache HIT for key: {cache_key}")
            raw_data = rawDataCache[cache_key]
        else:
            logger.info(f"Cache MISS for key: {cache_key}")
            resolver = input_type_to_resolver.get(input_type)
            if not resolver: return jsonify(f"Invalid input type: {input_type}"), 400
            raw_data = resolver(user_input)
            rawDataCache[cache_key] = raw_data
        if isinstance(raw_data, list):
            if not raw_data: return jsonify("No results found.")
            if template_format == 'custom':
                 formatted_string = "\n\n".join([custom_format(item) for item in raw_data])
            else:
                 outputs = [data_to_sfn_cit_ref(item, template_format=template_format) for item in raw_data]
                 format_map = {'sfn': 0, 'cite': 1, 'ref': 2}
                 idx = format_map.get(template_format, 1)
                 formatted_string = "\n".join([o[idx] for o in outputs])
        else:
            _, formatted_citation, _ = data_to_sfn_cit_ref(raw_data, template_format=template_format)
            formatted_string = formatted_citation
        return jsonify(formatted_string)
    except Exception as e:
        logger.exception(f"Error processing request for input: {params.get('user_input')}")
        return jsonify(f"An error occurred: {str(e)}"), 500

@app.route('/', defaults={'path': ''})
@app.route('/<path:path>')
def serve_static(path):
    if path and os.path.exists(os.path.join(app.static_folder, path)):
        return send_from_directory(app.static_folder, path)
    return send_from_directory(app.static_folder, 'index.html')

if __name__ == "__main__":
    app.run(debug=True, port=5001)