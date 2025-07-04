import logging
from json import loads
from threading import Thread

# Correctly import the 'classify' service as per the official documentation
from isbnlib import NotValidISBNError, classify, info as isbn_info, mask as isbn_mask
from langid import classify as lang_classify
from regex import search

from config import LANG
from lib import four_digit_num, logger, request
from lib.citoid import citoid_data
from lib.commons import (
    ReturnError,
    isbn10_search,
    isbn13_search,
)
from lib.ketabir import (
    isbn_to_url as ketabir_isbn2url,
    ketabir_data as ketabir_url_to_dict,
)
from lib.urls import url_data

class IsbnError(Exception):
    """Raise when bibliographic information is not available."""
    pass

# --- Thread-Target Helper Functions ---

def _get_oclc_data(isbn: str, results: dict):
    """
    Thread target for WorldCat/OCLC API. Finds OCLC ID from ISBN using the classify service.
    """
    logger.debug(f"[ISBN Fetch] Starting OCLC classify for {isbn}...")
    try:
        # CORRECT API USAGE: Call classify() and get the 'oclc' key from the result
        classifications = classify(isbn)
        if oclc_ids := classifications.get('oclc'):
            # The result can be a list of strings or dicts, handle both
            oclc_id = oclc_ids[0].get('id') if isinstance(oclc_ids[0], dict) else oclc_ids[0]
            if oclc_id:
                logger.debug(f"[ISBN Fetch] Found OCLC ID: {oclc_id}. Fetching data...")
                results['oclc'] = oclc_data(oclc_id)
                logger.debug(f"[ISBN Fetch] Successfully fetched data from OCLC.")
        else:
            logger.debug(f"[ISBN Fetch] No OCLC ID found for ISBN {isbn}.")
    except Exception as e:
        logger.warning(f"[ISBN Fetch] OCLC lookup failed for {isbn}: {e}")

def _get_citoid_data(isbn: str, results: dict):
    """Thread target for Wikipedia's Citoid API."""
    logger.debug(f"[ISBN Fetch] Starting Citoid lookup for {isbn}...")
    try:
        results['citoid'] = citoid_data(isbn)
        logger.debug(f"[ISBN Fetch] Successfully fetched data from Citoid.")
    except Exception as e:
        logger.warning(f"[ISBN Fetch] Citoid lookup failed for {isbn}: {e}")

def _get_google_books(isbn: str, results: dict):
    """Thread target for Google Books API."""
    logger.debug(f"[ISBN Fetch] Starting Google Books lookup for {isbn}...")
    try:
        api_url = f'https://www.googleapis.com/books/v1/volumes?q=isbn:{isbn.replace("-", "")}'
        j = request(api_url).json()
        if not j.get('items'):
            logger.debug(f"[ISBN Fetch] Google Books returned no items for {isbn}.")
            return
        results['google'] = j['items'][0].get('volumeInfo', {})
        logger.debug(f"[ISBN Fetch] Successfully fetched data from Google Books.")
    except Exception as e:
        logger.warning(f"[ISBN Fetch] Google Books lookup failed for {isbn}: {e}")

def _get_ketabir_data(isbn: str, results: dict):
    """Thread target for Ketab.ir (for Iranian ISBNs)."""
    logger.debug(f"[ISBN Fetch] Starting Ketab.ir lookup for Iranian ISBN {isbn}...")
    try:
        if (url := ketabir_isbn2url(isbn)) is None:
            logger.debug(f"[ISBN Fetch] Ketab.ir has no entry for ISBN {isbn}.")
            return
        if d := ketabir_url_to_dict(url):
            results['ketabir'] = d
            logger.debug(f"[ISBN Fetch] Successfully fetched data from Ketab.ir.")
    except Exception as e:
        logger.warning(f"Ketab.ir lookup failed for {isbn}: {e}")

# --- Main Data Aggregation Function ---

def isbn_data(isbn_container_str: str, pure: bool = False) -> dict:
    """
    Fetches book data from multiple sources (OCLC, Citoid, Google Books, Ketab.ir)
    and merges them to create the most complete record possible.
    """
    logger.debug(f"Starting multi-source search for: {isbn_container_str}")
    isbn = isbn_container_str if pure else (m[0] if (m := isbn13_search(isbn_container_str)) else (m[0] if (m := isbn10_search(isbn_container_str)) else None))
    if not isbn:
        raise IsbnError("No valid ISBN found in the input.")
    logger.debug(f"Found valid ISBN: {isbn}")

    # 1. Run all data fetches in parallel
    results = {}
    threads = [
        Thread(target=_get_google_books, args=(isbn, results)),
        Thread(target=_get_citoid_data, args=(isbn, results)),
        Thread(target=_get_oclc_data, args=(isbn, results)),
    ]
    is_iranian = isbn_info(isbn) == 'Iran'
    if is_iranian:
        threads.append(Thread(target=_get_ketabir_data, args=(isbn, results)))
    
    for t in threads: t.start()
    for t in threads: t.join()
    logger.debug(f"All fetches complete. Raw results: {results}")

    # 2. Intelligently merge the results
    final_data = {}
    oclc_res = results.get('oclc', {})
    citoid_res = results.get('citoid', {})
    google_res = results.get('google', {})
    ketabir_res = results.get('ketabir', {}) if is_iranian else {}

    # Define the priority of sources. For Farsi, Ketab.ir is most important.
    source_priority = [oclc_res, citoid_res, google_res, ketabir_res]
    if is_iranian and LANG == 'fa':
        source_priority = [ketabir_res, oclc_res, citoid_res, google_res]
    
    # Helper to get the first available value from sources
    def get_first(key):
        for source in source_priority:
            if value := source.get(key):
                return value
        return None

    # Merge fields based on priority
    final_data['title'] = get_first('title')
    final_data['publisher'] = get_first('publisher')
    final_data['address'] = get_first('address') or get_first('publisher-location')
    final_data['year'] = get_first('year') or (get_first('date') or get_first('publishedDate', ''))[:4]

    # Handle authors and editors. Editors are only used if no authors are found.
    authors = get_first('authors')
    editors = get_first('editors')
    if authors:
        final_data['authors'] = authors
    elif editors:
        final_data['editors'] = editors

    # Handle subtitle. Some sources have it split, some have it in the title.
    title_val = final_data.get('title', '')
    if ':' in title_val:
        parts = title_val.split(':', 1)
        final_data['title'] = parts[0].strip()
        final_data['subtitle'] = parts[1].strip()
    else:
        final_data['subtitle'] = get_first('subtitle')

    # Ensure we have a result
    if not final_data.get('title'):
        raise ReturnError(f'Could not find any information for ISBN: {isbn}', '', '')

    # 3. Final cleanup and formatting of the merged data
    final_data['isbn'] = isbn_mask(isbn)
    if not final_data.get('language'):
        title = final_data.get('title', '')
        if title: final_data['language'] = lang_classify(title)[0]

    # Normalize author/editor formats from different sources into the standard ('first', 'last') tuple
    for key in ['authors', 'editors']:
        if key in final_data:
            normalized_people = []
            for person in final_data[key]:
                if isinstance(person, dict): # From Citoid
                    normalized_people.append((person.get('given', ''), person.get('family', '')))
                elif isinstance(person, str): # From Google Books
                    normalized_people.append(tuple(person.rsplit(' ', 1)) if ' ' in person else ('', person))
                elif isinstance(person, (list, tuple)): # Already normalized
                    normalized_people.append(tuple(person))
            final_data[key] = normalized_people

    logger.debug(f"Returning final, merged data: {final_data}")
    return final_data


# --- WorldCat-Specific Functions (Unchanged) ---

def worldcat_data(url: str) -> dict:
    try:
        oclc = search(r'(?i)worldcat.org/(?:title|oclc)/(\d+)', url)[1]
    except TypeError:
        return url_data(url)
    return oclc_data(oclc)

def oclc_data(oclc: str) -> dict:
    r = request(
        'https://search.worldcat.org/api/search-item/' + oclc,
        headers={'Referer': 'https://search.worldcat.org/', 'Accept': '*/*'},
    )
    j = loads(r.content)
    if not j:
        raise IsbnError(f"Invalid or not found OCLC number: {oclc}")

    d = {}
    d['cite_type'] = j.get('generalFormat', 'book').lower()
    d['title'] = j.get('title')
    
    authors = []
    if contributors := j.get('contributors'):
        for c in contributors:
            if 'nonPersonName' in c:
                authors.append(('', c['nonPersonName'].get('text', '')))
            elif 'firstName' in c and 'secondName' in c:
                authors.append((c['firstName'].get('text', ''), c['secondName'].get('text', '')))
    d['authors'] = authors
    
    if (publisher := j.get('publisher')) and publisher != '[publisher not identified]':
        d['publisher'] = publisher
    if (place := j.get('publicationPlace')) and place != '[Place of publication not identified]':
        d['address'] = place
    if m := four_digit_num(j.get('publicationDate', '')):
        d['year'] = m[0]
    
    d['language'] = j.get('catalogingLanguage')
    if isbn := j.get('isbn13'):
        d['isbn'] = isbn
    if issns := j.get('issns'):
        d['issn'] = issns[0]
    
    d['oclc'] = oclc
    return d