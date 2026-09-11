# lib/jsonld.py
"""Minimal schema.org JSON-LD extraction for URL scraping (upstream #23).

Many article pages embed machine-readable metadata in a
``<script type="application/ld+json">`` block. This pulls the citation-relevant
fields out of it so ``url_data`` can fall back to it when the plain ``<meta>``
tags don't carry them.
"""
from html import unescape as html_unescape
from json import loads as json_loads

from regex import compile as rc

_SCRIPT = rc(
    r'<script\b[^>]*\btype=(?<q>["\'])application/ld\+json(?P=q)[^>]*>'
    r'(?<result>[\s\S]*?)</script\s*>'
)

# @type values we treat as a citable creative work.
ARTICLE_TYPES = {
    'ScholarlyArticle',
    'Article',
    'NewsArticle',
    'BlogPosting',
    'TechArticle',
    'Report',
    'Book',
    'Chapter',
    'WebPage',
    'Review',
    'Dataset',
    'CreativeWork',
    'PublicationIssue',
    'Periodical',
}


def _iter_nodes(html: str):
    for m in _SCRIPT.finditer(html):
        try:
            data = json_loads(html_unescape(m['result']).strip())
        except Exception:
            # JSON-LD is frequently malformed; ignore blocks we cannot parse.
            continue
        stack = [data]
        while stack:
            node = stack.pop()
            if isinstance(node, list):
                stack.extend(node)
            elif isinstance(node, dict):
                yield node
                graph = node.get('@graph')
                if isinstance(graph, list):
                    stack.extend(graph)
                elif isinstance(graph, dict):
                    stack.append(graph)


def _person_name(a) -> str:
    if isinstance(a, str):
        return a
    if isinstance(a, dict):
        if nm := a.get('name'):
            return str(nm)
        given, family = a.get('givenName', ''), a.get('familyName', '')
        return f'{given} {family}'.strip()
    return ''


def find_json_ld(html: str) -> dict:
    """Return citation fields found in the page's schema.org JSON-LD blocks.

    Only fields that are actually present are returned; the caller decides
    precedence (JSON-LD is used as a fallback behind the ``<meta>`` tags).
    Authors are normalised to ``(first, last)`` tuples like the rest of citer.
    """
    d: dict = {}
    for node in _iter_nodes(html):
        t = node.get('@type')
        types = t if isinstance(t, list) else [t]
        if not any(isinstance(tt, str) and tt in ARTICLE_TYPES for tt in types):
            continue
        get = node.get

        if not d.get('title'):
            if title := (get('name') or get('headline')):
                d['title'] = str(title)

        if not d.get('authors') and (author := get('author')):
            authors = author if isinstance(author, list) else [author]
            names = [n for n in map(_person_name, authors) if n]
            if names:
                d['authors'] = [
                    tuple(n.rsplit(' ', 1)) if ' ' in n else ('', n) for n in names
                ]

        if not d.get('date'):
            if dp := (get('datePublished') or get('dateCreated')):
                d['date'] = str(dp)[:10]

        if not d.get('publisher'):
            pub = get('publisher')
            if isinstance(pub, dict) and pub.get('name'):
                d['publisher'] = pub['name']
            elif isinstance(pub, str):
                d['publisher'] = pub

        if not d.get('journal'):
            is_part_of = get('isPartOf')
            if isinstance(is_part_of, dict) and is_part_of.get('name'):
                d['journal'] = is_part_of['name']
            elif isinstance(get('publication'), dict) and get('publication').get('name'):
                d['journal'] = get('publication')['name']

        if not d.get('page'):
            start, end = get('pageStart'), get('pageEnd')
            if start:
                d['page'] = f'{start}–{end}' if end else str(start)
            elif pagination := get('pagination'):
                d['page'] = str(pagination)

        if not d.get('volume') and (volume := (get('volumeNumber') or get('volume'))):
            d['volume'] = str(volume)

        if not d.get('isbn') and (isbn := get('isbn')):
            d['isbn'] = str(isbn).split()[0]

        if not d.get('issn') and (issn := get('issn')):
            d['issn'] = str(issn).split()[0]

        if not d.get('doi'):
            for key in ('sameAs', 'identifier', 'url'):
                val = get(key)
                if isinstance(val, str) and '10.' in val and 'doi.org' in val:
                    d['doi'] = val.split('doi.org/')[-1]
                    break

    return d
