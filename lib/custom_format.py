# lib/custom_format.py
import re
import datetime

def format_book(d):
    """Formats a book citation, correctly handling edited volumes and series."""
    is_edited_volume = not d.get("authors") and d.get("editors")
    creators = d.get("editors") if is_edited_volume else d.get("authors", [])
    
    creator_list = [f"{first} {last}".strip() for first, last in creators or []]
    creators_str = ", ".join(filter(None, creator_list))

    title = (d.get("title") or "").strip()
    subtitle = (d.get("subtitle") or "").strip()
    series = (d.get("series") or "").strip()

    title_part = title
    if subtitle:
        title_part += f": {subtitle}"
    if series:
        title_part += f" ({series})"
    
    address = (d.get("address") or "").strip()
    publisher = (d.get("publisher") or "").strip()
    year = str(d.get("year") or "").strip()
    
    pub_part_bits = []
    if address:
        pub_part_bits.append(f"{address}:")
    if publisher:
        pub_part_bits.append(publisher)
    if year:
        pub_part_bits.append(year)
    pub_part = " ".join(pub_part_bits)
    
    isbn = (d.get("isbn") or "").strip()
    isbn_part = f"ISBN {isbn}" if isbn else ""

    if is_edited_volume:
        final_parts = [f"{creators_str} (ed.): {title_part}"]
    else:
        final_parts = [f"{creators_str}: {title_part}" if creators_str else title_part]

    if pub_part:
        final_parts.append(pub_part)
    if isbn_part:
        final_parts.append(isbn_part)
        
    return ", ".join(filter(None, final_parts)) + "."

def format_article_in_journal(d):
    """Formats a journal article citation with full details."""
    authors_list = [f"{last}, {first}".strip(', ') for first, last in (d.get("authors") or [])]
    authors_str = " and ".join(filter(None, authors_list))

    title = (d.get("title") or "").strip()
    subtitle = (d.get("subtitle") or "").strip()
    full_title = f"{title}: {subtitle}" if subtitle else title

    journal = (d.get("journal") or "").strip()
    issue = (d.get("issue") or "").strip()
    volume = (d.get("volume") or "").strip()
    year = str(d.get("year") or "").strip()
    pages = (d.get("page") or "").strip()
    url = (d.get("url") or "").strip()

    # Assemble journal part: Journal Issue/Volume (Year)
    journal_part_bits = [journal]
    if volume:
        journal_part_bits.append(volume)
    if issue:
        journal_part_bits.append(f"({issue})")
    
    # Extract just the year from a full date for cleaner display
    year_match = re.search(r'\d{4}', str(year))
    if year_match:
        journal_part_bits.append(f"({year_match.group(0)})")
    
    journal_part = " ".join(filter(None, journal_part_bits))

    # Assemble the final string
    parts = [f"{authors_str}:" if authors_str else "", f'"{full_title}"', "in:", journal_part]
    if pages:
        parts.append(f", {pages}")
    
    final_string = " ".join(filter(None, parts))
    # Tidy up whitespace and punctuation
    final_string = re.sub(r'\s+,', ',', final_string).rstrip(',') + "."
    
    if url:
        access_date_str = f"accessed {datetime.date.today().strftime('%B %d, %Y')}"
        final_string += f" URL: {url} ({access_date_str})."
        
    return final_string


def format_article_in_book(d):
    """Formats an article in a book (chapter) citation."""
    authors_list = [f"{last}, {first}".strip(', ') for first, last in (d.get("authors") or [])]
    authors_str = " and ".join(filter(None, authors_list))

    article_title = (d.get("title") or "").strip()
    subtitle = (d.get("subtitle") or "").strip()
    full_article_title = f'"{article_title}: {subtitle}"' if subtitle else f'"{article_title}"'
    
    editors_list = [f"{first} {last}".strip() for first, last in (d.get("editors") or [])]
    editors_str = " and ".join(filter(None, editors_list))
    if editors_str: editors_str += " (ed.)"

    book_title = (d.get("series") or d.get("booktitle") or "").strip()
    
    address = (d.get("address") or "").strip()
    publisher = (d.get("publisher") or "").strip()
    year = str(d.get("year") or "").strip()
    
    pub_part_bits = []
    if address:
        pub_part_bits.append(f"{address}:")
    if publisher:
        pub_part_bits.append(publisher)
    if year:
        pub_part_bits.append(year)
    pub_part = " ".join(pub_part_bits)

    pages = (d.get("page") or "").strip()
    
    in_part_bits = [editors_str, book_title, pub_part]
    if pages:
        in_part_bits.append(f", {pages}")

    in_part = ", ".join(filter(None, in_part_bits))
    final_str = f"{authors_str}: {full_article_title}" if authors_str else full_article_title
    if in_part: final_str += f", in: {in_part}"
    
    return final_str + "."

def custom_format(d):
    """Returns a citation string in a custom format based on cite_type."""
    cite_type = d.get("cite_type", "book")
    
    # **FIXED**: Now correctly handles both 'journal' and 'article-journal'
    if cite_type in ("article-journal", "journal"):
        return format_article_in_journal(d)
    elif cite_type == "chapter":
        return format_article_in_book(d)
    # Default to book format for "book" and any other unknown types
    else:
        return format_book(d)