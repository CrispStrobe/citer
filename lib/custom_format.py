import re

def format_book(d):
    """Formats a book citation, correctly handling edited volumes and series."""
    # If there are no authors, the editors become the primary creators.
    is_edited_volume = not d.get("authors") and d.get("editors")
    creators = d.get("editors") if is_edited_volume else d.get("authors", [])
    
    creator_list = [f"{first} {last}".strip() for first, last in creators or []]
    creators_str = ", ".join(filter(None, creator_list))

    title = (d.get("title") or "").strip()
    subtitle = (d.get("subtitle") or "").strip()
    series = (d.get("series") or "").strip()

    # Combine title, subtitle, and series
    title_part = title
    if subtitle:
        title_part += f". {subtitle}"
    if series:
        title_part += f" ({series})"
    
    # Publication info
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

    # Assemble the final string
    if is_edited_volume:
        final_parts = [f"{creators_str} (Hg.): {title_part}"]
    else:
        final_parts = [f"{creators_str}: {title_part}" if creators_str else title_part]

    if pub_part:
        final_parts.append(pub_part)
    if isbn_part:
        final_parts.append(isbn_part)
        
    return ", ".join(filter(None, final_parts)) + "."

def format_article_in_journal(d):
    """Formats a journal article citation as per the required style."""
    authors_list = [f"{last}, {first}".strip(', ') for first, last in (d.get("authors") or [])]
    authors_str = " and ".join(filter(None, authors_list))

    # Combine title and subtitle for the full article title
    title = (d.get("title") or "").strip()
    subtitle = (d.get("subtitle") or "").strip()
    full_title = f"{title}: {subtitle}" if subtitle else title

    journal = (d.get("journal") or "").strip()
    issue = (d.get("issue") or "").strip()
    volume = (d.get("volume") or "").strip()
    year = str(d.get("year") or "").strip()
    pages = (d.get("page") or "").strip() # Page range should already have en-dash

    # Assemble journal part: Journal Issue/Volume (Year)
    # Prioritizes issue ("Heft") over volume as requested.
    journal_part_bits = [journal]
    if issue:
        journal_part_bits.append(issue)
    elif volume:
        journal_part_bits.append(volume)
    
    if year:
        journal_part_bits.append(f"({year})")
    
    journal_part = " ".join(filter(None, journal_part_bits))

    # Assemble the final string
    parts = [f"{authors_str}:" if authors_str else "", f"{full_title},", "in:", journal_part]
    if pages:
        parts.append(f", {pages}")
    
    final_string = " ".join(filter(None, parts))
    # Tidy up whitespace and punctuation for a clean output
    final_string = re.sub(r'\s+,', ',', final_string).rstrip(',') + "."
    return final_string


def format_article_in_book(d):
    """Formats an article in a book (chapter) citation."""
    authors_list = [f"{last}, {first}".strip(', ') for first, last in (d.get("authors") or [])]
    authors_str = " and ".join(filter(None, authors_list))

    article_title = (d.get("title") or "").strip()
    subtitle = (d.get("subtitle") or "").strip()
    full_article_title = f"{article_title}: {subtitle}" if subtitle else article_title
    
    editors_list = [f"{first} {last}".strip() for first, last in (d.get("editors") or [])]
    editors_str = " and ".join(filter(None, editors_list))
    if editors_str: editors_str += " (Hg.)"

    book_title = (d.get("series") or "").strip()
    
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
        in_part_bits.append(pages)

    in_part = ", ".join(filter(None, in_part_bits))
    final_str = f"{authors_str}: {full_article_title}" if authors_str else full_article_title
    if in_part: final_str += f", in: {in_part}"
    
    return final_str + "."

def custom_format(d):
    """Returns a citation string in a custom format based on cite_type."""
    cite_type = d.get("cite_type", "book")
    
    if cite_type == "book":
        return format_book(d)
    elif cite_type == "article-journal":
        return format_article_in_journal(d)
    elif cite_type == "chapter":
        return format_article_in_book(d)
    else:
        # Fallback to book format for other types
        return format_book(d)