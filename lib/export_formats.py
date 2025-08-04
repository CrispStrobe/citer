# lib/export_formats.py
import re

def escape_bibtex(text):
    """Escapes special characters for safe inclusion in BibTeX fields."""
    if not isinstance(text, str):
        return text
    # A more comprehensive list of characters to escape
    return text.replace('\\', '\\textbackslash{}') \
               .replace('{', '\\{') \
               .replace('}', '\\}') \
               .replace('$', '\\$') \
               .replace('&', '\\&') \
               .replace('%', '\\%') \
               .replace('#', '\\#') \
               .replace('_', '\\_') \
               .replace('^', '\\textasciicircum{}') \
               .replace('~', '\\textasciitilde{}')

def to_bibtex(d: dict) -> str:
    """Converts a standardized data dictionary to a BibTeX string."""
    cite_type = d.get("cite_type", "book")
    entry_type_map = {
        "article-journal": "article",
        "journal": "article",
        "chapter": "incollection",
        "book": "book",
        "thesis": "phdthesis",
    }
    entry_type = entry_type_map.get(cite_type, "misc")

    # Create a citation key (e.g., Abdel-Malek1963)
    key = "citekey"
    if authors := d.get("authors"):
        if authors and authors[0][1]:
            last_name = authors[0][1].split()[-1]
            year = d.get("year", "")
            match = re.search(r'\d{4}', str(year))
            year_str = match.group(0) if match else ""
            key = f"{re.sub(r'[^a-zA-Z]', '', last_name)}{year_str}"
    
    bib_fields = []
    
    # Handle titles (article title vs. book title)
    if entry_type == 'incollection':
        if title := d.get('title'): bib_fields.append(f"  title     = {{{escape_bibtex(title)}}}")
        if booktitle := d.get('series'): bib_fields.append(f"  booktitle = {{{escape_bibtex(booktitle)}}}")
    else:
        full_title = d.get('title', '')
        if subtitle := d.get('subtitle'):
            full_title += f": {subtitle}"
        if full_title: bib_fields.append(f"  title     = {{{escape_bibtex(full_title)}}}")

    # Authors and Editors
    if authors := d.get('authors'):
        author_str = " and ".join([f"{last}, {first}" for first, last in authors if first or last])
        bib_fields.append(f"  author    = {{{author_str}}}")
    if editors := d.get('editors'):
        editor_str = " and ".join([f"{last}, {first}" for first, last in editors if first or last])
        bib_fields.append(f"  editor    = {{{editor_str}}}")

    # Other common fields
    if journal := d.get('journal'): bib_fields.append(f"  journal   = {{{escape_bibtex(journal)}}}")
    if year := d.get('year'):
        match = re.search(r'\d{4}', str(year))
        if match: bib_fields.append(f"  year      = {{{match.group(0)}}}")
    if volume := d.get('volume'): bib_fields.append(f"  volume    = {{{volume}}}")
    if issue := d.get('issue'): bib_fields.append(f"  number    = {{{issue}}}")
    if pages := d.get('page'): bib_fields.append(f"  pages     = {{{str(pages).replace('–', '--')}}}")
    if publisher := d.get('publisher'): bib_fields.append(f"  publisher = {{{escape_bibtex(publisher)}}}")
    if address := d.get('address'): bib_fields.append(f"  address   = {{{escape_bibtex(address)}}}")
    if isbn := d.get('isbn'): bib_fields.append(f"  isbn      = {{{isbn}}}")
    if doi := d.get('doi'): bib_fields.append(f"  doi       = {{{doi}}}")
    if url := d.get('url'): bib_fields.append(f"  url       = {{{url}}}")

    fields_str = ",\n".join(bib_fields)
    return f"@{entry_type}{{{key},\n{fields_str}\n}}"

def to_ris(d: dict) -> str:
    """Converts a standardized data dictionary to an RIS string."""
    ris_lines = []
    
    cite_type = d.get("cite_type", "book")
    type_map = { "article-journal": "JOUR", "journal": "JOUR", "chapter": "CHAP", "book": "BOOK", "thesis": "THES" }
    ris_lines.append(f"TY  - {type_map.get(cite_type, 'GEN')}")

    full_title = d.get('title', '')
    if subtitle := d.get('subtitle'): full_title += f": {subtitle}"
    if full_title: ris_lines.append(f"TI  - {full_title}")
    
    if cite_type == 'chapter':
        if series := d.get('series'): ris_lines.append(f"T2  - {series}")
    
    if authors := d.get('authors'):
        for first, last in authors: ris_lines.append(f"AU  - {last}, {first}")
    if editors := d.get('editors'):
        for first, last in editors: ris_lines.append(f"ED  - {last}, {first}")

    if year := d.get('year'):
        match = re.search(r'\d{4}', str(year))
        if match: ris_lines.append(f"PY  - {match.group(0)}")
    
    if publisher := d.get('publisher'): ris_lines.append(f"PB  - {publisher}")
    if address := d.get('address'): ris_lines.append(f"CY  - {address}")
    if journal := d.get('journal'): ris_lines.append(f"JO  - {journal}")
    if volume := d.get('volume'): ris_lines.append(f"VL  - {volume}")
    if issue := d.get('issue'): ris_lines.append(f"IS  - {issue}")
    
    if pages := d.get('page'):
        page_str = str(pages)
        if '–' in page_str or '-' in page_str:
            try:
                sp, ep = re.split(r'[-–]', page_str, 1)
                ris_lines.append(f"SP  - {sp.strip()}")
                ris_lines.append(f"EP  - {ep.strip()}")
            except ValueError:
                ris_lines.append(f"SP  - {page_str}")
        else:
            ris_lines.append(f"SP  - {page_str}")
            
    if isbn := d.get('isbn'): ris_lines.append(f"SN  - {isbn}")
    if issn := d.get('issn'): ris_lines.append(f"SN  - {issn}") # SN is used for both
    if doi := d.get('doi'): ris_lines.append(f"DO  - {doi}")
    if url := d.get('url'): ris_lines.append(f"UR  - {url}")
    
    ris_lines.append("ER  -")
    return "\n".join(ris_lines)