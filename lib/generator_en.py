# lib/generator_en.py
"""Functions for generating English Wikipedia citation templates."""

from datetime import date as Date
from functools import partial
from string import ascii_lowercase, digits

from lib import (
    doi_url_match,
    four_digit_num,
    fullname,
    logger,
    make_ref_name,
    open_access_url,
    rc,
    type_to_cite,
)
from lib.language import TO_TWO_LETTER_CODE
from lib.custom_format import custom_format

rm_ref_arg = partial(
    rc(r'(\s?\|\s?ref=({{.*?}}|harv))(?P<repl>\s?\|\s?|}})').sub, r'\g<repl>'
)
DIGITS_TO_EN = str.maketrans('°¹²³´µ¶·¸¹', '0123456789')

def sfn_cit_ref(
    d: dict, date_format: str = '%Y-%m-%d', pipe: str = ' | ', template_format: str = 'wikipedia'
) -> tuple:
    """Return sfn, citation, and ref."""
    g = d.get
    if template_format == 'custom':
        d['cite_type'] = type_to_cite(g('cite_type'))
        final_custom_citation = custom_format(d)
        return '', final_custom_citation, ''

    if not (cite_type := type_to_cite(g('cite_type'))):
        logger.warning('Unknown citation type: %s, d: %s', g('cite_type'), d)
        cite_type = ''
        cit = '* {{cite'
    else:
        cit = '* {{cite ' + cite_type
    sfn = '{{sfn'

    publisher = g('publisher')
    website = g('website')
    title = g('title')
    date = g('date')

    if cite_type == 'journal':
        journal = g('journal') or g('container-title')
    else:
        journal = g('journal')

    if cite_type == 'thesis':
        if (thesis_type := g('thesisType')) is not None:
            cit += f'{pipe}degree={thesis_type}'

    if authors := g('authors'):
        cit += names2para(authors, pipe, 'first', 'last', 'author')
        for first, last in authors[:4]:
            sfn += '|' + last
    else:
        sfn_ref_name = publisher or (f"''{journal}''" if journal else f"''{website}''" if website else title or 'Anon.')
        sfn += '|' + sfn_ref_name

    if editors := g('editors'):
        cit += names2para(editors, pipe, 'editor-first', 'editor-last', 'editor')
    
    if translators := g('translators'):
        for i, (first, last) in enumerate(translators):
            translators[i] = first, f'{last} (مترجم)'
        # Todo: add a 'Translated by ' before name of translators?
        others = g('others')
        if others:
            others.extend(g('translators'))
        else:
            d['others'] = g('translators')
    if others := g('others'):
        cit += names1para(others, pipe, 'others')

    if cite_type == 'book':
        booktitle = g('booktitle') or g('container-title')
    else:
        booktitle = None

    if booktitle := g('booktitle') or (g('container-title') if cite_type == 'book' else None):
        cit += f'{pipe}title={booktitle}'
        if title: cit += f'{pipe}chapter={title}'
    elif title:
        cit += f'{pipe}title={title}'
    else:
        cit += f'{pipe}title='

    if journal: cit += f'{pipe}journal={journal}'
    elif website: cit += f'{pipe}website={website}'

    if chapter := g('chapter'): cit += f'{pipe}chapter={chapter}'
    if publisher := (g('publisher') or g('organization')): cit += f'{pipe}publisher={publisher}'
    if address := (g('address') or g('publisher-location')): cit += f'{pipe}publication-place={address}'
    if edition := g('edition'): cit += f'{pipe}edition={edition}'
    if series := g('series'): cit += f'{pipe}series={series}'
    if volume := g('volume'): cit += f'{pipe}volume={str(volume).translate(DIGITS_TO_EN)}'
    if issue := (g('issue') or g('number')): cit += f'{pipe}issue={issue}'

    # --- START OF THE FIX ---
    year_for_sfn = None
    if date:
        if not isinstance(date, str):
            date = date.strftime(date_format)
        cit += f'{pipe}date={date}'
        if match := four_digit_num(str(date)):
            year_for_sfn = match[0]

    year_from_year_field = None
    if year_str := g('year'):
        if match := four_digit_num(str(year_str)):
            year_from_year_field = match[0]
    
    if year_from_year_field:
        if not date or year_from_year_field not in str(date):
            cit += f'{pipe}year={year_from_year_field}'
        year_for_sfn = year_from_year_field
    
    if year_for_sfn:
        sfn += f'|{year_for_sfn}'
    # --- END OF THE FIX ---

    if isbn := g('isbn'): cit += f'{pipe}isbn={isbn}'
    if issn := g('issn'): cit += f'{pipe}issn={issn}'
    if pmid := g('pmid'): cit += f'{pipe}pmid={pmid}'
    if pmcid := g('pmcid'): cit += f'{pipe}pmc=' + str(pmcid).lower().removeprefix('pmc')

    doi = g('doi')
    url = g('url')

    if doi:
        if not str(doi).startswith('10.5555'):
            cit += f'{pipe}doi={doi}'
            if (oa_url := open_access_url(doi)) and not url:
                url = oa_url # Use the OA URL if no other URL is present
                cit += f'{pipe}doi-access=free'

    if oclc := g('oclc'): cit += f'{pipe}oclc={oclc}'
    if jstor := g('jstor'):
        cit += f'{pipe}jstor={jstor}'
        if g('jstor-access'): cit += f'{pipe}jstor-access=free'

    pages = g('page')
    pages_in_sfn = False
    pages_in_cit = False
    if pages:
        pages_str = str(pages)
        if '–' in pages_str or '-' in pages_str:
            sfn += f'|pp={pages_str}'
            pages_in_sfn = True
        else:
            sfn += f'|p={pages_str}'
            pages_in_sfn = True
        
        if cite_type == 'journal':
            cit += f'{pipe}pages={pages_str}'
            pages_in_cit = True

    if url:
        if not doi or not doi_url_match(str(url)):
            cit += f'{pipe}url={url}'
        else:
            url = None # Prevent access-date for DOI URLs

    if not pages and cite_type != 'web':
        sfn += '|p='
        pages_in_sfn = True

    if archive_url := g('archive-url'):
        cit += f'{pipe}archive-url={archive_url}'
        cit += f'{pipe}archive-date={g("archive-date").strftime(date_format)}'
        cit += f'{pipe}url-status={g("url-status")}'

    if language := g('language'):
        lang_code = TO_TWO_LETTER_CODE(str(language).lower(), language)
        if str(lang_code).lower() != 'en':
            cit += f'{pipe}language=' + str(lang_code)

    if not authors:
        cit += f"{pipe}ref={{sfnref|{sfn_ref_name}"
        if year_for_sfn:
            cit += f'|{year_for_sfn}'
        cit += '}}'

    if url:
        cit += f'{pipe}access-date={Date.today().strftime(date_format)}'

    cit += '}}'
    sfn += '}}'

    ref_name = make_ref_name(g)
    ref_content = rm_ref_arg(cit[2:])
    if pages_in_sfn and not pages_in_cit and pages:
        ref_content = f'{ref_content[:-2]}{pipe}pages={pages}}}}}'
    
    ref = f'<ref name="{ref_name}">{ref_content}</ref>'
    return sfn, cit, ref

# The names2para and names1para functions remain unchanged
def names2para(names, pipe, fn_parameter, ln_parameter, nofn_parameter=None):
    c = 0
    s = ''
    for first, last in names:
        c += 1
        num_suffix = '' if c == 1 else str(c)
        if first or not nofn_parameter:
            s += f'{pipe}{ln_parameter}{num_suffix}={last}{pipe}{fn_parameter}{num_suffix}={first}'
        else:
            s += f'{pipe}{nofn_parameter}{num_suffix}={fullname(first, last)}'
    return s

def names1para(translators, pipe, para):
    s = f'{pipe}{para}='
    full_names = [fullname(first, last) for first, last in translators]
    if len(full_names) > 1:
        s += ', '.join(full_names[:-1]) + f', and {full_names[-1]}'
    elif full_names:
        s += full_names[0]
    return s
