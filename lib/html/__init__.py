from html import escape
from os.path import dirname
from zlib import adler32

from config import LANG, STATIC_PATH

htmldir = dirname(__file__)

CSS = open(f'{htmldir}/common.css', 'rb').read()
# Note: The 'fa' (Farsi) language logic is preserved.
if LANG == 'fa':
    CSS = CSS.replace(b'right;', b'left;')

ALLOW_ALL_ORIGINS = ('Access-Control-Allow-Origin', '*')
CACHE_FOREVER = ('Cache-Control', 'immutable, public, max-age=31536000')
CSS_HEADERS = (
    ALLOW_ALL_ORIGINS,
    ('Content-Type', 'text/css; charset=UTF-8'),
    ('Content-Length', str(len(CSS))),
    CACHE_FOREVER,
)

# We now only load common.js, not en.js
JS = open(f'{htmldir}/common.js', 'rb').read()

JS_HEADERS = (
    ALLOW_ALL_ORIGINS,
    ('Content-Type', 'application/javascript; charset=UTF-8'),
    ('Content-Length', str(len(JS))),
    CACHE_FOREVER,
)

# Generate versioned paths
JS_PATH = STATIC_PATH + str(adler32(JS))
CSS_PATH = STATIC_PATH + str(adler32(CSS))

# Read the raw HTML template
HTML_TEMPLATE_STR = open(f'{htmldir}/{LANG}.html', encoding='utf8').read()

def scr_to_html(
    scr: tuple, date_format: str, pipe_format: str, input_type: str, template_format: str
) -> str:
    """
    Inserts citation data into the HTML template and returns the complete HTML body.
    Conditionally hides formatting options if 'custom' format is selected.
    """
    html_str = HTML_TEMPLATE_STR
    styles_injection = ''

    # Replace static asset paths with versioned paths for cache-busting.
    html_str = html_str.replace(f'href="static/{LANG}.css"', f'href="/{CSS_PATH}.css"')
    html_str = html_str.replace(f'src="static/common.js"', f'src="/{JS_PATH}.js"')

    if template_format == 'custom':
        custom_citation_output = scr[1]
        sfn, cit, ref = '', '', ''

        styles_injection = """
        <style>
            #standard-format-options, #standard-format-outputs {
                display: none;
            }
            #custom_format_output {
                display: block !important;
            }
        </style>
        """
    else:
        sfn, cit, ref = [escape(i) for i in scr]
        custom_citation_output = ''

    # Inject the conditional styles (or an empty string) into the placeholder.
    html_str = html_str.replace('{styles_placeholder}', styles_injection)
    
    # Replace the data placeholders with content.
    html_str = html_str.replace('$shortened', sfn + '\n\n' + cit)
    html_str = html_str.replace('$named_ref', ref)
    html_str = html_str.replace('$custom_citation', custom_citation_output)

    # Finally, set the selected options in the form controls.
    html_str = html_str.replace(f'value="{template_format}"', f'value="{template_format}" selected', 1)
    html_str = html_str.replace(f'value="{date_format}"', f'value="{date_format}" checked', 1)
    html_str = html_str.replace(f'value="{pipe_format}"', f'value="{pipe_format}" checked', 1)
    html_str = html_str.replace(f'value="{input_type}"', f'value="{input_type}" selected', 1)

    return html_str

# Predefined responses
if LANG == 'en':
    DEFAULT_SCR = ('Generated citation will appear here...', '', '')
    HTTPERROR_SCR = (
        'HTTP error:',
        'One or more of the web resources required to create this citation are not accessible at this moment.',
        '',
    )
else:
    DEFAULT_SCR = ('یادکرد ساخته‌شده اینجا نمایان خواهد شد...', '', '')
    HTTPERROR_SCR = (
        'خطای اچ‌تی‌تی‌پی:',
        'یک یا چند مورد از منابع اینترنتی مورد نیاز برای ساخت این یادکرد در این لحظه در دسترس نیستند و یا ورودی نامعتبر است.',
        '',
    )