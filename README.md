# Citer

Citer is a versatile web application and command-line tool designed to generate academic citations from various online sources. This is a fork of the original `citer` by 5j9, enhanced to support additional search protocols, custom citation formats, and an improved front-end.

This tool is intended for deployment on a Python-compatible web server. The original is hosted on Toolforge at: [citer.toolforge.org](https://citer.toolforge.org/)

## Features

Citer can generate citations from a wide range of inputs:

  * **Standard Identifiers**: URLs, DOIs, ISBNs, PMIDs, PMCIDs, and OCLC numbers.
  * **Web Content**: Automatically extracts metadata from many major news websites and academic pages.
  * **SRU Protocol**: Search major library catalogs like the Deutsche Nationalbibliothek (DNB) and Bibliothèque nationale de France (BnF) directly.
  * **IxTheo Search**: A specialized client for searching the Index Theologicus (IxTheo) for theological literature.
  * **Custom Citation Formatting**: In addition to standard Wikipedia templates, it provides a customizable citation style for specific academic needs.

## Command-Line Interface

Citer includes a command-line tool (`search.py`) for directly searching SRU and IxTheo endpoints from your terminal.

### List Available Endpoints

To see a list of all supported search endpoints, run:

```bash
python3 search.py --list
```

You can also filter by protocol:

```bash
python3 search.py --list --protocol sru
```

### Perform a Search

To search an endpoint, specify the protocol, the endpoint ID, and your query.

**SRU Search Example (DNB):**

```bash
python3 search.py --protocol sru --endpoint dnb --query "TIT=Theologie"
```

**IxTheo Search Example:**

```bash
python3 search.py --protocol ixtheo --endpoint ixtheo --query "Populismus"
```

You can also format the output as JSON:

```bash
python3 search.py --protocol ixtheo --query "Islam" --format json
```

## Installation and Setup

To set up the project locally, you will need Python 3.

1.  **Clone the repository:**

    ```bash
    git clone https://github.com/CrispStrobe/citer.git
    cd citer
    ```

2.  **Create a virtual environment:**

    ```bash
    python3 -m venv .venv
    source .venv/bin/activate
    ```

3.  **Install the required packages:**
    The necessary packages are listed in `pyproject.toml`. Install them using pip:

    ```bash
    pip install "curl-cffi[requests]" "jdatetime" "langid" "lxml" "isbnlib" "beautifulsoup4" "requests" "regex"
    ```

4.  **Run the application:**

    ```bash
    python3 app.py
    ```

    The application will be available at `http://localhost:5000`.

## License

This project is licensed under the **GNU General Public License v3.0**. See the [LICENSE](https://www.google.com/search?q=LICENSE) file for details.