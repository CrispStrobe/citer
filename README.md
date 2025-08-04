# Citer

Citer is a versatile web application designed to generate academic citations from various online sources. This is an enhanced fork of the original `citer` by 5j9, updated with a modern front-end, an efficient caching backend, and support for additional search protocols like SRU and IxTheo.

This tool is designed for deployment on any Python-compatible web server and is pre-configured for Vercel.

## Features

Citer can generate citations from a wide range of inputs:

  - **Standard Identifiers**: URLs, DOIs, ISBNs, PMIDs, PMCIDs, and OCLC numbers.
  - **Web Content**: Automatically extracts metadata from many major news websites and academic pages.
  - **SRU Protocol**: Search major library catalogs like the Deutsche Nationalbibliothek (DNB) and Bibliothèque nationale de France (BnF) directly.
  - **IxTheo Search**: A specialized client for searching the Index Theologicus (IxTheo) for theological literature.
  - **Custom Citation Formatting**: In addition to standard Wikipedia templates (`{{cite}}`, `{{sfn}}`), it provides a customizable citation style for specific academic needs.
  - **Modern UI**: A fast, responsive single-page application with client-side history and a clean user experience.

## Local Development Setup

Follow these instructions to get the application running on your local machine for development or testing.

### Prerequisites

  - Python 3.11 or higher.
  - Git for cloning the repository.

### 1\. Clone the Repository

```bash
git clone https://github.com/CrispStrobe/citer.git
cd citer
```

### 2\. Create and Activate a Virtual Environment

A virtual environment keeps your project's dependencies isolated. This is a crucial step to avoid system-wide package conflicts.

**For Windows (PowerShell):**

```powershell
# Create the virtual environment
python -m venv .venv

# Activate it (you must do this every time you open a new terminal for this project)
.\.venv\Scripts\Activate.ps1
```

**For Linux / macOS (bash/zsh):**

```bash
# Create the virtual environment
python3 -m venv .venv

# Activate it (you must do this every time you open a new terminal for this project)
source .venv/bin/activate
```

After activation, you will see `(.venv)` at the beginning of your command prompt.

### 3\. Install Dependencies with `uv`

We recommend using `uv`, a very fast Python package installer and resolver, which is a drop-in replacement for `pip`.

```bash
# Install uv itself
pip install uv

# Use uv to install all project dependencies from requirements.txt
uv pip install -r requirements.txt
```

### 4\. Create the `config.py` File

The application requires a `config.py` file for settings. You can create one from the provided example.

**For Windows (PowerShell/CMD):**

```powershell
copy config.py.example config.py
```

**For Linux / macOS (bash/zsh):**

```bash
cp config.py.example config.py
```

You can edit this new `config.py` file later if you need to add API keys (e.g., for NCBI).

## Running the Application

With the setup complete, you can run the local Flask development server. We recommend running in debug mode, which provides more detailed error messages.

**For Windows (PowerShell):**

```powershell
# Set the environment variable for the current session, then run the app
$env:CITER_DEBUG="1"; uv run app.py
```

**For Linux / macOS (bash/zsh):**

```bash
# Set the environment variable and run the app
export CITER_DEBUG=1
uv run app.py
```

The server will start, and you can access the application at **[http://127.0.0.1:5001](https://www.google.com/search?q=http://127.0.0.1:5001)**.

Or use the `run.bat` Script in Windows.

## Command-Line Interface

Citer includes a command-line tool (`search.py`) for directly searching SRU and IxTheo endpoints from your terminal.

### List Available Endpoints

To see a list of all supported search endpoints, run:

```bash
python3 search.py --list
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

## Deployment

This project is pre-configured for easy deployment on **Vercel**. The `vercel.json` file routes all requests to the Flask application in `api/index.py`, and the `public/index.html` is served as the static front-end.

To deploy, simply run the Vercel CLI command from the project's root directory:

```bash
vercel deploy
```

## License

This project is licensed under the **GNU General Public License v3.0**. See the `LICENSE.md` file for details.