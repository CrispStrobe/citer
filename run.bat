@echo off
echo Activating virtual environment...
call .\.venv\Scripts\activate.bat

echo Starting Citer server with DEBUG mode ON...
set CITER_DEBUG=1

uv run app.py