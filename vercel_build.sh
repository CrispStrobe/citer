#!/bin/bash

# Install all dependencies from requirements.txt
pip install -r requirements.txt

# Start the application using gunicorn (a production-grade server)
# This command is what Vercel will run.
gunicorn api.index:app
