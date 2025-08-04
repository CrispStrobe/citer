# api/index.py
# This file acts as the entry point for Vercel.
# It imports the main Flask app instance from app.py at the project root.

import sys
import os

# Add the project root to the Python path to allow importing 'app'
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from app import app