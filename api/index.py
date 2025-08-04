# api/index.py
import sys
import os

# Add the project root directory to the Python path.
# This allows Vercel's serverless function to find the 'app' and 'lib' modules.
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

# Now that the path is fixed, we can import the app.
from app import app