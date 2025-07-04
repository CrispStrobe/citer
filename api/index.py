from flask import Flask, request
# Import the main app logic from your existing app.py file
from app import app as wsgi_app

# Create a Flask app instance
app = Flask(__name__)

# This single route will catch all requests and pass them to your WSGI app
@app.route('/', defaults={'path': ''})
@app.route('/<path:path>')
def catch_all(path):
    # Recreate the WSGI environment from the Flask request object
    environ = request.environ

    # A simple, local "start_response" function
    status_headers = [None, None]
    def start_response(status, headers):
        status_headers[0] = status
        status_headers[1] = headers

    # Call your original WSGI app
    response_body = wsgi_app(environ, start_response)
    
    # Return the response in a way Flask understands
    return response_body, status_headers[0], status_headers[1]

# This is the entry point Vercel will use
# The name 'app' is the default that Vercel looks for.
if __name__ == "__main__":
    app.run(debug=True)