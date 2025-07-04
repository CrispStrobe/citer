from flask import Flask, request, Response
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

    # A simple, local "start_response" function to capture status and headers
    status_headers = [None, None]
    def start_response(status, headers):
        status_headers[0] = status
        status_headers[1] = headers

    # Call your original WSGI app, which returns an iterable (a tuple) of byte strings
    response_iterable = wsgi_app(environ, start_response)
    
    # Join the byte strings from the iterable to get the full response body
    response_body = b"".join(response_iterable)

    # Create a proper Flask Response object with the body, status, and headers
    response = Response(
        response=response_body,
        status=status_headers[0],
        headers=status_headers[1]
    )

    # Return the valid Flask Response object
    return response

# This is the entry point Vercel will use. The name 'app' is the default.
if __name__ == "__main__":
    app.run(debug=True)