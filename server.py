import time

import flask

from flask import request, jsonify, send_from_directory
from werkzeug.exceptions import NotFound, BadRequest
from flask_cors import CORS
from jsonschema import ValidationError
from waitress import serve


from setup import creds
from setup.creds import API
from setup.error_handler import ProcessInErrorHandler, ProcessOutErrorHandler, Logger

from traceback import format_exc as tb

from routes import shopify
from routes import marketing
from routes import inventory
from routes import sms
from routes.limiter import limiter

app = flask.Flask(__name__)

# Rate Limiting
limiter.init_app(app)


# Register API Route Blueprints
app.register_blueprint(shopify.shopify_routes)  # Shopify Routes
app.register_blueprint(marketing.marketing_routes)  # Marketing Routes
app.register_blueprint(inventory.availability_routes)  # Inventory Availability Routes
app.register_blueprint(sms.sms_routes)  # SMS Routes

CORS(app)

dev = True  # When False, app is served by Waitress


@app.before_request
def log_request():
    """Log incoming requests."""
    logger = Logger(log_directory=creds.Logs.server)
    logger.info(f'{request.method} - {request.url}')


# Error handling functions
@app.errorhandler(ValidationError)
def handle_validation_error(e):
    # Return a JSON response with a message indicating that the input data is invalid
    ProcessInErrorHandler.error_handler.add_error_v(
        error=f'Invalid input data: {e}', origin='validation_error', traceback=tb()
    )
    return jsonify({'error': 'Invalid input data'}), 400


@app.errorhandler(Exception)
def handle_exception(e):
    url = request.url
    # Return a JSON response with a generic error message
    ProcessInErrorHandler.error_handler.add_error_v(
        error=f'An error occurred: {e}', origin=f'exception - {url}', traceback=tb()
    )
    return jsonify({'error': 'Internal Server Error'}), 500


@app.route(f'{API.Route.file_server}/<path:path>', methods=['GET'])
@limiter.limit(creds.API.default_rate)
def serve_file(path):
    try:
        return send_from_directory(creds.API.public_files_local_path, path)
    except NotFound:
        return jsonify({'error': 'File not found'}), 404
    except BadRequest:
        return jsonify({'error': 'Bad request'}), 400
    except Exception as e:
        ProcessOutErrorHandler.error_handler.add_error_v(
            error=f'Error serving file: {e}', origin=API.Route.file_server, traceback=tb()
        )
        return jsonify({'error': 'Internal server error'}), 500


@app.route('/robots.txt', methods=['GET'])
def robots():
    return (
        """
    User-agent: *
    Disallow: /
    """,
        200,
    )


@app.route('/favicon.ico', methods=['GET'])
def favicon():
    return send_from_directory(creds.API.public_files_local_path, 'favicon.ico')


@app.route('/', methods=['GET'])
def index():
    return jsonify({'status': 'Server is running'}), 200


if __name__ == '__main__':
    # if dev:
    #     app.run(debug=False, port=API.port)
    # else:
    #     running = True
    #     while running:
    #         try:
    #             print('Flask Server Running')
    #             print(f'Host: localhost:{API.port}')
    #             serve(
    #                 app,
    #                 host='localhost',
    #                 port=API.port,
    #                 threads=8,
    #                 max_request_body_size=1073741824,  # 1 GB
    #                 max_request_header_size=8192,  # 8 KB
    #                 connection_limit=1000,
    #             )
    #         except Exception as e:
    #             print('Error serving Flask app: ', e)
    #             ProcessInErrorHandler.error_handler.add_error_v(
    #                 error=f'Error serving Flask app: {e}', origin='server', traceback=tb()
    #             )
    #             time.sleep(5)
    #         # Stop the server if Keyboard Interrupt
    #         running = False
    #         print('Flask Server Stopped')
    app.run(debug=False, port=API.port)
