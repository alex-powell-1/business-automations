from flask import Blueprint, request, jsonify
from traceback import format_exc as tb
import requests
import time
from routes.limiter import limiter
from setup import creds, authorization
from setup.creds import API
from setup.error_handler import ProcessInErrorHandler


availability_routes = Blueprint('availability_routes', __name__)


@availability_routes.route(API.Route.token, methods=['POST'])
@limiter.limit('10/minute')  # 10 requests per minute
def get_token():
    password = request.args.get('password')

    if password.lower() == creds.Company.commercial_availability_pw:
        session = authorization.Session(password)
        authorization.SESSIONS.append(session)
        return jsonify({'token': session.token, 'expires': session.expires}), 200

    ProcessInErrorHandler.error_handler.add_error_v(error=f'Invalid password: {password} ', origin=API.Route.token)
    return jsonify({'error': 'Invalid username or password'}), 401


@availability_routes.route(API.Route.commercial_availability, methods=['POST'])
@limiter.limit('10/minute')  # 10 requests per minute
def get_commercial_availability():
    token = request.args.get('token')

    session = next((s for s in authorization.SESSIONS if s.token == token), None)

    if not session or session.expires < time.time():
        authorization.SESSIONS = [s for s in authorization.SESSIONS if s.token != token]
        return jsonify({'error': 'Invalid token'}), 401

    response = requests.get(creds.Company.commercial_inventory_csv)
    if response.status_code == 200:
        return jsonify({'data': response.text}), 200
    else:
        ProcessInErrorHandler.error_handler.add_error_v(
            error='Error fetching data', origin=API.Route.commercial_availability, traceback=tb()
        )
        return jsonify({'error': 'Error fetching data'}), 500


@availability_routes.route(API.Route.retail_availability, methods=['POST'])
@limiter.limit('10/minute')  # 10 requests per minute
def get_availability():
    response = requests.get(creds.Company.retail_inventory_csv)
    if response.status_code == 200:
        return jsonify({'data': response.text}), 200
    else:
        ProcessInErrorHandler.error_handler.add_error_v(
            error='Error fetching data', origin=API.Route.retail_availability
        )
        return jsonify({'error': 'Error fetching data'}), 500
