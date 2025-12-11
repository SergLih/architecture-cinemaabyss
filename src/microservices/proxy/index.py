import os
import random
from flask import Flask, request, Response
import requests
from urllib.parse import urlparse

app = Flask(__name__)

PORT = int(os.getenv('PORT', '8000'))
MONOLITH_URL = os.getenv('MONOLITH_URL')
MOVIES_SERVICE_URL = os.getenv('MOVIES_SERVICE_URL')
GRADUAL_MIGRATION = os.getenv('GRADUAL_MIGRATION', 'false').lower() == 'true'
MOVIES_MIGRATION_PERCENT = int(os.getenv('MOVIES_MIGRATION_PERCENT', '0'))

if not MONOLITH_URL:
    raise RuntimeError('MONOLITH_URL environment variable is required')
if GRADUAL_MIGRATION and not MOVIES_SERVICE_URL:
    raise RuntimeError('MOVIES_SERVICE_URL environment variable is required when GRADUAL_MIGRATION is true')

def proxy_request(target_url):
    path = request.full_path
    if path.startswith('/'):
        path = path[1:]
    url = f"{target_url.rstrip('/')}/{path}"

    method = request.method
    headers = {key: value for key, value in request.headers if key != 'Host'}
    data = request.get_data()
    params = request.args

    response = requests.request(method, url, headers=headers, params=params, data=data, allow_redirects=False)

    excluded_headers = ['content-encoding', 'content-length', 'transfer-encoding', 'connection']
    headers = [(name, value) for (name, value) in response.headers.items() if name.lower() not in excluded_headers]
    return Response(response.content, status=response.status_code, headers=headers)

@app.route('/api/movies/health', methods=['GET', 'POST', 'PUT', 'DELETE', 'PATCH'])
@app.route('/api/movies', methods=['GET', 'POST', 'PUT', 'DELETE', 'PATCH'])
def handle_movies():
    if not GRADUAL_MIGRATION:
        target = MONOLITH_URL
    else:
        percent = random.uniform(0, 100)
        if percent < MOVIES_MIGRATION_PERCENT:
            target = MOVIES_SERVICE_URL
        else:
            target = MONOLITH_URL
    return proxy_request(target)

@app.route('/', defaults={'path': ''})
@app.route('/<path:path>')
def catch_all(path):
    return proxy_request(MONOLITH_URL)