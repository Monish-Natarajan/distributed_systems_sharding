from quart import Quart, jsonify
import sys
import os

app = Quart(__name__)

# Unique identifier for the server
# This will be the server's hostname
server_identifier = os.environ['HOSTNAME']

@app.route('/home', methods=['GET'])
async def home():
    data = {
        'response': {
            'message': 'Hello from server number: {}'.format(server_identifier),
            'status': 'successful',
        }
    }
    status_code = 200

    # Return the JSON response with status code
    return jsonify(data), status_code


@app.route('/heartbeat', methods=['GET'])
def heartbeat():
    # Send an empty response with status code 200
    return '', 200

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8080)