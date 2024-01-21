from flask import Flask, jsonify
import sys

app = Flask(__name__)

@app.route('/rep', methods=['GET'])
def rep():
    data = {
        'Response': {
            'message': {
                "N": 3,
                "replicas" : ["Server 1", "Server 2", "Server 3"]
            },
            'status': "successful"
        }
    }
    status_code = 200

    # Return the JSON response with status code
    return jsonify(data), status_code

#incomplete
@app.route('/add', methods=['POST'])
def add():
    data = {
        'payload': {
            "n" : 4,
            "replicas" : ["S5", "S6", "S7", "S8"]
        },
        'response': {
            'message': {
                "N": 7,
                "replicas" : ["Server 1", "Server 2", "Server 3", "S5", "S6", "S7", "S8"]
            },
            'status': "successful"
        }
    }
    status_code = 200

    # Return the JSON response with status code
    return jsonify(data), status_code


@app.route('/rm', methods=['DELETE'])
def rem():
    data = {
        'payload': {
            "n" : 3,
            "replicas" : ["S5", "S6"]
        },
        'response': {
            'message': {
                "N": 4,
                "replicas" : ["Server 1", "Server 3", "S7", "S8"]
            },
            'status': "successful"
        }
    }
    status_code = 200

    # Return the JSON response with status code
    return jsonify(data), status_code


@app.route('/<path>',method=['GET'])
def home(path):
    data = {
        'message': "<Error> \'{}\' endpoint does not exist in server replicas".format(path),
        'status': "failure"
    }
    status_code = 400

    # Return the JSON response with status code
    return jsonify(data), status_code

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)