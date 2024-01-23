from quart import Quart, jsonify
from quart import request, Response
import httpx
import random
import sys
from consistent_hash import ConsistentHashing, RequestNode, ServerNode

app = Quart(__name__)


@app.route('/rep', methods=['GET'])
def rep():
    # Get the list of servers from the consistent hashing object
    servers = ch.get_servers()
    server_hostnames = [server.hostname for server in servers]

    # Create the response data
    data = {
        'Response': {
            'message': {
                "N": len(servers),
                "replicas" : server_hostnames
            },
            'status': "successful"
        }
    }
    return jsonify(data), 200


# to be done
@app.route('/add', methods=['POST'])
def add():
    pass


# to be done
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


# FOR TESTING ONLY
@app.route('/test', methods=['GET'])
def test():
    return "lb is alive", 200


# returns  "204 No Content" for favicon.ico
@app.route('/favicon.ico')
def favicon():
    return Response(status=204)


# forwarding requests to the nearest server
@app.route('/<path>', methods=['GET'])
async def home(path):

    if path != "home":
        data = {
            'Response': {
                'message': f"<Error> ’/{path}’ endpoint does not exist in server replicas",
                'status': "failure"
            }
        }
        return jsonify(data), 400

    # Create a new RequestNode for the incoming request
    request_id = random.randint(0, sys.maxsize) # random id temporarily
    request_node = RequestNode(request_id)

    # Get the nearest server for the request
    nearest_server = ch.get_nearest_server(request_node)
    
    if nearest_server is None:
        return "No servers available", 503
    else:
        server_hostname = nearest_server.hostname
        # Forward the request to the nearest server
        async with httpx.AsyncClient() as client:
            response = await client.get(f"http://{server_hostname}:8080/{path}")

    # Return the response from the server
    return response.content, response.status_code


if __name__ == '__main__':
    
    # initialize the load balancer with 3 servers
    N = 3
    server_ids = [1,2,3]
    server_hostnames = {1: "server1", 2: "server2", 3: "server3"}

    ch = ConsistentHashing()

    for i in range(N):
        server_id = server_ids[i]
        server_hostname = server_hostnames[server_id]
        server_node = ServerNode(server_id, server_hostname)
        ch.add_server(server_node)

    app.run(host='0.0.0.0', port=5000)