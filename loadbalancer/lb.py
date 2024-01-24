from quart import Quart, jsonify
from quart import request, Response
import httpx
import random
import subprocess
import sys
import string
import os
from consistent_hash import ConsistentHashing, RequestNode, ServerNode

app = Quart(__name__)
next_port_no = 18084

@app.route('/rep', methods=['GET'])
def rep():
    # Get the list of servers from the consistent hashing object
    servers = ch.get_servers()
    server_hostnames = [server.hostname for server in servers]

    # Create the response data
    data = {
            'message': {
                "N": len(servers),
                "replicas" : server_hostnames
            },
            'status': "successful"
        }
    return jsonify(data), 200


# TODO : handle error cases
@app.route('/add', methods=['POST'])
async def add():

    print("Received add request")

    json_data = await request.json
    print("Request data:", json_data)

    n = json_data['n']
    hostnames = json_data['hostnames']

    # handling error cases
    if len(hostnames) > n:
        data = {
                'message': "<Error> Length of hostname list is more than newly added instances",
                'status': "failure"
        }
        return jsonify(data), 400

    while len(hostnames) < n:
        # append random hostnames of length <= 10
        hostnames.append(''.join(random.choices(string.ascii_lowercase, k = random.randint(1, 10))))    

    # add the requested servers
    for hostname in hostnames:
        res = spawn_server(hostname, hostname, 'myserver')
        
        if res == "success":
            try:    
                server_id = len(ch.get_servers()) + 1
                server_node = ServerNode(server_id, hostname)
                ch.add_server(server_node)
                print(f"Succesfully added server {hostname} with id {server_id}")
            except Exception as e:
                print(f"An error occurred while adding server {hostname} with id {server_id}: {e}")
          
        else:
            print(f"Unable to add server {hostname}")
            # return error response
            data = {
                    'message': f"<Error> Unable to add server {hostname}",
                    'status': "failure"
            }
            return jsonify(data), 400

    return rep()

# TODO : handle error cases
@app.route('/rm', methods=['POST'])
async def rem():

    print("Received remove request")

    json_data = await request.json
    print("Request data:", json_data)

    n = json_data['n']
    hostnames = json_data['hostnames']

    # handling error cases
    if len(hostnames) > n:
        data = {
                'message': "<Error>  Length of hostname list is more than removable instances",
                'status': "failure"
        }
        return jsonify(data), 400
    
    # ensure hostnames are valid

    # handle clashes and incomplete list cases
    pass

    assert len(hostnames) == n

    # remove the requested servers
    for hostname in hostnames:
        res = remove_server(hostname)
        
        if res == "success":
            try:
                ch.remove_server(hostname)
                print(f"Succesfully removed server {hostname} with id {server_id}")
            except Exception as e:
                print(f"An error occurred while removing server {hostname} with id {server_id}: {e}")
        
        else:
            print(f"Unable to remove server {hostname}")
            # return error response
            data = {
                    'message': f"<Error> Unable to remove server {hostname}",
                    'status': "failure"
            }
            return jsonify(data), 400

    return rep()


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


def spawn_server(node_name, host_name, server_image):
    global next_port_no
    command = f'docker run --name {node_name} --network mynet --network-alias {host_name} -e HOSTNAME=host_name -e SERVER_IDENTIFIER={node_name} -p {next_port_no}:8080 -d {server_image}'
    result = subprocess.run(command, shell=True, capture_output=True, text=True)
    next_port_no += 1

    if result.returncode != 0:
        print("Unable to start server container")
        print("Error:", result.stderr)
        return "failure"
    else:
        print("Successfully started server container")
        print("Output:", result.stdout)
        return "success"

def remove_server(node_name):
    command = f'docker stop {node_name} && docker rm {node_name}'
    result = subprocess.run(command, shell=True, capture_output=True, text=True)

    if result.returncode != 0:
        print("Unable to removed server container")
        print("Error:", result.stderr)
        return "failure"
    else:
        print("Successfully removed server container")
        print("Output:", result.stdout)
        return "success"

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

    app.run(host='0.0.0.0', port=5001)