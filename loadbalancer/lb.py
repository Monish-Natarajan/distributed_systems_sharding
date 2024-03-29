from quart import Quart, jsonify, redirect
from quart import request, Response
import httpx
import random
import time
import subprocess
import sys
import string
import asyncio
import logging
from consistent_hasher import ConsistentHashing

lock = asyncio.Lock()

def generate_hostname(n):
    # random hostname generated of len < n
    return "server_" + ''.join(random.choices(string.ascii_lowercase, k=random.randint(4, n)))


app = Quart(__name__)

logging.getLogger('werkzeug').disabled = True


@app.route('/rep', methods=['GET'])
def rep():
    # Get the list of servers from the consistent hashing object
    server_hostnames = ch.get_servers()

    # Create the response data
    data = {
        'response': {
            'message': {
                "N": len(server_hostnames),
                "replicas": str(list(server_hostnames))
            },
            'status': "successful"
        }
    }
    return jsonify(data), 200



@app.route('/add', methods=['POST'])
async def add():
    print("Received add request")

    json_data = await request.json

    n = json_data['n']
    hostnames = set(json_data['hostnames'])

    # handling error cases
    if len(hostnames) > n:
        data = {
            'message': "<Error> Length of hostname list is more than newly added instances",
            'status': "failure"
        }
        return jsonify(data), 400

    while len(hostnames) < n:
        # append random hostnames of length <= 10
        hostnames.add(generate_hostname(10))

        # add the requested servers
    for hostname in hostnames:
        res = spawn_container(hostname)

        if res == "success":
            try:
                ch.add_server(hostname)
                print(f"Succesfully added server {hostname}")
            except Exception as e:
                print(f"An error occurred while adding server {hostname}: {e}")
                ch.remove_server(hostname)

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

    n = json_data['n']
    hostnames = set(json_data['hostnames'])

    # handling error cases
    if len(hostnames) > n:
        data = {
            'message': "<Error>  Length of hostname list is more than removable instances",
            'status': "failure"
        }
        return jsonify(data), 400

    # ensure hostnames are valid
    # is every hostname in the hostnames set in the ring?
    server_list = ch.get_servers()
    for hostname in hostnames:
        if hostname not in server_list:
            data = {
                'message': f"<Error>  Hostname {hostname} is not in the ring",
                'status': "failure"
            }
            return jsonify(data), 400

    # handle incomplete list case
    # we need to delete more servers if len(hostnames) < n
    # select random servers to delete from the pool of existing servers
    while len(hostnames) < n:
        # choose a random server that's not already in hostnames
        while True:
            hostname = random.choice(server_list)
            if hostname not in hostnames:
                break
        hostnames.add(hostname)

    assert len(hostnames) == n

    # remove the requested servers
    for hostname in hostnames:
        try:
            ch.remove_server(hostname)
            print(f"Succesfully removed server {hostname} from hash ring")
        except Exception as e:
            print(f"An error occurred while removing server {hostname} from hash ring: {e}")
        # now that we have removed the server from the hash ring, we can stop the container
        res = remove_container(hostname)

        if res == "success":
            print(f"Successfully removed server {hostname} container")

        else:
            print(f"Unable to remove server {hostname} container")

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

    request_id = random.randint(100000, 1000000)  # random id temporarily

    # Get the nearest server for the request
    nearest_server = ch.get_nearest_server(request_id)
    if nearest_server == '':
        print("Couldn't route request: No servers available")
        return "No servers available", 503
    # Forward the request to the nearest server
    try:
        async with httpx.AsyncClient() as client:
            response = await client.get(f"http://{nearest_server}:8080/{path}")
    except:
        print(f"Server {nearest_server} didn't respond")
        # don't handle this failure multiple times, only once
        # Need to use mutex lock to ensure this
        # handle each failure one at a time
        async with lock:
            if nearest_server in ch.get_servers():
                await handle_failure(nearest_server)

        # access request object
        url = request.url
        return redirect(url, code=307)  # let's try again, since we have handled the failure

    # Return the response from the server
    return response.content, response.status_code


def spawn_container(hostname):
    server_image = 'server'
    command = f'docker run --rm --name {hostname} --network mynet --network-alias {hostname} -e HOSTNAME={hostname} -d {server_image}'
    try:
        result = subprocess.run(command, shell=True, capture_output=True, text=True)
    except Exception as e:
        print(f"An error occurred while trying to start server container {hostname}: {e}")
        return "failure"

    if result.returncode != 0:
        print("Unable to start server container")
        print("Error:", result.stderr)
        return "failure"
    else:
        print("Successfully started server container with hostname ", hostname)
        return "success"


def remove_container(node_name):
    command = f'docker rm --force {node_name}'
    result = subprocess.run(command, shell=True, capture_output=True, text=True)

    if result.returncode != 0:
        print("Unable to removed server container")
        print("Error:", result.stderr)
        return "failure"
    else:
        print("Successfully removed server container")
        print("Output:", result.stdout)
        return "success"


# checks if any of the servers are down, is called when a server doesn't respond
async def handle_failure(hostname):
    # if no hostname provided, error out
    if hostname is None:
        raise Exception("Fatal: No hostname provided to handle_failure()")

    # remove hostname temporarily so that other requests can be distributed to other servers
    ch.remove_server(hostname)
    timeout = httpx.Timeout(5.0, read=5.0)

    is_alive = False
    for _ in range(3):  # retry 3 times
        try:
            async with httpx.AsyncClient(timeout=timeout) as client:
                response = await client.get(f"http://{hostname}:8080/heartbeat")
            if response.status_code == 200:
                is_alive = True
                break  # server is alive
        except Exception as e:
            # ignore exceptions and try again
            print(
                f"handle_failure(): Exception raised while trying to check for liveliness of server {hostname}, "
                f"trying again: {e}")

    if is_alive:
        # server is alive, add it back to the ring
        ch.add_server(hostname)
        return
    else:
        # server is dead, spawn a new server to replace it
        print(f"handle_failure(): Server {hostname} is down!")

        # spawn new server with a random hostname
        new_hostname = hostname

        # keep trying to generate a hostname that's not already in use
        # why did I use ch.get_servers() here? Just in case an admin command is executed while this function is running
        while new_hostname in ch.get_servers():
            new_hostname = generate_hostname(10)

        res = spawn_container(new_hostname)
        time.sleep(1)

        if res == "success":
            try:
                ch.add_server(new_hostname)
                print(f"Succesfully added server {new_hostname} to replace {hostname}")
            except Exception as e:
                print(f"An error occurred while adding server {new_hostname} to replace {hostname}: {e}")
                ch.remove_server(new_hostname)
        else:
            print(f"Unable to add server {new_hostname}!")


ch = ConsistentHashing()
app.run(host='0.0.0.0', port=5001)

# add 2 servers
ch.add_server("server_1")

