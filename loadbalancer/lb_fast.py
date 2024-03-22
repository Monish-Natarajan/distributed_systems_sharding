from fastapi import FastAPI, Request, HTTPException
from fastapi.responses import JSONResponse, RedirectResponse
import uvicorn

import httpx
import random
import time
import subprocess
import string
import asyncio
import log
import database
from typing import List, Dict
from request_models import InitRequest, AdminRequest, ReadRequest, WriteRequest, AddRequest, RemoveRequest
from database import ShardRecord, MapRecord

from consistent_hasher import ConsistentHashing

def generate_hostname(n):
    # random hostname generated of len < n
    return "server_" + ''.join(random.choices(string.ascii_lowercase, k=random.randint(4, n)))

class ShardData:
    def __init__(self, shard_id):
        self.shard_id: str = shard_id
        self.ch: ConsistentHashing = ConsistentHashing()
        # mutex lock for write operations, typed as asyncio.Lock
        self.writeLock: asyncio.Lock = asyncio.Lock()

app = FastAPI()
chLock = asyncio.Lock()
ch = ConsistentHashing()
shardDataMap: Dict[str, ShardData] = {} # maps shard_id to a tuple containing consistent hashing object and mutex lock
#logging.getLogger('werkzeug').disabled = True


# initializes the load balancer and spawns servers
@app.post('/init')
async def initalize_loadbalancer(request_body: InitRequest):
    # insert into ShardT
    try:
        for shard in request_body.shards:
            record = ShardRecord(Stud_id_low=shard['Stud_id_low'], Shard_id=shard['Shard_id'], Shard_size=shard['Shard_size'], valid_idx=1)
            database.insert_shard_record(record)

        # insert into MapT
        for server, shards in request_body.servers.items():
            for shard in shards:
                record = MapRecord(Shard_id=shard, Server_id=server)
                database.insert_map_record(record)
    
    except Exception as e:

        # undo state changes, TBD

        data = {
            'message': f"<Error> An error occurred while initializing the load balancer: {e}",
            'status': "failure"
        }
        raise HTTPException(status_code=400, detail=data)

    # errors to handle
        # mismatch between shard_name in request_body.shards and request_body.servers
            
    # initialize the consistent hashing ring for each shard
    for shard in request_body.shards:
        shard_id = shard['Shard_id']
        shardDataMap[shard_id] = ShardData(shard_id)

    # spawn servers and add them to hashing ring of appropriate shards
    try:
        for server, shards in request_body.servers.items():
            # spawn server container
            res = spawn_container(server)
            
            if res == "success":
                # add server to the consistent hashing ring
                for shard in shards:
                    shardDataMap[shard].ch.add_server(server)
                    print(f"Succesfully added server {server} to hash ring of shard {shard}")
    
    except Exception as e:
        
        # undo state changes, TBD

        data = {
            'message': f"<Error> An error occurred while spawning servers: {e}",
            'status': "failure"
        }
        raise HTTPException(status_code=400, detail=data)
    
    reponse_data = {
        'message': "Configured database",
        'status': "success"
    }
    
    return JSONResponse(content=reponse_data, status_code=200)

@app.post('/add')
async def add_servers(request_body: AddRequest):
    # add the new shards
    try:
        for shard in request_body.new_shards:
            record = ShardRecord(Stud_id_low=shard['Stud_id_low'], Shard_id=shard['Shard_id'], Shard_size=shard['Shard_size'], valid_idx=1)
            database.insert_shard_record(record)
            shardDataMap[shard['Shard_id']] = ShardData(shard['Shard_id'])

        # add the new servers
        new_server_names = request_body.servers.keys()
        for server_name in new_server_names:
            res = spawn_container(server_name)
            if res == "success":
                for shard in request_body.servers[server_name]:
                    shardDataMap[shard].ch.add_server(server_name)
            else:
                print(f"Unable to add server {server_name}")
    except Exception as e:
        data = {
            'message': f"<Error> An error occurred while adding new servers: {e}",
            'status': "failure"
        }
        raise HTTPException(status_code=400, detail=data)

    # get total number of servers from MapT
    unique_servers = database.get_unique_servers()
    num_servers = len(unique_servers)

    # message = 'Added Server:<name1>, Server:<name2>, ...'
    message = 'Added Servers: ' + ', '.join(new_server_names)

    repsonse_data = {
        'N': num_servers,
        'message': message,
        'status': "success"
    }

    return JSONResponse(content=repsonse_data, status_code=200)


@app.delete('/rm')
async def remove_servers(request_body: RemoveRequest):
    # remove the servers
    try:
        num_remove = request_body.n
        server_names = request_body.servers

        # get server list from MapT
        unique_servers = database.get_unique_servers()
        num_existing_servers = len(unique_servers)
        
        # create set of all servers
        all_servers = set()
        for server in unique_servers:
            all_servers.add(server[0])     

        if num_remove > num_existing_servers:
            data = {
                'message': f"<Error> Number of servers to remove is more than the existing servers",
                'status': "failure"
            }
            raise HTTPException(status_code=400, detail=data)

        if num_remove > len(server_names):
            # select random servers to delete from the pool of existing servers
            candidate_servers = list(all_servers - set(server_names))
            server_names += random.sample(candidate_servers, num_remove - len(server_names))

        for server_name in server_names:
            # remove server from the consistent hashing ring
            for shard in shardDataMap.keys():
                shardDataMap[shard].ch.remove_server(server_name)
            
            # remove server from the MapT table
            database.delete_map_server(server_name)
            
            # remove server container
            res = remove_container(server_name)
            if res != "success":
                print(f"Unable to remove server {server_name}")
    
    except Exception as e:
        data = {
            'message': f"<Error> An error occurred while removing servers: {e}",
            'status': "failure"
        }
        raise HTTPException(status_code=400, detail=data)

    # get total number of servers from MapT
    unique_servers = database.get_unique_servers()
    num_servers = len(unique_servers)

    # message = 'Removed Server:<name1>, Server:<name2>, ...'
    message = 'Removed Servers: ' + ', '.join(server_names)

    repsonse_data = {
        'N': num_servers,
        'message': message,
        'status': "success"
    }

    return JSONResponse(content=repsonse_data, status_code=200)


# function for reading low to high student IDs from a shard
async def read_from_shard(shard_id: str, low: int, high: int):
    chRing = shardDataMap[shard_id].ch
    # generate random 6 digit number
    request_id = random.randint(100000, 1000000)
    # get the nearest server for the request
    nearest_server = chRing.get_nearest_server(request_id)
    if nearest_server == '':
        log("Couldn't route request: No servers available")
        return JSONResponse(content="No servers available", status_code=503)
    # Forward the request to the nearest server
    try:
        async with httpx.AsyncClient() as client:
            request = {
                "shard": shard_id,
                "Stud_id": {
                    "low": low,
                    "high": high
                }
            }
            response = (await client.post(f"http://{nearest_server}:8080/read", json=request)).json()
            
            if response['status'] == "success":
                return response['data']
            else:
                raise Exception(f"read_from_shard() failed for shard {shard_id} with status {response['status']}")
    except Exception as e:
        log("Error while reading from shard: ", e)


def get_shard_id(student_id: int):
    all_shards = database.get_shards()
    for shard in all_shards:
        lowerEnd = int(shard[0])
        upperEnd = lowerEnd + int(shard[2]) - 1
        if student_id >= lowerEnd and student_id <= upperEnd:
            return shard[1]
    return None

@app.post('/read')
async def read(request_body: ReadRequest):
    low = request_body.Stud_id['low']
    high = request_body.Stud_id['high']
    # check which shard to read low from
    all_shards = database.get_shards() # List[tuples] sorted in ascending order of Stud_id_low
    shards_queried = []
    records = []
    for shard in all_shards:
        lowerEnd = int(shard[0])
        upperEnd = lowerEnd + int(shard[2]) - 1
        if low >= lowerEnd and low <= upperEnd:
            # read the relevant records from this shard
            records += read_from_shard(shard[1], low, min(high, upperEnd))
            shards_queried.append(shard[1])
            low = upperEnd + 1
        if low > high:
            break
    response = {
        "shards_queried": shards_queried,
        "data": records,
        "status": "success"
    }


@app.post('/write')
async def write(request_body: WriteRequest):
    data = request_body.data
    # check which shard to write to
    all_shards = database.get_shards()
    # Group the writes by shard
    # shard -> List of records to write to that shard
    shardWriteMap: Dict[str, List] = {}
    for record in data:
        for shard in all_shards:
            lowerEnd = int(shard[0])
            upperEnd = lowerEnd + int(shard[2]) - 1
            if record.Stud_id >= lowerEnd and record.Stud_id <= upperEnd:
                if shard[1] not in shardWriteMap:
                    shardWriteMap[shard[1]] = []
                shardWriteMap[shard[1]].append(record)
                break
    # For each shard that will be written to, take mutex lock for that shardId

    shardsWritten: List[str] = [] 
    for shard_id, records in shardWriteMap.items():
        try:
            chRing = shardDataMap[shard_id].ch
            writeLock = shardDataMap[shard_id].writeLock
            with writeLock:
                # get all servers that have this shard
                servers = chRing.get_servers()

                request = {
                    "shard": shard_id,
                    "curr_idx": 0,
                    "data": records
                }

                # perform the write on all servers
                serversWritten: List[str] = []
                for idx, server in enumerate(servers):
                    try:
                        with httpx.AsyncClient() as client:
                            response = (await client.post(f"http://{server}:8080/write", json=request)).json()
                            if response['status'] != "success":
                                raise Exception(f"write() failed for shard {shard_id} with status {response['status']}")
                            serversWritten.append(server)
                    except Exception as e:
                        log(f"Error while writing to server {server}: ", e)
                        log("Rolling back writes to other servers in the shard")
                        for server in serversWritten:
                            with httpx.AsyncClient() as client:
                                for record in records:
                                    request = {
                                        "shard": shard_id,
                                        "Stud_id": record['Stud_id']
                                    }
                                    response = (await client.post(f"http://{server}:8080/del", json=request)).json()
                                    if response['status'] != "success":
                                        log(f"Failed to rollback write to server {server}")
                                        log(f"It is what it is. Cannot guarantee consistency of writes anymore. Shutting down load balancer")
                                        raise Exception("Fatal -- Failed to rollback write")
                        raise e
            # if we have reached this, it means all writes to this shard were successful
            shardsWritten.append(shard_id)
        except e as Exception:
            log(f"Error while writing to shard {shard_id}: ", e)
            log("Rolling back writes to other shards")
            for shard_id in shardsWritten:
                writeLock = shardDataMap[shard_id].writeLock
                chRing = shardDataMap[shard_id].ch
                with writeLock:
                    servers = chRing.get_servers()
                    for server in servers:
                        with httpx.AsyncClient() as client:
                            for record in shardWriteMap[shard_id]:
                                request = {
                                    "shard": shard_id,
                                    "Stud_id": record['Stud_id']
                                }
                                response = (await client.post(f"http://{server}:8080/del", json=request)).json()
                                if response['status'] != "success":
                                    log(f"Failed to rollback write to server {server}")
                                    log(f"It is what it is. Cannot guarantee consistency of writes anymore. Shutting down load balancer")
                                    raise Exception("Fatal -- Failed to rollback write")
        # if we have reached this, it means all writes to this shard were successful
        shardsWritten.append(shard_id)
    return JSONResponse(
        content={
            "message": f"Successfully performed {data.size()} writes",
            "status": "success"
        },
        status_code=200
    )

async def modify_record(request_body):
    is_update: bool = (type(request_body) is UpdateRequest)
    shard_id = get_shard_id(request_body.Stud_id)
    if shard_id is None:
        return JSONResponse(
            content={
                "message": "Student ID not found",
                "status": "failure"
            },
            status_code=404
        )
    # get the current record for this student id
    oldRecord = (await read_from_shard(shard_id, request_body.Stud_id, request_body.Stud_id))[0]
    # update every server that contains this shard
    chRing = shardDataMap[shard_id].ch
    writeLock = shardDataMap[shard_id].writeLock
    with writeLock:
        servers = chRing.get_servers()
        if is_update:
            request = {
                "shard": shard_id,
                "Stud_id": request_body.Stud_id,
                "data": request_body.data
            }
        else:
            request = {
                "shard": shard_id,
                "Stud_id": request_body.Stud_id
            }
        serversUpdated: List[str] = []
        for server in servers:
            try:
                with httpx.AsyncClient() as client:
                    if is_update:
                        response = (await client.put(f"http://{server}:8080/update", json=request)).json()
                    else:
                        response = (await client.delete(f"http://{server}:8080/del", json=request)).json()
                    if response['status'] != "success":
                        raise Exception(f"update() failed for shard {shard_id} with status {response['status']}")
                    serversUpdated.append(server)
            except Exception as e:
                log(f"Error while updating server {server}: ", e)
                log("Rolling back updates to other servers in the shard")
                for server in serversUpdated:
                    with httpx.AsyncClient() as client:
                        if is_update:
                            request = {
                                "shard": shard_id,
                                "Stud_id": request_body.Stud_id,
                                "data": oldRecord
                            }
                            response = (await client.put(f"http://{server}:8080/update", json=request)).json()
                        else:
                            request = {
                                "shard": shard_id,
                                "curr_idx": 0,
                                "data": [oldRecord]
                            }
                            response = (await client.post(f"http://{server}:8080/write", json=request)).json()
                        if response['status'] != "success":
                            log(f"Failed to rollback update to server {server}")
                            log(f"It is what it is. Cannot guarantee consistency of updates anymore. Shutting down load balancer")
                            raise Exception("Fatal -- Failed to rollback update")
                return JSONResponse(
                    content={
                        "message": "Failed to update record",
                        "status": "failure"
                    },
                    status_code=500
                )

    return JSONResponse(
        content={
            "message": "Successfully updated record",
            "status": "success"
        },
        status_code=200
    )

@app.put('/update')
async def update(request_body: UpdateRequest):
    return await modify_record(request_body)

@app.delete('/del')
async def delete(request_body: DeleteRequest):
    return await modify_record(request_body)

@app.get('/rep')
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
    return JSONResponse(content=data, status_code=200)


@app.post('/add')
async def add(request_body: AdminRequest):
    with chLock: # ensure 
        log("Received add request")

        n = request_body.n
        hostnames = set(request_body.hostnames)

        # handling error cases
        if len(hostnames) > n:
            data = {
                'message': "<Error> Length of hostname list is more than newly added instances",
                'status': "failure"
            }
            raise HTTPException(status_code=400, detail=data)

        while len(hostnames) < n:
            # append random hostnames of length <= 10
            hostnames.add(generate_hostname(10))

            # add the requested servers
        for hostname in hostnames:
            res = spawn_container(hostname)

            if res == "success":
                try:
                    ch.add_server(hostname)
                    log(f"Succesfully added server {hostname}")
                except Exception as e:
                    log(f"An error occurred while adding server {hostname}: {e}")
                    ch.remove_server(hostname)

            else:
                log(f"Unable to add server {hostname}")
                # return error response
                data = {
                    'message': f"<Error> Unable to add server {hostname}",
                    'status': "failure"
                }
                raise HTTPException(status_code=400, detail=data)

        return rep()


# TODO : handle error cases
@app.delete('/rm')
async def rem(request_body: AdminRequest):
    log("Received remove request")

    n = request_body.n
    hostnames = set(request_body.hostnames)

    # handling error cases
    if len(hostnames) > n:
        data = {
            'message': "<Error>  Length of hostname list is more than removable instances",
            'status': "failure"
        }
        raise HTTPException(status_code=400, detail=data)

    # ensure hostnames are valid
    # is every hostname in the hostnames set in the ring?
    server_list = ch.get_servers()
    for hostname in hostnames:
        if hostname not in server_list:
            data = {
                'message': f"<Error>  Hostname {hostname} is not in the ring",
                'status': "failure"
            }
            raise HTTPException(status_code=400, detail=data)

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
            log(f"Succesfully removed server {hostname} from hash ring")
        except Exception as e:
            log(f"An error occurred while removing server {hostname} from hash ring: {e}")
        # now that we have removed the server from the hash ring, we can stop the container
        res = remove_container(hostname)

        if res == "success":
            log(f"Successfully removed server {hostname} container")

        else:
            log(f"Unable to remove server {hostname} container")

    return rep()


# FOR TESTING ONLY
@app.get('/test')
def test():
    return JSONResponse(content="Test successful", status_code=200)


# returns  "204 No Content" for favicon.ico
@app.get('/favicon.ico')
def favicon():
    return JSONResponse(status=204)


# forwarding requests to the nearest server
@app.get('/{path}')
async def home(path: str, request: Request):
    if path != "home":
        data = {
            'Response': {
                'message': f"<Error> ’/{path}’ endpoint does not exist in server replicas",
                'status': "failure"
            }
        }
        raise HTTPException(status_code=400, detail=data)

    request_id = random.randint(100000, 1000000)  # random id temporarily

    # Get the nearest server for the request
    nearest_server = ch.get_nearest_server(request_id)
    if nearest_server == '':
        log("Couldn't route request: No servers available")
        return JSONResponse(content="No servers available", status_code=503)
    # Forward the request to the nearest server
    try:
        async with httpx.AsyncClient() as client:
            response = await client.get(f"http://{nearest_server}:8080/{path}")
    except:
        log(f"Server {nearest_server} didn't respond")
        # don't handle this failure multiple times, only once
        # Need to use mutex lock to ensure the ConsistentHashing ring
        # is not modified by anyone else
        with chLock:
            if nearest_server in ch.get_servers():
                handle_failure(nearest_server)

        # access request object
        url = str(request.url)
        log("Redirecting to {}".format(url))
        return RedirectResponse(url=url, status_code=307)  # let's try again, since we have handled the failure

    # Return the response from the server
    return  JSONResponse(content = response.content.decode('utf-8'), status_code = response.status_code)


def spawn_container(hostname):
    server_image = 'server'
    command = f'docker run --rm --name {hostname} --network mynet --network-alias {hostname} -e HOSTNAME={hostname} -d {server_image}'
    try:
        result = subprocess.run(command, shell=True, capture_output=True, text=True)
    except Exception as e:
        log(f"An error occurred while trying to start server container {hostname}: {e}")
        return "failure"

    if result.returncode != 0:
        log("Unable to start server container")
        log("Error:", result.stderr)
        return "failure"
    else:
        log("Successfully started server container with hostname ", hostname)
        return "success"


def remove_container(node_name):
    command = f'docker rm --force {node_name}'
    result = subprocess.run(command, shell=True, capture_output=True, text=True)

    if result.returncode != 0:
        log("Unable to removed server container")
        log("Error:", result.stderr)
        return "failure"
    else:
        log("Successfully removed server container")
        log("Output:", result.stdout)
        return "success"


# checks if any of the servers are down, is called when a server doesn't respond
def handle_failure(hostname):
    # if no hostname provided, error out
    if hostname is None:
        raise Exception("Fatal: No hostname provided to handle_failure()")

    # remove hostname temporarily so that other requests can be distributed to other servers
    ch.remove_server(hostname)
    timeout = httpx.Timeout(5.0, read=5.0)

    is_alive = False
    for _ in range(3):  # retry 3 times
        try:
            with httpx.Client(timeout=timeout) as client:
                response = client.get(f"http://{hostname}:8080/heartbeat")
            if response.status_code == 200:
                is_alive = True
                break  # server is alive
        except Exception as e:
            # ignore exceptions and try again
            log(
                f"handle_failure(): Exception raised while trying to check for liveliness of server {hostname}, "
                f"trying again: {e}")

    if is_alive:
        # server is alive, add it back to the ring
        ch.add_server(hostname)
        return
    else:
        # server is dead, spawn a new server to replace it
        log(f"handle_failure(): Server {hostname} is down!")

        # spawn new server with a random hostname
        new_hostname = generate_hostname(10)

        # keep trying to generate a hostname that's not already in use
        # why did I use ch.get_servers() here? Just in case an admin command is executed while this function is running
        while new_hostname in ch.get_servers():
            new_hostname = generate_hostname(10)

        res = spawn_container(new_hostname)
        time.sleep(1)

        if res == "success":
            try:
                ch.add_server(new_hostname)
                log(f"Succesfully added server {new_hostname} to replace {hostname}")
            except Exception as e:
                log(f"An error occurred while adding server {new_hostname} to replace {hostname}: {e}")
                ch.remove_server(new_hostname)
        else:
            log(f"Unable to add server {new_hostname}!")

async def heartbeat_check():
    while True:
        log("Checking heartbeat")
        with chLock:
            server_list = ch.get_servers()
            for server_hostname in server_list:
                is_alive = False
                for _ in range(3):
                    try:
                        with httpx.Client(timeout=httpx.Timeout(5)) as client:
                            response = client.get(f"http://{server_hostname}:8080/heartbeat")
                        if response.status_code == 200:
                            is_alive = True
                            break
                    except Exception as e:
                        log(f"heartbeat_check(): Exception raised wheile trying to check for liveliness of server {server_hostname}, "
                              f"trying again: {e}")
                if is_alive:
                    log(f"{server_hostname} is alive -- passed heartbeat check")
                else:
                    # server is dead, spawn a new server to replace it
                    log(f"handle_failure(): Server {server_hostname} is down!")

                    # spawn new server with a random hostname
                    new_hostname = generate_hostname(10)

                    # keep trying to generate a hostname that's not already in use
                    while new_hostname in ch.get_servers() or new_hostname in server_list:
                        new_hostname = generate_hostname(10)

                    res = spawn_container(new_hostname)
                    time.sleep(1)

                    if res == "success":
                        try:
                            ch.add_server(new_hostname)
                            log(f"Succesfully added server {new_hostname} to replace {server_hostname}")
                        except Exception as e:
                            log(f"An error occurred while adding server {new_hostname} to replace {server_hostname}: {e}")
                            ch.remove_server(new_hostname)
                    else:
                        log(f"Unable to add server {new_hostname} as a replacement for {server_hostname}!")
        await asyncio.sleep(5 * 60) # sleep for 5 minutes and then perform a check
        

async def main():
    loop = asyncio.get_event_loop()
    config = uvicorn.Config("lb_fast:app", host="0.0.0.0", port=5001)
    server = uvicorn.Server(config)
    #uvicorn.run("lb_fast:app", host="0.0.0.0", port=5001)
    loop.create_task(heartbeat_check)
    await server.serve()

if __name__ == "__main__":
    asyncio.run(main())
    