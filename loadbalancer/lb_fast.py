from fastapi import FastAPI, Request, HTTPException
from fastapi.encoders import jsonable_encoder
from fastapi.responses import JSONResponse, RedirectResponse, Response
import uvicorn

import httpx
import random
import time
import subprocess
import string
import asyncio
from log import log
import database
from typing import List, Dict
from request_models import *
from database import ShardRecord, MapRecord, set_primary, primary_for_shard

from consistent_hasher import ConsistentHashing

def generate_hostname(n):
    # random hostname generated of len < n
    return "server_" + ''.join(random.choices(string.ascii_lowercase, k=random.randint(4, n)))

class ShardData:
    def __init__(self, shard_id):
        self.shard_id: str = shard_id
        self.ch: ConsistentHashing = ConsistentHashing()

app = FastAPI()
shardDataMap: Dict[str, ShardData] = {} # maps shard_id to a tuple containing consistent hashing object
failureLocks: Dict[str, asyncio.Lock] = {} # maps server hostname to a lock to prevent multiple failures from being handled concurrently
deadServers: List[str] = []
adminLock = asyncio.Lock()
schemaConfig = {}

async def reapDeadServer(hostname):
    await asyncio.sleep(60)
    # by the time this sleep ends, hopefully all requests to this server have been redirected (i.e. "failed")
    deadServers.remove(hostname)
    log(f"Reaped dead server {hostname}")

# initializes the load balancer and spawns servers
@app.post('/init')
async def initalize_loadbalancer(request_body: InitRequest):
    # errors to handle
    # mismatch between shard_name in request_body.shards and request_body.servers
    # Reject request if error found before spawning anything
    shards_to_add = set()
    for shard in request_body.shards:
        if shard.Shard_id in shards_to_add:
            data = {
                'message': f"<Error> Duplicate shard id {shard.Shard_id} found in request",
                'status': "failure"
            }
            return JSONResponse(
                content=data,
                status_code=400
            )
        shards_to_add.add(shard.Shard_id)
    shards_referred = set()
    for server, shards in request_body.servers.items():
        shards_referred.update(shards)
    # if the two sets are not equal, then there is a mismatch
    if shards_to_add != shards_referred:
        data = {
            'message': f"<Error> Mismatch between shards in 'shards' and 'servers'",
            'status': "failure"
        }
        return JSONResponse(
            content=data,
            status_code=400
        )
    
    # if we have reached this point, we can safely assume that the request is valid
    log("Initializing load balancer")
    # insert into ShardT
    try:
        for shard in request_body.shards:
            record = ShardRecord(Stud_id_low=shard.Stud_id_low, Shard_id=shard.Shard_id, Shard_size=shard.Shard_size, valid_idx=1)
            database.insert_shard_record(record)
        log("Successfully inserted records into ShardT")
        # insert into MapT
        for server, shards in request_body.servers.items():
            for shard in shards:
                record = MapRecord(Shard_id=shard, Server_id=server)
                database.insert_map_record(record)
        log("Successfully inserted records into MapT")
    except Exception as e:

        # undo state changes, TBD

        data = {
            'message': f"<Error> An error occurred while initializing the load balancer: {e}",
            'status': "failure"
        }
        raise HTTPException(status_code=400, detail=data)


            
    # initialize the consistent hashing ring for each shard
    for shard in request_body.shards:
        shard_id = shard.Shard_id
        shardDataMap[shard_id] = ShardData(shard_id)

    # spawn servers and add them to hashing ring of appropriate shards
    try:
        for server, shards in request_body.servers.items():
            res = spawn_container(server)
            if res != "success":
                raise Exception(f"Unable to spawn server container {server}")
        # wait for the servers to start
        await asyncio.sleep(60)
        log("Done with waiting for servers to start")
        # config all the servers
        for server, shards in request_body.servers.items():
            async with httpx.AsyncClient() as client:
                request = {
                    "schema": request_body.schema_.model_dump(),
                    "shards": shards
                }
                log(f"Trying to config server {server}")
                response = (await client.post(f"http://{server}:8080/config", json=jsonable_encoder(request))).json()
                if response['status'] != "success":
                    raise Exception(f"config() failed for server {server} with status {response['status']}")
            # add server to the consistent hashing ring
            for shard in shards:
                shardDataMap[shard].ch.add_server(server)
                log(f"Succesfully added server {server} to hash ring of shard {shard}")
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
    global schemaConfig
    schemaConfig = request_body.schema_.model_dump()
    # need to elect primaries for each shard through the /primary_elect endpoint of the shard manager
    for s in request_body.shards:
        async with httpx.AsyncClient() as client:
            response = (await client.get(f"http://shard_manager:8080/primary_elect/{s.Shard_id}")).json()
            set_primary(s.Shard_id, response['primary_server'])

    return JSONResponse(content=reponse_data, status_code=200)


@app.get('/status')
async def status():

    # get the schema (static)
    schema = {
        "columns": ["Stud_id", "Stud_name", "Stud_marks"],
        "dtypes": ["Number", "String", "String"]
    }

    # get the shard records
    shard_records = database.get_shards() # shard_records is a list of tuples of the form (Stud_id_low, Shard_id, Shard_size, primary_server)
    shards = [{"Stud_id_low": record[0], "Shard_id": record[1], "Shard_size": record[2], "primary_server": primary_for_shard(record[1])} for record in shard_records]

    server_names = database.get_unique_servers()
    
    serverToShards = {}
    for server_name in server_names:
        shard_names = database.get_shards_for_server(server_name)
        serverToShards[server_name] = shard_names

    response_data = {
        "N": len(server_names),
        "schema": schema,
        "shards": shards,
        "servers": serverToShards
    }

    return JSONResponse(content=response_data, status_code=200)


@app.post('/add')
async def add_servers(request_body: AddRequest):
    if request_body.n > len(request_body.servers):
        # return error response saying n is invalid for the number of servers provided
        return JSONResponse(
            content={
                "message": "Number of servers to add is more than the servers provided",
                "status": "failure"
            },
            status_code=400
        )
    async with adminLock:
        try:
            # spawn the servers first
            new_server_names = request_body.servers.keys()
            for ind, server_name in enumerate(new_server_names):
                res = spawn_container(server_name)
                if res != "success":
                    # rolling back container spawning
                    for i in range(0, ind):
                        remove_container(new_server_names[i])
                    return JSONResponse(
                        content={
                            "message": f"Failed to spawn server {server_name}",
                            "status": "failure"
                        },
                        status_code=400
                    )
            for shard in request_body.new_shards:
                record = ShardRecord(Stud_id_low=shard.Stud_id_low, Shard_id=shard.Shard_id, Shard_size=shard.Shard_size, valid_idx=1)
                database.insert_shard_record(record)
                shardDataMap[shard.Shard_id] = ShardData(shard.Shard_id)
            await asyncio.sleep(60) # wait for the servers to finish spawning

            # config the new servers just like what we did in handle_failure when we spawned a new server
            # or like what we did in /init
            global schemaConfig
            for server_name in new_server_names:
                async with httpx.AsyncClient() as client:
                    request = {
                        "schema": schemaConfig,
                        "shards": request_body.servers[server_name]
                    }
                    response = (await client.post(f"http://{server_name}:8080/config", json=jsonable_encoder(request))).json()
                    if response['status'] != "success":
                        raise Exception(f"config() failed for server {server_name} with status {response['status']}")
            # copy the old shards to the newly spawned servers as needed
            for server_name in new_server_names:
                for shard in request_body.servers[server_name]:
                    if shard in request_body.new_shards:
                        continue # no need to copy anything
                    prim_server = primary_for_shard(shard)
                    # Step 1: copy over the log file for this shard by asking for it through the /log_file/{shard_id} endpoint
                    # on the primary server
                    # Step 2: contact the primary server through the /copy endpoint, copy over the records to the new server
                    async with httpx.AsyncClient() as client:
                        response = await client.get(f"http://{prim_server}:8080/log_file/{shard}", stream=True)
                        if response.status_code != 200:
                            raise Exception(f"Failed to get log file for shard {shard} from primary server {prim_server}")
                        # place this log file on the new server through /upload_log_file/{shard_id} endpoint on the new server
                        log_file_data = await response.read()
                        files = {"file": ("distributed_systems_logger_{shard}.db", log_file_data, "application/octet-stream")}
                        upload_response = await client.post(f"http://new_server:8000/upload_log_file/{shard}", files=files)
                        if upload_response.status_code != 200:
                            raise Exception(f"Failed to upload log file for shard {shard} to new server")


                    # choose a server that contains this shard
                    for server in shardDataMap[shard].ch.get_servers():
                        # copy data from this server
                        try:
                            async with httpx.AsyncClient() as client:
                                response = (await client.post(f"http://{server}:8080/copy", json=jsonable_encoder({"shards": [shard]}))).json()
                                if response['status'] == "success":
                                    # write this shard to the new server
                                    request = {
                                        "shard": shard,
                                        "curr_idx": 0,
                                        "data": response[shard]
                                    }
                                    response = (await client.post(f"http://{server_name}:8080/write", json=jsonable_encoder(request))).json()
                                    if response['status'] != "success":
                                        raise Exception(f"write() failed for shard {shard} with status {response['status']}")
                                    break
                                else:
                                    raise Exception(f"status={response['status']}")
                        except Exception as e:
                            log(f"An error occurred while trying to copy shard {shard} from server {server}: {e}")
                            log(f"Replicas have been lost")
                            return
            # all servers set up
            # need to expose them by adding them to the hashing rings and the database (MapT)
            for server_name in new_server_names:
                for shard in request_body.servers[server_name]:
                    async with shardDataMap[shard].writeLock:
                        database.insert_map_record(MapRecord(Shard_id=shard,Server_id=server_name))
                        shardDataMap[shard].ch.add_server(server_name)
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
    async with adminLock:
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
                all_servers.add(server)     

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
                delete_server(server_name)
        
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
            response = (await client.post(f"http://{nearest_server}:8080/read", json=jsonable_encoder(request))).json()
            
            if response['status'] == "success":
                return response['data']
            else:
                raise Exception(f"read_from_shard() failed for shard {shard_id} with status {response['status']}")
    except Exception as e:
        # couldn't connect to the nearest server, what do we do?
        await handle_failure(nearest_server)
        # Let this request fail, return an error response
        raise e


def get_shard_id(student_id: int):
    all_shards = database.get_shards()
    for shard in all_shards:
        lowerEnd = int(shard[0])
        upperEnd = lowerEnd + int(shard[2]) - 1
        if student_id >= lowerEnd and student_id <= upperEnd:
            return shard[1]
    return None

# returns all the records contained on a particular server
@app.get('/read/{server_id}')
async def read_from_server(server_id: str):
    # get shards that are contained on this server from MapT
    shards = database.get_shards_for_server(server_id)
    async with httpx.AsyncClient() as client:
        request = {
            "shards": shards
        }
        response = (await client.post(f"http://{server_id}:8080/copy", json=jsonable_encoder(request))).json()
        return JSONResponse(content=response, status_code=200)

@app.post('/read')
async def read(request_body: ReadRequest):
    low = request_body.Stud_id['low']
    high = request_body.Stud_id['high']
    # check which shard to read low from
    all_shards = database.get_shards() # List[tuples] sorted in ascending order of Stud_id_low
    shards_queried = []
    records = []
    try:
        for shard in all_shards:
            lowerEnd = int(shard[0])
            upperEnd = lowerEnd + int(shard[2]) - 1
            if low >= lowerEnd and low <= upperEnd:
                # read the relevant records from this shard
                records += await read_from_shard(shard[1], low, min(high, upperEnd))
                shards_queried.append(shard[1])
                low = upperEnd + 1
            if low > high:
                break
        response = {
            "shards_queried": shards_queried,
            "data": records,
            "status": "success"
        }
        return response
    except Exception as e:
        log(f"Error while reading from shard {shard[1]}: ", e)
        return JSONResponse(
            content={
                "message": "Failed to read records, try again",
                "status": "failure"
            },
            status_code=500 
        )


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

    shardsWritten: List[str] = [] 
    for shard_id, records in shardWriteMap.items():
        # contact the primary server responsible for this shard and pass on the request to it
        primary_server = primary_for_shard(shard_id)
        async with httpx.AsyncClient() as client:
            request = {
                "shard": shard_id,
                "data": records
            }
            response = (await client.post(f"http://{primary_server}:8080/write", json=jsonable_encoder(request))).json()
            if response.status_code != 200:
                log(f"Failed to write to shard {shard_id}")
                # no need to rollback writes
                # proceed with other shards
                continue

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
    oldRecord = (await read_from_shard(shard_id, request_body.Stud_id, request_body.Stud_id))
    if len(oldRecord) == 0:
        return JSONResponse(
            content={
                "message": "Student ID not found",
                "status": "failure"
            },
            status_code=404
        )
    oldRecord = oldRecord[0]
    # forward the request to the appropriate primary server for this shard
    primary_server = primary_for_shard(shard_id)
    request = {
        "shard": shard_id,
        "Stud_id": request_body.Stud_id,
        "data": request_body.data
    }
    async with httpx.AsyncClient() as client:
        response = (await client.post(f"http://{primary_server}:8080/update", json=jsonable_encoder(request))).json()
        if response['status'] == "success":
            return JSONResponse(
                content={
                    "message": "Successfully updated record",
                    "status": "success"
                },
                status_code=200
            )
        else: 
            return JSONResponse(
                content={
                    "message": "Failed to update record",
                    "status": "failure"
                },
                status_code=500
            )
            

@app.put('/update')
async def update(request_body: UpdateRequest):
    return await modify_record(request_body)

@app.delete('/del')
async def delete(request_body: DeleteRequest):
    return await modify_record(request_body)


# FIXME: Should be removed ig
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


# FOR TESTING ONLY
@app.get('/test')
def test():
    return JSONResponse(content="Test successful", status_code=200)


# returns  "204 No Content" for favicon.ico
@app.get('/favicon.ico', status_code=204)
def favicon():
    pass


def delete_server(hostname, temporary=False):
    # remove this server from the consistent hashing ring of all shards
    for shard in shardDataMap.keys():
        shardDataMap[shard].ch.remove_server(hostname)
    if not temporary:
        try:
            # remove server from the MapT table
            database.delete_map_server(hostname)
        except Exception as e:
            log(f"Fatal - Unable to remove server {hostname} from the MapT table: {e}")
            exit(1)
        # remove server container
        res = remove_container(hostname)
        if res != "success":
            log(f"Unable to remove server container {hostname}")

# inserts a server that already exists as a container back into the ring and tables
def insert_server(hostname):
    # get all the shards that this server is responsible for
    shards = database.get_shards_for_server(hostname)
    for shard in shards:
        shardDataMap[shard].ch.add_server(hostname)


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

@app.get('/handle_failure/{hostname}')
async def handle_failure_route(hostname: str):
    await handle_failure(hostname)

# checks if any of the servers are down, is called when a server doesn't respond
async def handle_failure(hostname):
    log(f"Handling failure for server {hostname}")
    if hostname is None:
        raise Exception("Fatal: No hostname provided to handle_failure()")
    async with adminLock:
        log(f"Acquired lock to handle failure for server {hostname}")
        if hostname in deadServers:
            # server didn't respond to our repeated requests and was respawned under
            # a different name
            # nothing to do here
            return
        # get the list of all servers
        # remove hostname temporarily so that other requests can be distributed to other servers
        delete_server(hostname, temporary=True)
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
            insert_server(hostname)
            return
        else:
            # server is dead, spawn a new server to replace it
            log(f"handle_failure(): Server {hostname} is down!")
            # get the shards that this server once held
            shards_to_copy = database.get_shards_for_server(hostname)
            # need to remove it from the MapT table, it has already been removed the consistent hashing rings
            try:
                database.delete_map_server(hostname)
            except Exception as e:
                log(f"Fatal - Unable to remove server {hostname} from the MapT table: {e}")
                exit(1)
            # spawn new server with a random hostname
            new_hostname = generate_hostname(10)
            current_server_names = database.get_unique_servers()
            # keep trying to generate a hostname that's not already in use
            while new_hostname in deadServers or new_hostname in current_server_names:
                new_hostname = generate_hostname(10)

            res = spawn_container(new_hostname)
            # need to sleep for sometime until server has been spawned
            await asyncio.sleep(60)
            if res == "success":
                try:                    
                    # * copy the old shards from servers that contain them
                    # * each server has an endpoint called /copy that gives us the
                    #   shard we want in its entirety
                    # * shards_to_copy is a list of shard_ids that we hope to copy from the remaining
                    #   replicas, retrievedShards is a dict of shards that we could actually recover mapping
                    #   to their contents
                    retrievedShards: Dict[str, List[StudentModel]] = {}
                    for shard in shards_to_copy:
                        # choose a server that contains this shard
                        chRing = shardDataMap[shard].ch
                        servers_with_shard = chRing.get_servers()
                        # try to copy from every server possible until one of them
                        # responds
                        request = {
                            "shards": [shard]
                        }
                        for server in servers_with_shard:
                            try:
                                async with httpx.AsyncClient() as client:
                                    response = (await client.post(f"http://{server}:8080/copy", json=jsonable_encoder(request))).json()
                                    if response['status'] == "success":
                                        retrievedShards[shard] = response[shard]
                                        break
                                    else:
                                        raise Exception(f"status={response['status']}")  # try the next server after exception handling                                    
                            except Exception as e:
                                log(f"handle_failure(): Exception raised while trying to copy shard {shard} from server {server}: {e}")
                    # * place the shard data in the new server
                    # * we need to config the server through the /config endpoint
                    try:
                        async with httpx.AsyncClient() as client:
                            request = {
                                "schema": schemaConfig,
                                "shards": list(retrievedShards.keys())
                            }
                            response = (await client.post(f"http://{new_hostname}:8080/config", json=jsonable_encoder(request))).json()
                            log(f"DEBUG -- {response}")
                            log(f"DEBUG -- status={response['status']}")
                            if response['status'] != "success":
                                log(f"DEBUG -- I am here")
                                raise Exception(f"config() failed for server {new_hostname} with status {response['status']}")
                    except Exception as e:
                        log(f"handle_failure(): Exception raised while trying to config server {new_hostname}: {e}")
                        log(f"Replicas have been lost")
                        return
                    # config is done, need to copy the data we retrieved to the new server
                    successfullyCopiedShards: List[str] = []
                    for shard in retrievedShards:
                        # try to write this shard to the new server
                        try:
                            async with httpx.AsyncClient() as client:
                                request = {
                                    "shard": shard,
                                    "curr_idx": 0,
                                    "data": retrievedShards[shard]
                                }
                                response = (await client.post(f"http://{new_hostname}:8080/write", json=jsonable_encoder(request))).json()
                                if response['status'] != "success":
                                    raise Exception(f"write() failed for shard {shard} with status {response['status']}")
                            successfullyCopiedShards.append(shard)
                        except Exception as e:
                            log(f"handle_failure(): Exception raised while trying to write shard {shard} to server {new_hostname}: {e}")
                            log(f"Lost a replica")
                            continue # copying this shard failed, lost a replica, can't do much, move on to the next shard
                    # add it to the MapT table with the corresponding old shards
                    for shard in successfullyCopiedShards:
                        record = MapRecord(Shard_id=shard, Server_id=new_hostname)
                        database.insert_map_record(record)
                        # need to add the new server to the consistent hashing ring for all the shards it has a replica of
                        shardDataMap[shard].ch.add_server(new_hostname)
                    log(f"Succesfully added server {new_hostname} to replace {hostname}")
                    deadServers.append(hostname) # prevent further failure handling for the old server that's no more
                    asyncio.get_event_loop().create_task(reapDeadServer(hostname))
                except Exception as e:
                    log(f"An error occurred while adding server {new_hostname} to replace {hostname}: {e}")
                    # can't do much, let's just continue
            else:
                log(f"Unable to add server {new_hostname}!")

async def heartbeat_check():
    # clean up the failureLocks dictionary
    # get servers in use
    while True:
        async with adminLock:
            servers_in_use = database.get_unique_servers()
            log("Checking heartbeat")
            for server_hostname in servers_in_use:
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
                    await handle_failure(server_hostname)
        await asyncio.sleep(20 * 60) # sleep for 20 minutes and then perform a check
        

async def main():
    loop = asyncio.get_event_loop()
    config = uvicorn.Config("lb_fast:app", host="0.0.0.0", port=5001)
    server = uvicorn.Server(config)
    #uvicorn.run("lb_fast:app", host="0.0.0.0", port=5001)
    loop.create_task(heartbeat_check())
    await server.serve()

if __name__ == "__main__":
    asyncio.run(main())
    