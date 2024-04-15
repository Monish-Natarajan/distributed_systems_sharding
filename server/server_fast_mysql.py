import uvicorn
from fastapi import FastAPI
from fastapi import UploadFile, File
from fastapi.responses import JSONResponse, StreamingResponse
from fastapi.encoders import jsonable_encoder
from typing import List, Dict, Optional
from typing_extensions import TypedDict
from pydantic import BaseModel, Field
import mysql.connector
from mysql.connector import Error
import sqlite3
from time import sleep
import string
import random
from httpx import AsyncClient
from models import *
from log_utils import *
import asyncio
import json
import os

app = FastAPI()

# server_identifier is the hostname
server_identifier = os.environ['HOSTNAME']
isPrimaryForShard: Dict[str, bool] = {} # is this server the primary server for a given shard id
writeLocks: Dict[str, asyncio.Lock] = {} # writeLocks maps shard_id to a write lock for that shard

server_replicas: Dict[str, List[str]] = {} # server_replicas maps shard_id to a list of replica hostnames (only useful for primary servers)

@app.get('/home')
async def home():
    data = {
        'response': {
            'message': 'Hello from server: ' + server_identifier,
            'status': 'success',
        }
    }
    return JSONResponse(content=data, status_code=200)


@app.get('/heartbeat')
def heartbeat():
    return {}


@app.post('/config')
async def config(config_request: ConfigRequest):
    '''
        payload_json = {
            "schema":
                {
                    "columns":["Stud_id","Stud_name","Stud_marks"],
                    "dtypes":["Number","String","Float"]
                }
            "shards":["sh1","sh2"],
        }
    '''
    try:
        cursor = db_connection.cursor()
    except Error as error:
        print(f"MySQL Error: '{error}'")

    # send shard list and init logger
    init_logger(config_request.shards)

    for shard in config_request.shards:
        table_creation_query = f"CREATE TABLE IF NOT EXISTS {shard} ("
        # create locks for writing
        writeLocks[shard] = asyncio.Lock()
        for column, dtype in zip(config_request.schema_["columns"], config_request.schema_["dtypes"]):
            if dtype == "String":
                sql_dtype = "VARCHAR(255)"
            elif dtype == "Number":
                sql_dtype = "INT"
            elif dtype == "Float":
                sql_dtype = "FLOAT"
            else:
                sql_dtype = "VARCHAR(255)"
            if column == "Stud_id":
                sql_dtype += " PRIMARY KEY"
            table_creation_query += f"{column} {sql_dtype} NOT NULL, "

        table_creation_query = table_creation_query.rstrip(", ") + ")"
        try:
            cursor.execute(table_creation_query)
            print("Cursor executed")
        except Error as error:
            print(f"MySQL Error: '{error}'")
            endpoint_response = {
                "message": f"Configuration failed :{error}",
                "status": "failed"
            }
            return JSONResponse(content=endpoint_response, status_code=500)

    cursor.close()    
    db_connection.commit()

    server_shards = ', '.join([f'{server_identifier}:{shard}' for shard in config_request.shards])
    endpoint_response = {
        "message": f"{server_shards} configured",
        "status": "success"
    }
    return JSONResponse(content=endpoint_response, status_code=200)

# for testing purposes
@app.post('/autopopulate')
async def autopopulate():
    cursor = db_connection.cursor()
    try:
        for _ in range(5):
            stud_id = random.randint(1, 100)
            stud_name = ''.join(random.choices(string.ascii_uppercase + string.ascii_lowercase, k=5))
            stud_marks = round(random.uniform(1, 100), 2)
            cursor.execute(f"INSERT INTO sh1 (Stud_id, Stud_name, Stud_marks) VALUES ({stud_id}, '{stud_name}', {stud_marks})")
        db_connection.commit()
    except Exception as e:
        print(f"An error occurred: {e}")
    finally:
        cursor.close()


@app.post('/copy')
async def get_all_entries(copy_request: CopyRequest):
    cursor = db_connection.cursor()
    copy_response = {}

    try:
        for shard in copy_request.shards:
            cursor.execute(f"SELECT * FROM {shard}")
            column_names = [column[0] for column in cursor.description]
            rows = cursor.fetchall()
            copy_response[shard] = [dict(zip(column_names, row)) for row in rows]
    except Error as error:
        print(f"MySQL Error: '{error}'")
        endpoint_response = {
            "message": f"MySQL Error :{error}",
            "status": "failed"
        }
        return JSONResponse(content=endpoint_response, status_code=500)
    finally: # finally will be executed even though the except returns
        cursor.close()

    copy_response["status"] = "success"
    return JSONResponse(content=copy_response, status_code=200)


@app.post('/read')
async def read_entries(read_request: ReadRequest):
    cursor = db_connection.cursor()
    read_response = {}

    try:
        shard = read_request.shard
        query = f"SELECT * FROM {shard} WHERE Stud_id >= {read_request.Stud_id['low']} AND Stud_id <= {read_request.Stud_id['high']}"
        cursor.execute(query)
        column_names = [column[0] for column in cursor.description]
        rows = cursor.fetchall()
        read_response["data"] = [dict(zip(column_names, row)) for row in rows]
    except Error as error:
        print(f"MySQL Error: '{error}'")
        endpoint_response = {
            "message": f"MySQL Error :{error}",
            "status": "failed"
        }
        return JSONResponse(content=endpoint_response, status_code=500)
    finally: # finally will be executed even though the except returns
        cursor.close()

    read_response["status"] = "success"
    return JSONResponse(content=read_response, status_code=200)

# end point for udating primary status
# @app.post('/primary')

@app.post('/primary_elect')
async def make_primary(request: PrimaryElectRequest):
    isPrimaryForShard[request.shard] = True
    server_replicas[request.shard] = request.replicas
    return JSONResponse(content={"message": f"{server_identifier} is now primary for shard {request.shard}", "status": "success"}, status_code=200)

@app.post('/add_slave')
async def add_slave(request: AddSlaveRequest):
    if not isPrimaryForShard.get(request.shard_id, False):
        # return error response saying this server is not a primary server for this shard
        return JSONResponse(content={"message": f"{server_identifier} is not primary for shard {request.shard_id}", "status": "failed"}, status_code=500)
    if request.server_hostname not in server_replicas.get(request.shard_id, []):
        if not server_replicas.get(request.shard_id, False):
            server_replicas[request.shard_id] = []
        server_replicas[request.shard_id].append(request.server_hostname)
    return JSONResponse(content={"message": f"Added {request.server_hostname} as a replica for shard {request.shard_id}", "status": "success"}, status_code=200)
@app.post('/remove_slave')
async def remove_slave(request: RemoveSlaveRequest):
    if not isPrimaryForShard.get(request.shard_id, False):
        # return error response saying this server is not a primary server for this shard
        return JSONResponse(content={"message": f"{server_identifier} is not primary for shard {request.shard_id}", "status": "failed"}, status_code=500)
    if request.server_hostname not in server_replicas.get(request.shard_id, []):
        server_replicas[request.shard_id].remove(request.server_hostname)
    return JSONResponse(content={"message": f"Removed {request.server_hostname} as a replica for shard {request.shard_id}", "status": "success"}, status_code=200)


async def modification_request(mod_request, should_log=True):
    print(f"DEBUG -- modification_request called with {mod_request}", flush=True)
    print(f"DEBUG -- Am I primary for this shard? -- {isPrimaryForShard.get(mod_request.shard, False)}", flush=True)
    num_records = 1
    query = ""
    if type(mod_request) is WriteRequest:
        op_type = "write"
        num_records = len(mod_request.data)
        # create entries
        entries = ", ".join([f"({entry['Stud_id']}, '{entry['Stud_name']}', {entry['Stud_marks']})" for entry in mod_request.data])
        query = f"INSERT INTO {mod_request.shard} (Stud_id, Stud_name, Stud_marks) VALUES {entries}"
    elif type(mod_request) is UpdateRequest:
        op_type = "update"
        query = (f"UPDATE {mod_request.shard} SET Stud_id = {mod_request.data['Stud_id']}, "
            f"Stud_name = '{mod_request.data['Stud_name']}', "
            f"Stud_marks = {mod_request.data['Stud_marks']} " 
            f"WHERE Stud_id = {mod_request.Stud_id}")
    elif type(mod_request) is DeleteRequest:
        op_type = "delete"
        query = f"DELETE FROM {mod_request.shard} WHERE Stud_id = {mod_request.Stud_id}"
    cursor = db_connection.cursor()
    write_response = {}
    async with writeLocks[mod_request.shard]:
        # write to logger
        if should_log:
            write_log_entry(
                shard_id=mod_request.shard,
                op_type=op_type,
                num_records=num_records,
                json_data=json.dumps(jsonable_encoder(mod_request))
            )

        shard = mod_request.shard
        is_primary = isPrimaryForShard.get(shard, False)

        if is_primary:
            # get the replica address and 
            if not server_replicas.get(shard, False):
                server_replicas[shard] = []
            hostnames = server_replicas[shard]
            hostnames = set(hostnames)
            if server_identifier in hostnames:
                hostnames.remove(server_identifier)
            num_replicas = len(hostnames)

            response_count = 0
            async def send_mod_request(replica_hostname, mod_request_copy):
                print(f"DEBUG -- send_mod_request for {replica_hostname} called with mod_request = \n {mod_request_copy}", flush=True)
                async with AsyncClient() as client:
                    response = await client.post(f"http://{replica_hostname}:8080/{op_type}", json=jsonable_encoder(mod_request_copy))
                    print(f"DEBUG -- send_mod_request response received from {replica_hostname}", flush=True)
                    return (replica_hostname, response)
            # launch async requests to secondary replicas together at once
            # asynchronously resume the execution of the function once majority of the replicas
            # have successfully written the data
            tasks = []
            cnt = 0
            print(f"Hostnames for write_request primary forwarding: {hostnames}", flush=True)
            print(f"DEBUG -- {cnt} tasks created", flush=True)
            for replica_hostname in hostnames:
                if replica_hostname == server_identifier:
                    continue
                task = asyncio.create_task(send_mod_request(replica_hostname, mod_request))
                cnt += 1
                tasks.append(task)
            print(f"DEBUG -- {cnt} tasks created", flush=True)
            response_count = 0
            rollback_servers = []
            for task in asyncio.as_completed(tasks):
                replica_hostname, response = await task
                if response.status_code == 200:
                    response_count += 1
                    rollback_servers.append(replica_hostname)
            print(f"DEBUG -- reached after as_completed", flush=True)
            if response_count < (num_replicas + 1) / 2:
                endpoint_response = {
                    "message": f"{op_type} failed",
                    "status": "failed"
                }
                # DO ROLLBACK
                for server in rollback_servers:
                    if type(mod_request) is WriteRequest:
                        for record in mod_request.data:
                            request = {
                                "shard": shard,
                                "data": record['Stud_id']
                            }
                            async with AsyncClient() as client:
                                response = await client.post(f"http://{server}:8080/delete", json=jsonable_encoder(request))
                                if response.status_code != 200:
                                    print(f"Failed to rollback record from {server}")
                return JSONResponse(content=endpoint_response, status_code=500)

        # commit the transactions for the specified shard into the actual database
        print("DEBUG -- Going to execute query", flush=True)
        try:
            cursor.execute(query)
            db_connection.commit()
        except Error as error:
            print(f"MySQL Error: '{error}'", flush=True)
            endpoint_response = {
                "message": f"MySQL Error :{error}",
                "status": "failed"
            }
            return JSONResponse(content=endpoint_response, status_code=500)
        finally:
            cursor.close()
        
        write_response["message"] = "Operation '{op_type}' completed"
        write_response["status"] = "success"
        
        return JSONResponse(content=write_response, status_code=200)

@app.post('/write')
async def write_entries(write_request: WriteRequest):
    print(f"Write request received: {write_request}", flush=True)
    return await modification_request(write_request)



@app.post('/update')
async def update_entry(update_request: UpdateRequest):
    return await modification_request(update_request)



@app.post('/delete')
async def delete_entry(delete_request: DeleteRequest):
    return await modification_request(delete_request)


@app.get('/log_file/{shard_id}')
async def get_log_file(shard_id: str):
    file = open(f"distributed_systems_logger_{shard_id}.db", "rb")
    return StreamingResponse(file, media_type="application/octet-stream")
@app.get('/num_log_entries/{shard_id}')
async def get_num_entries(shard_id: str):
    response = {
        "num_entries": count_log_entries(shard_id)
    }
    print(f"Sending num_log_entries response: {response}")
    return JSONResponse(content=response, status_code=200)

@app.post('/upload_log_file/{shard_id}')
async def upload_log_file(shard_id: str, file: UploadFile = File(...)):
    with open(f"distributed_systems_logger_{shard_id}.db", "wb") as out_file:
        content = await file.read()
        out_file.write(content)
    add_connector(shard_id)
    # read the log file through sqlite3 and commit the entries to the actual database
    logs = get_logs(shard_id)
    try:
        for log in logs:
            operation = log[1]
            if operation == "write":
                request = WriteRequest(**json.loads(log[3]))
            elif operation == "update":
                request = UpdateRequest(**json.loads(log[3]))
            elif operation == "delete":
                request = DeleteRequest(**json.loads(log[3]))
            await modification_request(request, should_log=False)
    except Exception as e:
        print(f"An error occurred: {e}")
        return JSONResponse(content={"message": f"Error occurred while processing log file: {e}", "status": "failed"}, status_code=500)
    # JSON success response
    return JSONResponse(content={"message": "Log file uploaded successfully", "status": "success"}, status_code=200) 


sleep(10)
db_connection = mysql.connector.connect(
    host="127.0.0.1",
    user="root", 
    password="testing",
    database="distributed_systems",
    auth_plugin='mysql_native_password'
)


PRIMARY=False # UPDATE THIS

if __name__ == "__main__":
    uvicorn.run("server_fast_mysql:app", host="0.0.0.0", port=8080, reload=True)