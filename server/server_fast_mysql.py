import uvicorn
from fastapi import FastAPI, StreamingResponse
from fastapi.responses import JSONResponse
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

from log_utils import write_log_entry, commit_logs

app = FastAPI()

server_identifier = 'server_test' # os.environ['HOSTNAME']

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


class Schema(TypedDict):
    columns: List[str]
    dtypes: List[str]

class ConfigRequest(BaseModel):
    schema_: Schema = Field(alias='schema')
    shards: List[str]

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


    for shard in config_request.shards:
        table_creation_query = f"CREATE TABLE IF NOT EXISTS {shard} ("

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

class CopyRequest(BaseModel):
    shards: List[str]

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


class Stud_id(TypedDict):
    low: int
    high: int

class ReadRequest(BaseModel):
    shard: str
    Stud_id: Stud_id

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

class RowData(TypedDict):
    Stud_id: int
    Stud_name: str
    Stud_marks: float

class WriteRequest(BaseModel):
    shard: str
    curr_idx: int
    data: List[RowData]
    replica_hostnames: Optional[List[str]] = None


@app.post('/write')
async def write_entries(write_request: WriteRequest):
    cursor = db_connection.cursor()
    write_response = {}
    curr_idx = write_request.curr_idx
    
    # write to logger
    write_log_entry(
        shard_id=write_request.shard,
        op_type="write",
        stud_id_low=write_request.data[0]["Stud_id"],
        stud_id_high=write_request.data[-1]["Stud_id"]
    )

    shard = write_request.shard
    query = "SELECT is_primary FROM shard_primary_mapping WHERE shard_id = %s"
    cursor.execute(query, (shard,))
    result = cursor.fetchone()
    is_primary = result[0] if result else None
    cursor.close()

    if is_primary:
        # get the replica address and 
        hostnames = write_request.replica_hostnames
        num_replicas = len(hostnames)

        # create copy of request json and nullify the replica_hostnames
        write_request_copy = write_request.dict()
        del write_request_copy['replica_hostnames']
        write_request_copy = WriteRequest(**write_request_copy)

        response_count = 0
        async with AsyncClient() as client:
            for hostname in hostnames:
                response = await client.post(f"http://{hostname}/write", json=write_request_copy.dict())
                if response.status_code == 200:
                    response_count += 1

        if response_count < num_replicas:
            endpoint_response = {
                "message": "Write failed",
                "status": "failed"
            }
            # DO ROLLBACK
            return JSONResponse(content=endpoint_response, status_code=500)

    # commit the logs for the specified shard
    commit_logs(write_request.shard)

    try:
        shard = write_request.shard
        # create entries
        entries = ", ".join([f"({entry['Stud_id']}, '{entry['Stud_name']}', {entry['Stud_marks']})" for entry in write_request.data])
        query = f"INSERT INTO {shard} (Stud_id, Stud_name, Stud_marks) VALUES {entries}"
        cursor.execute(query)
        db_connection.commit()

        # get the number of entries written
        num_entries_written = cursor.rowcount
        curr_idx += num_entries_written
    
    except Error as error:
        print(f"MySQL Error: '{error}'")
        endpoint_response = {
            "message": f"MySQL Error :{error}",
            "status": "failed"
        }
        return JSONResponse(content=endpoint_response, status_code=500)
    finally:
        cursor.close()
    
    write_response["message"] = "Data entries added"
    write_response["curr_idx"] = curr_idx
    write_response["status"] = "success"
    
    return JSONResponse(content=write_response, status_code=200)


class UpdateRequest(BaseModel):
    shard: str
    Stud_id: int
    data: RowData

@app.put('/update')
async def update_entry(update_request: UpdateRequest):
    cursor = db_connection.cursor()
    update_response = {}

    try:
        shard = update_request.shard
        query = (f"UPDATE {shard} SET Stud_id = {update_request.data['Stud_id']}, "
            f"Stud_name = '{update_request.data['Stud_name']}', "
            f"Stud_marks = {update_request.data['Stud_marks']} " 
            f"WHERE Stud_id = {update_request.Stud_id}")
        
        cursor.execute(query)
        db_connection.commit()
    except Error as error:
        print(f"MySQL Error: '{error}'")
        endpoint_response = {
            "message": f"MySQL Error :{error}",
            "status": "failed"
        }
        return JSONResponse(content=endpoint_response, status_code=500)
    finally:
        cursor.close()
    
    update_response["message"] = f"Data entry for Stud_id: {update_request.Stud_id} updated"
    update_response["status"] = "success"

    return JSONResponse(content=update_response, status_code=200)


class DeleteRequest(BaseModel):
    shard: str
    Stud_id: int

@app.post('/del')
async def delete_entry(delete_request: DeleteRequest):
    cursor = db_connection.cursor()
    delete_response = {}

    try:
        shard = delete_request.shard
        query = f"DELETE FROM {shard} WHERE Stud_id = {delete_request.Stud_id}"
        cursor.execute(query)
        db_connection.commit()
    except Error as error:
        print(f"MySQL Error: '{error}'")
        endpoint_response = {
            "message": f"MySQL Error :{error}",
            "status": "failed"
        }
        return JSONResponse(content=endpoint_response, status_code=500)
    finally:
        cursor.close()
    
    delete_response["message"] = f"Data entry for Stud_id: {delete_request.Stud_id} removed"
    delete_response["status"] = "success"

    return JSONResponse(content=delete_response, status_code=200)


@app.get('/log_file/{shard_id}')
async def get_log_file(shard_id: str):
    file = open(f"distributed_systems_logger_{shard_id}.db", "rb")
    return StreamingResponse(file, media_type="application/octet-stream")


def initialize():
    cursor = db_connection.cursor()

    cursor.execute("SHOW TABLES")
    tables = cursor.fetchall()

    for table in tables:
        try:
            cursor.execute(f"DROP TABLE {table[0]}")
        except Error as error:
            print(f"MySQL Error: '{error}'")


    create_table_query = """
    CREATE TABLE IF NOT EXISTS shard_primary_mapping (
        shard_id VARCHAR(32),
        is_primary BOOLEAN,
        PRIMARY KEY (shard_id)
    )
    """
    try:
        cursor.execute(create_table_query)
        print("shard_primary_mapping table created successfully.")
    except Error as e:
        print(f"The error '{e}' occurred.")
    finally:
        cursor.close()

    cursor.close()
    db_connection.commit()


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
    initialize()
    uvicorn.run("server_fast_mysql:app", host="0.0.0.0", port=8080, reload=True)