import asyncio
from log import log
import httpx
import database
from fastapi import FastAPI, Request, HTTPException
from fastapi.encoders import jsonable_encoder
from fastapi.responses import JSONResponse, RedirectResponse, Response
# include sqlite related libraries
import sqlite3

app = FastAPI()

# /primary_elect endpoint that picks one of the servers as the primary server for a particular shard
@app.get("/primary_elect/{shard_id}")
async def primary_elect(shard_id: str):
    # assumption: called only when none of the current servers is a primary server
    current_servers = database.get_servers_for_shard(shard_id)
    if not current_servers:
        raise HTTPException(status_code=404, detail="No servers are currently serving this shard")
    # query the servers to get their log files, and elect the one with the most number of log entries
    max_log_entries = -1
    primary_server = None
    for server_hostname in current_servers:
        try:
            with httpx.AsyncClient() as client:
                response = await client.get(f"http://{server_hostname}:8080/num_log_entries/{shard_id}")
            if response.status_code == 200:
                num_entries = response.json()["num_entries"]
                if num_entries > max_log_entries:
                    max_log_entries = num_entries
                    primary_server = server_hostname
        except Exception as e:
            log(f"primary_elect(): Exception raised while trying to get log entries from server {server_hostname}: {e}")
    # inform the primary server that it is the primary for a particular shard through the primary_elect endpoint of the server
    try:
        with httpx.AsyncClient() as client:
            response = await client.get(f"http://{primary_server}:8080/primary_elect/{shard_id}")
        if response.status_code == 200:
            return JSONResponse(content={"primary_server": primary_server})
    except Exception as e:
        log(f"primary_elect(): Exception raised while trying to inform server {primary_server} that it is the primary server: {e}")
        raise HTTPException(status_code=500, detail="Error while trying to inform the primary server")


async def heartbeat_check():
    # clean up the failureLocks dictionary
    # get servers in use
    while True:
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
                with httpx.Client() as client:
                    response = client.get(f"http://load_balancer:5001/handle_failure/{server_hostname}")
                if response.status_code == 200:
                    log(f"{server_hostname} heartbeat failure dealt with successfully")
        await asyncio.sleep(20 * 60) # sleep for 20 minutes and then perform a check