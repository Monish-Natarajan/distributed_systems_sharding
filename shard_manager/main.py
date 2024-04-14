import asyncio
from log import log
import httpx
import database
from fastapi import FastAPI, Request, HTTPException
from fastapi.encoders import jsonable_encoder
from fastapi.responses import JSONResponse, RedirectResponse, Response

app = FastAPI()

# /primary_elect endpoint that picks one of the servers as the primary server
@app.get("/primary_elect")
async def primary_elect():
    # assumption: called only when none of the current servers is a primary server
    current_servers = database.get_unique_servers()
    # choose a server at random
    primary_server = current_servers[0]
    # update the MapT table
    


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
                await handle_failure(server_hostname)
    await asyncio.sleep(20 * 60) # sleep for 20 minutes and then perform a check