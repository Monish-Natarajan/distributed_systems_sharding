import uvicorn
from fastapi import FastAPI
from fastapi.responses import JSONResponse
import os

app = FastAPI()

# Unique identifier for the server
# This will be the server's hostname
server_identifier = os.environ['HOSTNAME']

@app.get('/home')
async def home():
    data = {
        'response': {
            'message': 'Hello from server: ' + server_identifier,
            'status': 'successful',
        }
    }
    return JSONResponse(content=data, status_code=200)


@app.get('/heartbeat')
def heartbeat():
    # returns status code 200 by default
    return {}

if __name__ == "__main__":
    uvicorn.run("server_fast:app", host="0.0.0.0", port=8080)