import os
import time
# Spawning server container from spawner
#-------------------------------

server_image = "myserver:latest"
server_name = "server101"

# sleep for 10 seconds
time.sleep(10)
res = os.popen(f'docker run --name {server_name} --network mynet --network-alias {server_name} -e HOSTNAME=Server1 -e SERVER_IDENTIFIER=Server1 -p 8082:8080 -d {server_image}').read()

if len(res)==0:
    print("Unable to start server container")
else:
    print("successfully started server container")
