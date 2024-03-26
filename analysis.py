import os
import httpx
import time
import matplotlib.pyplot as plt
from tqdm import tqdm
import tqdm.asyncio
import asyncio
performance = {"Write": {}, "Read": {}}
numOfRW = 10000

def printGraph():
    global performance
    print(performance)
    # print the graph
    fig, ax = plt.subplots()
    ax.bar(performance["Write"].keys(), performance["Write"].values())
    ax.set_ylabel('Time (in seconds)')
    ax.set_xlabel('Configurations')
    ax.set_title('Write Performance')
    # plt.xticks(rotation=90)
    plt.show()
    fig, ax = plt.subplots()
    ax.bar(performance["Read"].keys(), performance["Read"].values())
    ax.set_ylabel('Time (in seconds)')
    ax.set_xlabel('Configurations')
    ax.set_title('Read Performance')
    # plt.xticks(rotation=90)
    plt.show()


async def send_write_request(i):
    payload = {
        "data": [{"Stud_id":i, "Stud_name":f"Student{i}", "Stud_marks": i%100}]
    }
    async with httpx.AsyncClient() as client:
        response = await client.post("http://localhost:5001/write", json=payload, timeout=1000)
        if response.status_code != 200:
            print("Error in writing")
            print(response.text)
            exit(0)
        return response.elapsed.microseconds
async def send_read_request(i):
    payload = {"Stud_id": {"low":i, "high":i+1}}
    async with httpx.AsyncClient() as client:
        response = await client.post(f"http://localhost:5001/read", json=payload, timeout=1000)
        if response.status_code != 200:
            print("Error in reading")
            print(response.text)
            exit(0)
        return
async def analysis(numOfShards, numOfServers, numOfReplicas):
    global performance, numOfRW
    # start the system in the background
    os.system("make clean")
    os.system("make setup")
    os.system("make run")
    # init the system
    shards = []
    for i in range(numOfShards):
        shards.append({"Stud_id_low": i*4096, "Shard_id": f"sh{i}", "Shard_size": 4096})
    servers = {}
    for i in range(numOfServers):
        servers[f"Server{i}"] = []
    j = 0
    for i in range(numOfShards):
        for k in range(numOfReplicas):
            servers[f"Server{j}"].append(f"sh{i}")
            j = (j+1) % numOfServers
    payload = {
        "N":3, 
        "schema": {
            "columns":["Stud_id","Stud_name","Stud_marks"], 
            "dtypes":["Number","String","String"]
            }, 
        "shards":[{"Stud_id_low":0, "Shard_id": "sh1", "Shard_size":4096}, {"Stud_id_low":4096, "Shard_id": "sh2", "Shard_size":4096}, {"Stud_id_low":8192, "Shard_id": "sh3", "Shard_size":4096}], 
        "servers":{"Server0":["sh1","sh2"], "Server1":["sh2","sh3"], "Server2":["sh1","sh3"]}
    }
    time.sleep(60)
    response = httpx.Client().post("http://localhost:5001/init", json=payload, timeout=1000)
    if response.status_code != 200:
        print("Error in init")
        print(response.text)
        exit(0)
    # perform writes
    readTime = 0
    writeTime = 0
    print("Performing writes")
    # launch numOfRW requests asynchronusly at the same time
    write_tasks = [send_write_request(i) for i in range(0, numOfRW)]
    print("Sending requests")
    start_write_time = time.time()
    for f in tqdm.asyncio.tqdm.as_completed(write_tasks):
        await f
    end_write_time = time.time()
    writeTime = end_write_time - start_write_time
            
    print("Performing reads")
    read_tasks = [send_read_request(i) for i in range(numOfRW)]
    start_read_time = time.time()
    for f in tqdm.asyncio.tqdm.as_completed(read_tasks):
        await f
    end_read_time = time.time()
    readTime = end_read_time - start_read_time
    performance["Write"][f"{numOfShards} Shards, {numOfServers} Servers, {numOfReplicas} Replicas"] = writeTime
    performance["Read"][f"{numOfShards} Shards, {numOfServers} Servers, {numOfReplicas} Replicas"] = (readTime)

    # stop the system
    os.system("make stop")
    os.system("make clean")


async def main():
    await analysis(4, 6, 3)
    await analysis(4, 6, 6)
    await analysis(6, 10, 8)
    printGraph()


if __name__ == "__main__":
    asyncio.run(main())
