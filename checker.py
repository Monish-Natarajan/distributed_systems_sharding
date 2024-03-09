import asyncio
import json
import aiohttp
PORT_NO = 5001


async def instantiate_server():
    url = f'http://localhost:{PORT_NO}/add'
    # make a POST request to the add endpoint
    async with aiohttp.ClientSession() as session:
        await session.post(url, json={'n': 1, 'hostnames': []})


async def delete_server():
    url = f'http://localhost:{PORT_NO}/rm'
    # make a POST request to the add endpoint
    async with aiohttp.ClientSession() as session:
        await session.post(url, json={'n': 1, 'hostnames': []})


async def list_servers():
    url = f'http://localhost:{PORT_NO}/rep'
    # make a GET request that will return the current list of servers
    async with aiohttp.ClientSession() as session:
        async with session.get(url) as response:
            result = json.loads(await response.text())
            print(result['response']['message']['replicas'])


async def checker():
    # take user input, should be either "add" or "rm"
    # loop until user enters "done"
    # if user enters "add", call instantiate_server()
    # if user enters "rm", call delete_server()
    # if user enters "rep", call list_servers()
    # if user enters "done", break out of the loop
    # else, print "Invalid input"
    user_input = input("Enter command (add, rm, rep, done) (no args for anything): ")
    while user_input != 'done':
        if user_input == 'add':
            await instantiate_server()
        elif user_input == 'rm':
            await delete_server()
        elif user_input == 'rep':
            await list_servers()
        else:
            print("Invalid input")
        user_input = input("Enter command (add, rm, rep, done) (no args for anything): ")

if __name__ == '__main__':
    asyncio.run(checker())