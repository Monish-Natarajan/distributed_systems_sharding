import aiohttp
import asyncio
import matplotlib.pyplot as plt
import json
import os
import ast


async def make_request(session, url):
    async with session.get(url) as response:
        return await response.text()


async def launch_requests():
    async with aiohttp.ClientSession() as session:
        tasks = [make_request(session, 'http://localhost:5000/home') for _ in range(10000)]
        results = await asyncio.gather(*tasks)
        return results


async def count_responses(results):
    # get current list of servers
    url = 'http://localhost:5000/rep'
    counts = {}
    # make a GET request that will return the current list of servers
    async with aiohttp.ClientSession() as session:
        async with session.get(url) as response:
            result = json.loads(await response.text())
            for i in ast.literal_eval(result['response']['message']['replicas']):
                counts[i] = 0
    for response in results:
        # response is a json string
        # convert it into a dict
        response_dict = json.loads(response)
        message = response_dict['response']['message']
        # message is of the format: Hello from server number: {server_identifier}
        # extract the server identifier
        server_identifier = message.split(':')[-1].strip()
        counts[server_identifier] = counts.get(server_identifier, 0) + 1
    # Sort the counts based on keys (server identifiers)
    sorted_counts = dict(sorted(counts.items()))

    return sorted_counts


def plot_barchart(counts, filename):
    labels = list(counts.keys())
    values = list(counts.values())

    fig, ax = plt.subplots()

    # Your bar plot code here
    ax.bar(labels, values)

    # Set labels and title
    ax.set_xlabel('Server Instances')
    ax.set_ylabel('Request Count')
    ax.set_title('Request Count Handled by Each Server Instance (Load Balancer)')
    plt.xticks(rotation='vertical')
    plt.tight_layout()
    # Ensure the 'output' directory exists
    if not os.path.exists('output'):
        os.makedirs('output')

    # Save the figure to the specified output filename
    plt.savefig(f'output/{filename}.png')

async def instantiate_server():
    url = 'http://localhost:5000/add'
    # make a POST request to the add endpoint
    async with aiohttp.ClientSession() as session:
        await session.post(url, json={'n': 1, 'hostnames': []})


async def delete_server():
    url = 'http://localhost:5000/rm'
    # make a POST request to the add endpoint
    async with aiohttp.ClientSession() as session:
        await session.post(url, json={'n': 1, 'hostnames': []})


async def list_servers():
    url = 'http://localhost:5000/rep'
    # make a GET request that will return the current list of servers
    async with aiohttp.ClientSession() as session:
        async with session.get(url) as response:
            result = json.loads(await response.text())
            print(result['response']['message']['replicas'])


async def common(n, filename):
    for i in range(n):
        await instantiate_server()
    results = await launch_requests()
    counts = await count_responses(results)
    # print a tabulated version of counts
    print("{:<20} {:<10}".format('Label', 'Number'))
    for k, v in counts.items():
        print("{:<20} {:<10}".format(k, v))

    plot_barchart(counts, filename)
    for i in range(n):
        await delete_server()


SEPARATOR = "----------------------------------------"


async def a1():
    print(SEPARATOR)
    print("| Running a1 |")
    print(SEPARATOR)
    await common(3, "a1_n_3")


async def a2():
    print(SEPARATOR)
    print("| Running a2 |")
    print(SEPARATOR)
    for n in range(2, 7):
        print(f"Running for n = {n}")
        await common(n, "a2_n_{}".format(n))


async def a3():
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


async def main():
    await a2()


if __name__ == '__main__':
    asyncio.run(main())
