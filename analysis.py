import aiohttp
import asyncio
import matplotlib.pyplot as plt
import json
import os
import ast
import tqdm
import tqdm.asyncio

PORT_NO = 5000


async def make_request(session, url):
    async with session.get(url) as response:
        return await response.text()


async def launch_requests():
    async with aiohttp.ClientSession() as session:
        tasks = [make_request(session, f'http://localhost:{PORT_NO}/home') for _ in range(10000)]
        results = []
        
        # with tqdm.tqdm(total=len(tasks)) as pbar:
        #     for f in asyncio.as_completed(tasks):
        #         result = await f
        #         results.append(result)
        #         pbar.update(1)
        print("Sending requests")
        for f in tqdm.asyncio.tqdm.as_completed(tasks):
            results.append(await f)

        # results = await asyncio.gather(*tasks)
        return results


async def count_responses(results):
    # get current list of servers
    url = f'http://localhost:{PORT_NO}/rep'
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


async def common(n, filename):
    print("Instantiating servers")
    for i in tqdm.tqdm(range(n)):
        await instantiate_server()
    print("Finished instantiating servers")
    results = await launch_requests()
    print("Finished sending requests")
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

    # plot a line plot of 10000/N for N = 2 to 6
    fig, ax = plt.subplots()
    y = [10000 / i for i in range(2, 7)]
    x = [i for i in range(2, 7)]
    ax.plot(x, y)
    plt.tight_layout()
    ax.set_xlabel('Number of Servers')
    ax.set_ylabel('Number of Requests per Server')


def a3():
    print(SEPARATOR)
    print("| Running a3 |")
    print(SEPARATOR)
    # plot a line plot of 10000/N for N= 2 to 6
    fig, ax = plt.subplots()
    y = [10000 / i for i in range(2, 7)]
    x = [i for i in range(2, 7)]
    ax.plot(x, y)
    plt.tight_layout()
    ax.set_xlabel('Number of Servers')
    ax.set_ylabel('Number of Requests per Server')


async def main():
    # user prompt for a1, a2, a3
    user_input = input("Enter a1, a2 or a3 (or 'done' to exit): ")
    while user_input != 'done':
        if user_input == 'a1':
            await a1()
        elif user_input == 'a2':
            await a2()
        elif user_input == 'a3':
            a3()
        else:
            print("Invalid input")
        user_input = input("Enter a1, a2 or a3 (or 'done' to exit): ")


if __name__ == '__main__':
    asyncio.run(main())
