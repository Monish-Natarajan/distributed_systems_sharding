import aiohttp
import asyncio
import matplotlib.pyplot as plt
import json
import sys

async def make_request(session):
    url = sys.argv[1]
    async with session.get(url) as response:
        return await response.text()

async def launch_requests():
    async with aiohttp.ClientSession() as session:
        tasks = [make_request(session) for _ in range(100)]
        results = await asyncio.gather(*tasks)
        return results

def count_responses(results):
    counts = {}
    for response in results:
        # response is a json string
        # convert it into a dict
        print(response)
        response_dict = json.loads(response)
        message = response_dict['message']
        # message is of the format: Hello from server number: {server_identifier}
        # extract the server identifier
        server_identifier = message.split(':')[-1].strip()
        counts[server_identifier] = counts.get(server_identifier, 0) + 1
    # Sort the counts based on keys (server identifiers)
    sorted_counts = dict(sorted(counts.items()))

    return sorted_counts


def plot_barchart(counts):
    labels = list(counts.keys())
    values = list(counts.values())

    plt.bar(labels, values)
    plt.xlabel('Server Instances')
    plt.ylabel('Request Count')
    plt.title('Request Count Handled by Each Server Instance (Load Balancer)')
    plt.show()


async def main():
    results = await launch_requests()
    counts=count_responses(results)
    print(counts)
    print(f'Total Requests: {len(results)}')
    plot_barchart(counts)


if __name__ == '__main__':
    asyncio.run(main())
