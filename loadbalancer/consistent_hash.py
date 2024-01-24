import random

class RequestNode:
    def __init__(self, request_id):
        self.id = request_id

class ServerNode:
    def __init__(self, server_id, server_hostname):
        self.id = server_id
        self.hostname = server_hostname

class ConsistentHashing:
    def __init__(self):
        self.server_list = []
        pass

    def add_server(self, node):
        # Add a server node to the consistent hashing ring
        self.server_list.append(node)

    def add_request(self, node):
        # Add a request node to the consistent hashing ring
        pass

    def remove_server(self, key):
        self.server_list.remove(key)

    def get_nearest_server(self, key):
        # Get the node responsible for a given key
        if len(self.server_list) == 0:
            return None
        else:
            # return a random server for self.server_list
            return random.choice(self.server_list)
        
    def search_server(self, key):
        if key in self.server_list:
            return True
        else:
            return False

    def get_servers(self):
        # Get all the server nodes in the consistent hashing ring
        return self.server_list
