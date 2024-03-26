# Report

## Files and Folders
`analysis.py` - performs the required analysis for subtasks
`loadbalancer/` - contains all the code for the loadbalancer
`server/` - contains all the code for the server
`output/` - contains the output graphs for the analysis

## How to run?
```bash
python3 -m venv venv
source venv/bin/activate    # to activate the Python virtual env

make clean                  # to ensure we are starting afresh
make setup                  # to install the dependencies
make run                    # run the load balancer
make clean                  # to wrap things up
```

# Performance

![Read performance](output/read_performance.png)
![Write performance](output/write_performance.png)


| Configuration                      |   Write   |   Read    |
|------------------------------------|-----------|-----------|
| 4 Shards, 6 Servers, 3 Replicas   |  896.493  |  446.517  |
| 4 Shards, 6 Servers, 6 Replicas   |  914.811  |  447.068  |
| 6 Shards, 10 Servers, 8 Replicas  |  892.064  |  454.847  |



# server_fast_mysql.py

This Python script uses the FastAPI framework to create a RESTful API server that interacts with a MySQL database. The server provides several endpoints for managing data in the database.

## Dependencies

- uvicorn: ASGI server
- fastapi: web framework
- mysql.connector: MySQL driver
- pydantic: data validation
- typing and typing_extensions: type hints

## Endpoints

- `GET /home`: Returns a greeting message from the server.
- `GET /heartbeat`: Returns an empty response.
- `POST /config`: Configures the database schema and shards based on the provided request.
- `POST /autopopulate`: Populates the database with random data for testing purposes.
- `POST /copy`: Copies all entries from the specified shards.
- `POST /read`: Reads entries from a specified shard within a given Stud_id range.
- `POST /write`: Writes a list of entries to a specified shard.
- `PUT /update`: Updates a specific entry in a specified shard.
- `POST /del`: Deletes a specific entry from a specified shard.

## Data Models

- `Schema`: Defines the database schema, including column names and data types.
- `ConfigRequest`: Defines the request body for the `/config` endpoint.
- `CopyRequest`: Defines the request body for the `/copy` endpoint.
- `Stud_id`: Defines a range of Stud_id values.
- `ReadRequest`: Defines the request body for the `/read` endpoint.
- `RowData`: Defines the data for a single row in the database.
- `WriteRequest`: Defines the request body for the `/write` endpoint.
- `UpdateRequest`: Defines the request body for the `/update` endpoint.
- `DeleteRequest`: Defines the request body for the `/del` endpoint.

## Database Connection

The script connects to a MySQL database using the `mysql.connector` library. The connection parameters are specified at the bottom of the script.

## Server Initialization

The server is initialized and started at the bottom of the script using the `uvicorn.run()` function. Before the server is started, the `initialize()` function is called to reset the database.

# lb_fast.py
## Documentation

### Overview

This module implements functionality for a distributed system utilizing FastAPI for HTTP server endpoints. It includes features for handling requests, managing server shards, consistent hashing, and server failure recovery.

### Dependencies

- `FastAPI`: FastAPI is used to create the HTTP server and handle incoming requests.
- `uvicorn`: Uvicorn is used to run the FastAPI application.
- `httpx`: HTTP client library used for making HTTP requests.
- `asyncio`: Asynchronous I/O library for concurrency.
- `log`: Custom logging module for logging system events.
- `database`: Module for interacting with the database.
- `typing`: Typing module for type hints.

### Functions and Endpoints

#### `generate_hostname(n)`

Generates a random hostname of length less than `n`.

- Parameters:
  - `n`: Maximum length of the generated hostname.

- Returns:
  - A randomly generated hostname string.

#### `ShardData`

Class representing shard data.

##### Attributes:

- `shard_id`: `str`. Unique identifier for the shard.
- `ch`: `ConsistentHashing`. Object for consistent hashing.
- `writeLock`: `asyncio.Lock`. Mutex lock for write operations.

#### `reapDeadServer(hostname)`

Coroutine function to remove a dead server from the list of dead servers after a specified time.

- Parameters:
  - `hostname`: Hostname of the server to be removed.

### Variables

#### `app`

Instance of `FastAPI` representing the main application.

#### `shardDataMap`

Dictionary mapping shard IDs to `ShardData` objects.

#### `failureLocks`

Dictionary mapping server hostnames to locks to prevent concurrent handling of failures.

#### `deadServers`

List of dead server hostnames.

#### `adminLock`

Lock for administrative operations.

#### `schemaConfig`

Dictionary containing schema configurations.

### Endpoints

#### `/init` (POST)

Endpoint to initialize the load balancer and spawn servers.

- Method: POST
- Request Body: `InitRequest`

##### Request Body Parameters:

- `shards`: List of shards to initialize.
- `servers`: Dictionary mapping server names to lists of shard IDs assigned to each server.

##### Responses:

- **200 OK**: Load balancer initialization and server spawning successful.
- **400 Bad Request**: If errors occur during initialization or spawning.

### Functionality

1. **Request Validation**:
   - Ensures the request is valid by checking for duplicate shard IDs and mismatch between shards and servers.

2. **Initialization**:
   - Inserts shard and mapping records into the database (`ShardT` and `MapT`).
   - Initializes consistent hashing ring for each shard.

3. **Server Spawning**:
   - Spawns server containers for each server specified in the request.
   - Configures servers with appropriate schema and shard mappings.
   - Adds servers to the consistent hashing ring of corresponding shards.

4. **Error Handling**:
   - Handles exceptions gracefully and provides appropriate error messages.

### Variables

- **`shardDataMap`**: Dictionary mapping shard IDs to `ShardData` objects.
- **`schemaConfig`**: Stores schema configurations for the initialized database.

### Note

- This endpoint is critical for the initialization and setup of the load balancer and servers in the distributed system. It ensures proper configuration and distribution of shards across servers.

#### `/status` (GET)

Endpoint to retrieve the status of the distributed system including schema information, shard records, and server assignments.

- Method: GET

##### Responses:

- **200 OK**: Successful retrieval of system status.

### Functionality

1. **Schema Retrieval**:
   - Retrieves the static schema information including column names and data types.

2. **Shard Records**:
   - Retrieves shard records from the database (`ShardT`) and formats them into a list of dictionaries.

3. **Server Information**:
   - Retrieves unique server names and their assigned shards from the database.

4. **Response Data**:
   - Constructs a JSON response containing the number of servers (`N`), schema information, shard details, and server-to-shard mappings.

### Variables

- **`schema`**: Static schema information containing column names and data types.
- **`shard_records`**: List of tuples representing shard records retrieved from the database.
- **`shards`**: Formatted list of dictionaries containing shard details.
- **`server_name_tuples`**: List of tuples representing unique server names retrieved from the database.
- **`server_names`**: List of unique server names.
- **`serverToShards`**: Dictionary mapping server names to lists of shard names assigned to each server.

### Note

- This endpoint provides essential information about the current state of the distributed system, including the schema configuration, shard distribution, and server assignments. It facilitates monitoring and management of the system.

#### `/add` (POST)

Endpoint to add new servers and shards to the distributed system.

- Method: POST
- Request Body: `AddRequest`

##### Request Body Parameters:

- `new_shards`: List of new shards to be added.
- `servers`: Dictionary mapping new server names to lists of shard IDs to be assigned to each server.

##### Responses:

- **200 OK**: Addition of servers and shards successful.
- **400 Bad Request**: If errors occur during addition.

#### `/rm` (DELETE)

Endpoint to remove servers from the distributed system.

- Method: DELETE
- Request Body: `RemoveRequest`

##### Request Body Parameters:

- `n`: Number of servers to remove.
- `servers`: List of server names to be removed.

##### Responses:

- **200 OK**: Removal of servers successful.
- **400 Bad Request**: If errors occur during removal.

### Functionality

1. **Adding Servers and Shards**:
   - Adds new shards to the database (`ShardT`) and initializes consistent hashing for each shard.
   - Spawns containers for new servers and adds them to the consistent hashing ring of appropriate shards.

2. **Removing Servers**:
   - Removes servers from the consistent hashing ring.
   - Handles scenarios where the number of servers to remove is more than existing servers by selecting random servers for removal.

3. **Error Handling**:
   - Gracefully handles exceptions and provides appropriate error messages.

### Variables

- **`new_server_names`**: List of new server names to be added.
- **`unique_servers`**: List of unique server names retrieved from the database.
- **`num_servers`**: Total number of servers after addition or removal.
- **`all_servers`**: Set of all existing servers in the system.
- **`message`**: Message indicating the servers added or removed.

### Note

- These endpoints provide functionality for dynamically scaling the distributed system by adding or removing servers and shards. Proper synchronization is ensured using the `adminLock` to prevent conflicts during these operations.


#### `/read` (POST)

Endpoint to read student records within a specified range.

- Method: POST
- Request Body: `ReadRequest`

##### Request Body Parameters:

- `Stud_id`: Dictionary containing low and high values for the student ID range to read.

##### Responses:

- **200 OK**: Successful retrieval of records.
- **500 Internal Server Error**: If an error occurs during record retrieval.

### Functionality

1. **Determine Shards**:
   - Determines which shards contain records within the specified student ID range.
   - Queries relevant shards for records.

2. **Read Records**:
   - Reads records from each relevant shard within the specified student ID range using the `read_from_shard` function.

3. **Response**:
   - Constructs a response containing the shards queried and the retrieved records.

4. **Error Handling**:
   - Logs errors encountered during record retrieval.
   - Returns a failure response if an error occurs.

### Note

- This endpoint facilitates the retrieval of student records within a specified range by querying the relevant shards in the distributed system. It ensures efficient distribution and retrieval of records across shards while handling errors gracefully.


#### `/write` (POST)

Endpoint to write data to the distributed system.

- Method: POST
- Request Body: `WriteRequest`

##### Request Body Parameters:

- `data`: List of records to be written.

##### Responses:

- **200 OK**: Successful completion of writes.
- **500 Internal Server Error**: If an error occurs during writes.

### Functionality

1. **Determining Shards**:
   - Determines which shards contain records to be written based on the provided data.

2. **Grouping Writes**:
   - Groups the writes by shard to minimize the number of requests.

3. **Writing Data**:
   - Performs writes to each shard, taking the appropriate mutex lock for each shard.
   - For each shard, writes data to all servers hosting that shard.

4. **Error Handling**:
   - Logs errors encountered during writes.
   - Rolls back writes and handles exceptions gracefully to ensure data consistency.
   - Returns a failure response if an error occurs.

### Note

- This endpoint allows for writing data to the distributed system while ensuring consistency and fault tolerance. It handles errors and rollbacks writes in case of failures to maintain data integrity across shards and servers.

#### `/update` (PUT)

Endpoint to update a record in the distributed system.

- Method: PUT
- Request Body: `UpdateRequest`

##### Request Body Parameters:

- `Stud_id`: Student ID of the record to be updated.
- `data`: New data to update the record with.

##### Responses:

- **200 OK**: Successful update of the record.
- **500 Internal Server Error**: If an error occurs during the update.

#### `/del` (DELETE)

Endpoint to delete a record from the distributed system.

- Method: DELETE
- Request Body: `DeleteRequest`

##### Request Body Parameters:

- `Stud_id`: Student ID of the record to be deleted.

##### Responses:

- **200 OK**: Successful deletion of the record.
- **500 Internal Server Error**: If an error occurs during the deletion.

### Functionality

- These endpoints serve as wrappers for the `modify_record` function, providing a standardized interface for updating and deleting records in the distributed system.

### Note

- The `modify_record` function handles the logic for updating and deleting records. These endpoints delegate the actual modification to that function, ensuring consistency in handling update and delete operations.


#### `/test` (GET)

Endpoint for testing purposes.

- Method: GET
- Returns a JSON response indicating the success of the test.

#### `/favicon.ico` (GET)

Endpoint to handle favicon requests.

- Method: GET
- Returns a 204 No Content response for favicon.ico requests.

### Functionality

- These endpoints serve auxiliary purposes such as testing and handling favicon requests.

### Note

- The `/test` endpoint provides a simple way to verify the functionality of the service.
- The `/favicon.ico` endpoint ensures that requests for favicon.ico return a 204 No Content response to prevent unnecessary requests.


---
### Functions

#### `read_from_shard(shard_id: str, low: int, high: int)`

Function for reading low to high student IDs from a shard.

- Parameters:
  - `shard_id`: `str`. ID of the shard to read from.
  - `low`: `int`. Lowest student ID to read.
  - `high`: `int`. Highest student ID to read.

- Returns:
  - JSON response containing the data retrieved from the shard.

### Functionality

- **Routing Request**:
  - Finds the nearest server for the request using consistent hashing.
  - Forwards the request to the nearest server for processing.

- **Handling Failures**:
  - If unable to route the request due to no available servers, logs an error and returns a 503 Service Unavailable response.
  - If an error occurs during the request, handles failure by invoking the `handle_failure` function.

- **Error Handling**:
  - Raises an exception if the request to the nearest server fails.
---
#### `get_shard_id(student_id: int)`

Function to determine the shard ID for a given student ID.

- Parameters:
  - `student_id`: `int`. Student ID to determine the shard for.

- Returns:
  - Shard ID associated with the given student ID.

### Note

- These functions are crucial for routing requests to the appropriate shards and handling failures gracefully. `read_from_shard` ensures that requests are directed to the nearest available server, while `get_shard_id` determines the shard ID for a given student ID based on the shard distribution in the database.
---
#### `modify_record(request_body)`

Function to modify a record in the distributed system.

- Parameters:
  - `request_body`: Request body containing data for modification.

- Returns:
  - JSON response indicating the success or failure of the modification.
---
### Functionality

1. **Determine Shard**:
   - Determines the shard containing the record based on the provided student ID.

2. **Retrieve Current Record**:
   - Retrieves the current record for the specified student ID from the shard.

3. **Update Record**:
   - Updates the record on all servers hosting the shard.
   - Handles both updates and deletions based on the type of request.

4. **Error Handling**:
   - Logs errors encountered during updates.
   - Rolls back updates and handles exceptions gracefully to ensure data consistency.
   - Returns appropriate failure responses in case of errors.

### Note

- This function allows for modifying records in the distributed system, ensuring consistency and fault tolerance. It handles errors and rollbacks updates in case of failures to maintain data integrity across shards and servers.
---
#### `delete_server(hostname, temporary=False)`

Function to delete a server from the distributed system.

- Parameters:
  - `hostname`: Hostname of the server to be deleted.
  - `temporary`: Optional. Boolean indicating whether the deletion is temporary. Default is `False`.

- Functionality:
  - Removes the server from the consistent hashing ring of all shards.
  - Deletes the server from the `MapT` table in the database if `temporary` is `False`.
  - Removes the server container.
---
#### `insert_server(hostname)`

Function to insert an existing server container back into the distributed system.

- Parameters:
  - `hostname`: Hostname of the server container to be inserted.

- Functionality:
  - Adds the server back to the consistent hashing ring for all shards it is responsible for.
---
#### `spawn_container(hostname)`

Function to spawn a new server container in the distributed system.

- Parameters:
  - `hostname`: Desired hostname for the new server container.

- Functionality:
  - Spawns a new server container using Docker.
  - Associates the container with the network `mynet`.
  - Sets the network alias to the hostname.
  - Logs success or failure of the container creation.

### Note

- These functions provide essential operations for managing servers in the distributed system, including adding, removing, and spawning server containers. They ensure proper maintenance and scaling of the system.
---
#### `remove_container(node_name)`

Function to remove a server container from the distributed system.

- Parameters:
  - `node_name`: Name of the server container to be removed.

- Functionality:
  - Removes the specified server container using Docker.
  - Logs success or failure of the container removal.
---
#### `handle_failure(hostname)`

Function to handle failure of a server in the distributed system.

- Parameters:
  - `hostname`: Hostname of the failed server.

- Functionality:
  - Checks the liveliness of the failed server.
  - Removes the failed server temporarily from the consistent hashing ring.
  - If the server is determined to be permanently down, spawns a new server container to replace it.
  - Copies data from the failed server's replicas to the new server.
  - Configures the new server and adds it to the consistent hashing ring.
  - Logs events and errors during the failure handling process.

### Note

- This function is crucial for maintaining the availability and reliability of the distributed system by handling server failures gracefully. It ensures that data redundancy and consistency are maintained in the face of server failures.


---

**`heartbeat_check()` Function:**

- **Description:**  
  Asynchronous coroutine to periodically check the liveliness of servers.

- **Functionality:**  
  This function runs an infinite loop to periodically check the liveliness of servers. It retrieves the list of servers in use, sends a heartbeat request to each server, and handles any server failures.

- **Raises:**  
  - `Exception`: If there's an error while trying to check the liveliness of a server.

---

**`main()` Function:**

- **Description:**  
  Main entry point for the application.

- **Functionality:**  
  This function sets up the event loop and configures the UVicorn server. It creates a task to run the `heartbeat_check()` function asynchronously.

- **Raises:**  
  - `Exception`: If there's an error while executing the main function.

---
