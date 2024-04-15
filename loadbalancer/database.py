import mysql.connector
from mysql.connector import Error
from typing import List
from log import log
from time import sleep

USER = "root"
PASSWORD = "testing"
HOST = "localhost"
DB = "load_balancer_database"

try:
    # busy loop with a sleep of 1 second until the DB server is up
    retries = 5
    while retries:
        try:
            conn = mysql.connector.connect(
                user=USER,
                password=PASSWORD,
                host=HOST,
                database=DB
            )
            log("load_balancer -- Connected to load_balancer_database")
            break
        except Error as e:
            retries -= 1
            log(f"load_balancer -- Error connecting to load_balancer_database: {e}")
            log(f"load_balancer -- {retries} retries left")
            if retries > 0:
                sleep(1)
            else:
                log("load_balancer -- Exiting because of DB connection error")
                exit(1)
    # create the ShardT and MapT tables if they don't exist
    with conn.cursor() as cursor:
        cursor.execute("CREATE TABLE IF NOT EXISTS ShardT (Stud_id_low INT, Shard_id VARCHAR(255) PRIMARY KEY, Shard_size INT)")
        cursor.execute("CREATE TABLE IF NOT EXISTS MapT (Shard_id VARCHAR(255), Server_id VARCHAR(255), IsPrimary BOOLEAN)")
        conn.commit()
except Error as e:
    print(f"Error connecting to load_balancer_database: {e}")

class ShardRecord:
    def __init__(self, Stud_id_low: int, Shard_id: str, Shard_size: int):
        self.Stud_id_low = Stud_id_low
        self.Shard_id = Shard_id
        self.Shard_size = Shard_size

class MapRecord:
    def __init__(self, Shard_id: str, Server_id: str, IsPrimary: bool):
        self.Shard_id = Shard_id
        self.Server_id = Server_id
        self.IsPrimary = IsPrimary

def get_shards():
    with conn.cursor() as cursor:
        # order by Stud_id_low so that we can easily find the shard for a given student id
        query = "SELECT * FROM ShardT ORDER BY Stud_id_low"
        cursor.execute(query)
        return cursor.fetchall()
    
def get_unique_servers():
    with conn.cursor() as cursor:
        query = "SELECT DISTINCT Server_id FROM MapT"
        cursor.execute(query)
        return [x[0] for x in cursor.fetchall()]

def insert_shard_record(record: ShardRecord) -> None:
    with conn.cursor() as cursor:
        query = f"INSERT INTO ShardT VALUES ({record.Stud_id_low}, '{record.Shard_id}', {record.Shard_size})"
        cursor.execute(query)
        conn.commit()

def insert_map_record(record: MapRecord) -> None:
    with conn.cursor() as cursor:
        query = f"INSERT INTO MapT VALUES ('{record.Shard_id}', '{record.Server_id}', {record.IsPrimary})"
        cursor.execute(query)
        conn.commit()

# if a server goes down, all the shards mapped to it should be removed from MapT
def delete_map_server(server_id: str) -> None:
    with conn.cursor() as cursor:
        query = f"DELETE FROM MapT WHERE Server_id = '{server_id}'"
        cursor.execute(query)
        conn.commit()

# shard can be mapped to multiple servers, return list of servers for a shard
def get_servers_for_shard(shard_id: str):
    with conn.cursor() as cursor:
        query = f"SELECT Server_id FROM MapT WHERE Shard_id = '{shard_id}'"
        cursor.execute(query)
        return [x[0] for x in cursor.fetchall()]

# get shards that a particular server services
def get_shards_for_server(server_id: str):
    with conn.cursor() as cursor:
        query = f"SELECT Shard_id FROM MapT WHERE Server_id = '{server_id}'"
        cursor.execute(query)
        # result would be a list of tuples, return a list of the first and only element
        # of each tuple
        return [x[0] for x in cursor.fetchall()]

# mark the given server as a primary server
def mark_as_primary(server_id: str) -> None:
    with conn.cursor() as cursor:
        query = f"UPDATE MapT SET IsPrimary = TRUE WHERE Server_id = '{server_id}'"
        cursor.execute(query)
        conn.commit()

def primary_for_shard(shard_id: str):
    # query the MapT table and see which server is marked as primary for this shard
    with conn.cursor() as cursor:
        query = f"SELECT Server_id FROM MapT WHERE Shard_id = '{shard_id}' AND IsPrimary = TRUE"
        cursor.execute(query)
        result = cursor.fetchone()
        if result:
            return result[0]
        else:
            return None
def set_primary(shard_id: str, hostname: str):
    # set the given server as the primary server for the given shard
    # and unset all other servers as primary
    with conn.cursor() as cursor:
        query = f"UPDATE MapT SET IsPrimary = FALSE WHERE Shard_id = '{shard_id}'"
        cursor.execute(query)
        query = f"UPDATE MapT SET IsPrimary = TRUE WHERE Shard_id = '{shard_id}' AND Server_id = '{hostname}'"
        cursor.execute(query)
        conn.commit()

def get_primaries(hostname: str):
    # get all the shards for which the given server is the primary server
    with conn.cursor() as cursor:
        query = f"SELECT Shard_id FROM MapT WHERE Server_id = '{hostname}' AND IsPrimary = TRUE"
        cursor.execute(query)
        return [x[0] for x in cursor.fetchall()]