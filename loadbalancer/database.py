import mysql.connector
from typing import List
import log

USER = "ravanan"
PASSWORD = "testing"
HOST = "localhost"
DB = "load_balancer_database"

try:
    conn = mysql.connector.connect(
        user=USER,
        password=PASSWORD,
        host=HOST,
        database=DB
    )
    # create the ShardT and MapT tables if they don't exist
    with conn.cursor() as cursor:
        cursor.execute("CREATE TABLE IF NOT EXISTS ShardT (Stud_Id_low INT, Shard_id VARCHAR(255), Shard_size INT, valid_idx INT)")
        cursor.execute("CREATE TABLE IF NOT EXISTS MapT (Shard_id VARCHAR(255), Server_id VARCHAR(255)")
        conn.commit()
except Exception as e:
    log(f"Error connecting to load_balancer_database: {e}")

class ShardRecord:
    Stud_Id_low: int
    Shard_id: str
    Shard_size: int
    valid_idx: int

class MapRecord:
    Shard_id: str
    Server_id: str

def get_shards():
    with conn.cursor() as cursor:
        # order by Stud_Id_low so that we can easily find the shard for a given student id
        query = "SELECT * FROM ShardT ORDER BY Stud_Id_low"
        cursor.execute(query)
        return cursor.fetchall()

def insert_shard_record(record: ShardRecord) -> None:
    with conn.cursor() as cursor:
        query = f"INSERT INTO ShardT VALUES ({record.Stud_Id_low}, {record.Shard_id}, {record.Shard_size}, {record.valid_idx})"
        cursor.execute(query)
        conn.commit()

def insert_map_record(record: MapRecord) -> None:
    with conn.cursor() as cursor:
        query = f"INSERT INTO MapT VALUES ({record.Shard_id}, {record.Server_id})"
        cursor.execute(query)
        conn.commit()

# if a server goes down, all the shards mapped to it should be removed from MapT
def delete_map_server(server_id: str) -> None:
    with conn.cursor() as cursor:
        query = f"DELETE FROM MapT WHERE Server_id = {server_id}"
        cursor.execute(query)
        conn.commit()

# shard can be mapped to multiple servers, return list of servers for a shard
def get_servers_for_shard(shard_id: str):
    with conn.cursor() as cursor:
        query = f"SELECT Server_id FROM MapT WHERE Shard_id = {shard_id}"
        cursor.execute(query)
        return cursor.fetchall()
