from pydantic import BaseModel, Field
from typing import List, Dict

# structures for deserializing JSON requests
class StudentModel(BaseModel):
    Stud_id: int
    Stud_name: str
    Stud_marks: int


# deserialize the following JSON request:
# payload_json = {
# "N":3,
# "schema": {
#     "columns":["Stud_id","Stud_name","Stud_marks"],
#     "dtypes":["Number","String","String"]
#     },
# "shards": [
#     {"Stud_id_low":0, "Shard_id": "sh1", "Shard_size":4096},
#     {"Stud_id_low":4096, "Shard_id": "sh2", "Shard_size":4096},
#     {"Stud_id_low":8192, "Shard_id": "sh3", "Shard_size":4096},
#     ],
# "servers": {
#         "Server0":["sh1","sh2"],
#         "Server1":["sh2","sh3"],
#         "ANY_NAME":["sh1","sh3"],
#         }
# }

class ShardSchemaModel(BaseModel):
    Stud_id_low: int
    Shard_id: str
    Shard_size: int

class SchemaModel(BaseModel):
    columns: List[str]
    dtypes: List[str]

class InitRequest(BaseModel):
    schema_: SchemaModel = Field(alias='schema')
    shards: List[ShardSchemaModel] # List[Dict[str, int]]
    servers: Dict[str, List[str]] # server_name is the key, value is list of shards


class AdminRequest(BaseModel):
    n: int
    hostnames: List[str]


class ReadRequest(BaseModel):
    Stud_id: Dict[str, int]


class WriteRequest(BaseModel):
    data: List[StudentModel]

class UpdateRequest(BaseModel):
    Stud_id: int
    data: StudentModel

class DeleteRequest(BaseModel):
    Stud_id: int

class AddRequest(BaseModel):
    n: int
    new_shards: List[ShardSchemaModel]
    servers: Dict[str, List[str]]

class RemoveRequest(BaseModel):
    n: int
    servers: List[str] 