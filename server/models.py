from pydantic import BaseModel, Field
from typing import List, Dict, TypedDict
class AddSlaveRequest(BaseModel):
    shard_id: str
    server_hostname: str

class RemoveSlaveRequest(BaseModel):
    shard_id: str
    server_hostname: str

class Schema(TypedDict):
    columns: List[str]
    dtypes: List[str]

class ConfigRequest(BaseModel):
    schema_: Schema = Field(alias='schema')
    shards: List[str]

class CopyRequest(BaseModel):
    shards: List[str]

class Stud_id(TypedDict):
    low: int
    high: int

class ReadRequest(BaseModel):
    shard: str
    Stud_id: Stud_id

class RowData(TypedDict):
    Stud_id: int
    Stud_name: str
    Stud_marks: float

class UpdateRequest(BaseModel):
    shard: str
    Stud_id: int
    data: RowData

class DeleteRequest(BaseModel):
    shard: str
    Stud_id: int


class WriteRequest(BaseModel):
    shard: str
    data: List[RowData]

class PrimaryElectRequest(BaseModel):
    shard: str
    replicas: List[str]