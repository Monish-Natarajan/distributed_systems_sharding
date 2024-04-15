from pydantic import BaseModel

class AddSlaveRequest(BaseModel):
    shard_id: str
    server_hostname: str

class RemoveSlaveRequest(BaseModel):
    shard_id: str
    server_hostname: str