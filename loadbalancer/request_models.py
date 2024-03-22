from pydantic import BaseModel
from typing import List, Dict

# structures for deserializing JSON requests
class StudentModel(BaseModel):
    Stud_id: int
    Stud_name: str
    Stud_marks: int

class AdminRequest(BaseModel):
    n: int
    hostnames: List[str]

class ReadRequest(BaseModel):
    Stud_id: Dict[str, int]

class WriteRequest(BaseModel):
    data: List[StudentModel]
