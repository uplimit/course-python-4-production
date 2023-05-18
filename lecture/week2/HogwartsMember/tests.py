# =============================================================================
# HogwartsMember
# =============================================================================
from fastapi import FastAPI
from pydantic import BaseModel
from typing import Optional

app = FastAPI()

class HogwartsMember(BaseModel):
    name: str
    house: Optional[str] = None
    year: Optional[int] = None

members = [
    {"name": "Harry Potter", "house": "Gryffindor", "year": 5},
    {"name": "Hermione Granger", "house": "Gryffindor", "year": 5},
]

@app.get("/hogwarts/members/{member_id}")
def get_member(member_id: int):
    if 0 <= member_id < len(members):
        return members[member_id]
    return {"error": "Member not found"}

@app.post("/hogwarts/members")
def add_member(member: HogwartsMember):
    members.append(member.dict())
    return {"success": True, "member_id": len(members) - 1}# 

