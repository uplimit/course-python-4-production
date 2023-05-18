# =============================================================================
# HogwartsClass
# =============================================================================
from fastapi import FastAPI
from pydantic import BaseModel
from typing import Optional

app = FastAPI()

class HogwartsClass(BaseModel):
    id: int
    name: str
    professor: str
    description: str

# In-memory storage for Hogwarts classes
classes = {}

@app.get("/welcome")
async def hello_world():
    return {"message": "Welcome to Hogwarts School of Witchcraft and Wizardry"}

@app.get("/classes/{class_id}")
async def get_class(class_id: int):
    return classes.get(class_id, {"message": "Class not found"})

@app.post("/classes/")
async def create_class(hogwarts_class: HogwartsClass):
    classes[hogwarts_class.id] = hogwarts_class
    return {"message": "Class created successfully"}

@app.put("/classes/{class_id}")
async def update_class(class_id: int, hogwarts_class: HogwartsClass):
    if class_id in classes:
        classes[class_id] = hogwarts_class
        return {"message": "Class updated successfully"}
    else:
        return {"message": "Class not found"}

@app.delete("/classes/{class_id}")
async def delete_class(class_id: int):
    if class_id in classes:
        del classes[class_id]
        return {"message": "Class deleted successfully"}
    else:
        return {"message": "Class not found"}

class UpdateClass(BaseModel):
    name: Optional[str] = None
    professor: Optional[str] = None
    description: Optional[str] = None

@app.patch("/classes/{class_id}")
async def partial_update_class(class_id: int, update: UpdateClass):
    if class_id in classes:
        if update.name is not None:
            classes[class_id].name = update.name
        if update.professor is not None:
            classes[class_id].professor = update.professor
        if update.description is not None:
            classes[class_id].description = update.description
        return {"message": "Class partially updated successfully"}
    else:
        return {"message": "Class not found"}
