
from typing import List, Dict, Union
from pydantic import BaseModel


class ProcessStatus(BaseModel):
    process_id: str
    file_name: Union[str, None] = None
    file_path: Union[str, None] = None
    description: Union[str, None] = None
    start_time: str
    end_time: Union[str, None] = None
    total_time: Union[int, None] = None
