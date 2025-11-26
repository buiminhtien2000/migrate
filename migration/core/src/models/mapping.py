# src/models/mapping.py
from datetime import datetime
from typing import Optional
from pydantic import BaseModel

class EntityMapping(BaseModel):
    id: Optional[int]
    processor_type: str
    source_id: str
    target_id: str
    created_at: Optional[datetime]

    class Config:
        orm_mode = True