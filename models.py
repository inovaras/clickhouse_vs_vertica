from datetime import datetime
from uuid import UUID
from pydantic import BaseModel
from typing import Optional

class Event(BaseModel):
    event_type: str
    timestamp: datetime
    user_id: Optional[UUID] = None
    url: Optional[str] = None
