import datetime
from typing import Dict, Optional

from .base import Base


class AOI(Base):
    id: str
    external_ref: Optional[str] = None
    geometry: Optional[Dict] = None
    hectares: float
    created_at: datetime.datetime
    created_by: str
    archived_at: Optional[datetime.datetime] = None
    archived_by: Optional[str] = None
