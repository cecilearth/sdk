import datetime
from typing import List, Optional

from .base import Base


class Webhook(Base):
    id: str
    url: str
    # Event types this webhook receives. None on API responses that predate
    # event selection.
    events: Optional[List[str]] = None
    created_at: datetime.datetime
    created_by: str
