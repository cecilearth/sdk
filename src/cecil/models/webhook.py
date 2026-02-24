import datetime

from .base import Base


class Webhook(Base):
    id: str
    url: str
    created_at: datetime.datetime
    created_by: str
