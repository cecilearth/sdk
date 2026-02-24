import datetime

from .base import Base


class User(Base):
    id: str
    first_name: str
    last_name: str
    email: str
    created_at: datetime.datetime
    created_by: str
