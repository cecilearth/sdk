import datetime
from typing import Dict, List, Optional

from .base import Base


class Subscription(Base):
    id: str
    aoi_id: str
    dataset_id: str
    external_ref: Optional[str]
    created_at: datetime.datetime
    created_by: str
    archived_at: Optional[datetime.datetime] = None
    archived_by: Optional[str] = None


class SubscriptionListFiles(Base):
    provider_name: str
    dataset_id: str
    dataset_name: str
    aoi_id: str
    subscription_id: str
    bucket: "Bucket"
    credentials: "BucketCredentials"
    allowed_actions: List
    file_mapping: Dict[str, "File"]


class SubscriptionParquetFiles(Base):
    files: List[str]


class Bucket(Base):
    name: str
    prefix: str


class BucketCredentials(Base):
    access_key_id: str
    secret_access_key: str
    session_token: str
    region: str
    expiration: datetime.datetime


class File(Base):
    bands: List["Band"]


class Band(Base):
    number: int
    name: str
    dtype: str
    nodata: Optional[float | int] = None
