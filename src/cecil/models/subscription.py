import datetime
from typing import Dict, List, Optional

from .base import Base


class Subscription(Base):
    id: str
    aoi_id: str
    dataset_id: str
    # Publication of the dataset this subscription was created under, and the
    # dataset's current publication. They differ when a newer publication
    # exists. Either may be None for subscriptions the API does not (yet) pin.
    dataset_publication: Optional[str] = None
    dataset_current_publication: Optional[str] = None
    # Delivery status: pending / processing / completed / partial / failed,
    # and a message when there is something to say (the provider's error, or
    # how long the pipeline has been quiet). None on API responses that
    # predate the field.
    status: Optional[str] = None
    status_message: Optional[str] = None
    external_ref: Optional[str] = None
    created_at: datetime.datetime
    created_by: str
    archived_at: Optional[datetime.datetime] = None
    archived_by: Optional[str] = None


class SubscriptionFormat(Base):
    format: str


class SubscriptionStorage(Base):
    storage: str


class SubscriptionSelfHostedParquet(Base):
    aoi_id: str
    subscription_id: str
    geometry: Dict
    bucket: "S3Bucket"
    credentials: "S3BucketCredentials"
    allowed_actions: List[str]


class SubscriptionParquet(Base):
    files: List[str]


class SubscriptionTIFF(Base):
    provider_name: str
    dataset_id: str
    dataset_name: str
    aoi_id: str
    subscription_id: str
    bucket: "S3Bucket"
    credentials: "S3BucketCredentials"
    allowed_actions: List[str]
    file_mapping: Dict[str, "File"]


class SubscriptionZarr(Base):
    provider_name: str
    dataset_id: str
    dataset_name: str
    aoi_id: str
    subscription_id: str
    geometry: Dict
    bucket: "S3Bucket"
    credentials: "S3BucketCredentials"
    allowed_actions: List[str]


class S3Bucket(Base):
    name: str
    prefix: str


class S3BucketCredentials(Base):
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
    scale: Optional[float] = None
    offset: Optional[float] = None
