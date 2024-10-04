import datetime
from enum import Enum
from typing import Dict, List, Optional

from pydantic import BaseModel, ConfigDict
from pydantic.alias_generators import to_camel


class SubRequestStatus(str, Enum):
    COMPLETED = "Completed"
    FAILED = "Failed"
    PROCESSING = "Processing"


class DataRequestStatus(str, Enum):
    COMPLETED = "Completed"
    PROCESSING = "Processing"


class AOI(BaseModel):
    model_config = ConfigDict(alias_generator=to_camel, populate_by_name=True)
    id: str
    name: str
    geometry: Dict
    hectares: float
    created: datetime.datetime


class AOICreate(BaseModel):
    model_config = ConfigDict(alias_generator=to_camel, populate_by_name=True)
    name: str
    geometry: Dict


class SubRequest(BaseModel):
    model_config = ConfigDict(alias_generator=to_camel, populate_by_name=True)
    external_id: str
    description: str
    status: SubRequestStatus
    error_message: Optional[str]


class DataRequest(BaseModel):
    model_config = ConfigDict(alias_generator=to_camel, populate_by_name=True)
    id: str
    aoi_id: str
    dataset_id: str
    sub_requests: List[SubRequest]
    status: DataRequestStatus
    created: datetime.datetime


class DataRequestCreate(BaseModel):
    model_config = ConfigDict(alias_generator=to_camel, populate_by_name=True)
    aoi_id: str
    dataset_id: str


class Reprojection(BaseModel):
    model_config = ConfigDict(alias_generator=to_camel, populate_by_name=True)
    id: str
    data_request_id: str
    crs: str
    resolution: float
    created: datetime.datetime


class ReprojectionCreate(BaseModel):
    model_config = ConfigDict(alias_generator=to_camel, populate_by_name=True)
    data_request_id: str
    crs: str
    resolution: float
