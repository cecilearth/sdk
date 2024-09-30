from enum import Enum
from pydantic import BaseModel
from typing import Dict, List, Optional
import datetime


class AOI(BaseModel):
    ID: str
    Name: str
    Geometry: Dict
    Hectares: Optional[float]  # TODO: Add hectares to model once Create endpoints returns it
    Created: datetime.datetime


class SubRequestStatus(str, Enum):
    COMPLETED = "Completed"
    FAILED = "Failed"
    PROCESSING = "Processing"


class DataRequestStatus(str, Enum):
    COMPLETED = "Completed"
    PROCESSING = "Processing"


class SubRequest(BaseModel):
    ExternalID: str
    Description: str
    Status: SubRequestStatus
    ErrorMessage: Optional[str]


class DataRequest(BaseModel):
    ID: str
    AOIID: str
    DatasetID: str
    SubRequests: List[SubRequest]
    # Status: DataRequestStatus # TODO: add once implemented
    Created: datetime.datetime


class Reprojection(BaseModel):
    ID: str
    DataRequestID: str
    CRS: str
    Resolution: float
    Created: datetime.datetime
