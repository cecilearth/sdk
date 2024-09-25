from pydantic import BaseModel
import datetime


class DataRequest(BaseModel):
    ID: str
    OrganisationID: str
    AOIID: str
    DatasetID: str
    Created: datetime.datetime
