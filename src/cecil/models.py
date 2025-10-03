import datetime
from typing import Dict, Optional, List

from pydantic import BaseModel, ConfigDict, SecretStr
from pydantic.alias_generators import to_camel


class AOI(BaseModel):
    model_config = ConfigDict(alias_generator=to_camel, populate_by_name=True)
    id: str
    external_ref: Optional[str]
    geometry: Dict
    hectares: float
    created_at: datetime.datetime
    created_by: str


class AOIRecord(BaseModel):
    model_config = ConfigDict(alias_generator=to_camel, populate_by_name=True)
    id: str
    external_ref: Optional[str]
    hectares: float
    created_at: datetime.datetime
    created_by: str


class AOICreate(BaseModel):
    model_config = ConfigDict(alias_generator=to_camel, populate_by_name=True)
    geometry: Dict
    external_ref: Optional[str]


class DataRequest(BaseModel):
    model_config = ConfigDict(alias_generator=to_camel, populate_by_name=True)
    id: str
    aoi_id: str
    dataset_id: str
    external_ref: Optional[str]
    created_at: datetime.datetime
    created_by: str


class DataRequestCreate(BaseModel):
    model_config = ConfigDict(alias_generator=to_camel, populate_by_name=True)
    aoi_id: str
    dataset_id: str
    external_ref: Optional[str]


class OrganisationSettings(BaseModel):
    model_config = ConfigDict(alias_generator=to_camel, populate_by_name=True)
    monthly_data_request_limit: Optional[int] = None


class RecoverAPIKey(BaseModel):
    model_config = ConfigDict(alias_generator=to_camel, populate_by_name=True)
    message: str


class RecoverAPIKeyRequest(BaseModel):
    model_config = ConfigDict(alias_generator=to_camel, populate_by_name=True)
    email: str


class RotateAPIKey(BaseModel):
    model_config = ConfigDict(alias_generator=to_camel, populate_by_name=True)
    new_api_key: str


class RotateAPIKeyRequest(BaseModel):
    model_config = ConfigDict(alias_generator=to_camel, populate_by_name=True)


class Transformation(BaseModel):
    model_config = ConfigDict(alias_generator=to_camel, populate_by_name=True)
    id: str
    data_request_id: str
    crs: str
    spatial_resolution: float
    created_at: datetime.datetime
    created_by: str


class TransformationCreate(BaseModel):
    model_config = ConfigDict(alias_generator=to_camel, populate_by_name=True)
    data_request_id: str
    crs: str
    spatial_resolution: float


class SnowflakeUserCredentials(BaseModel):
    model_config = ConfigDict(alias_generator=to_camel, populate_by_name=True)
    account: SecretStr
    user: SecretStr
    private_key: SecretStr


class User(BaseModel):
    model_config = ConfigDict(alias_generator=to_camel, populate_by_name=True)
    id: str
    first_name: str
    last_name: str
    email: str
    created_at: datetime.datetime
    created_by: str


class UserCreate(BaseModel):
    model_config = ConfigDict(alias_generator=to_camel, populate_by_name=True)
    first_name: str
    last_name: str
    email: str


class Band(BaseModel):
    model_config = ConfigDict(alias_generator=to_camel, populate_by_name=True)
    variable_name: str
    time: str
    time_pattern: str
    number: int


class File(BaseModel):
    model_config = ConfigDict(alias_generator=to_camel, populate_by_name=True)
    url: str
    bands: List[Band]


class DataRequestMetadata(BaseModel):
    model_config = ConfigDict(alias_generator=to_camel, populate_by_name=True)
    provider_name: str
    dataset_id: str
    dataset_name: str
    dataset_crs: str
    aoi_id: str
    data_request_id: str
    files: List[File]


class DataRequestParquetFiles(BaseModel):
    model_config = ConfigDict(alias_generator=to_camel, populate_by_name=True)
    files: List[str]
