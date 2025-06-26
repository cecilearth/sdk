import datetime
from typing import Dict

from pydantic import BaseModel, ConfigDict, SecretStr
from pydantic.alias_generators import to_camel


class AOI(BaseModel):
    model_config = ConfigDict(alias_generator=to_camel, populate_by_name=True)
    id: str
    name: str
    geometry: Dict
    hectares: float
    created_at: datetime.datetime
    created_by: str


class AOICreate(BaseModel):
    model_config = ConfigDict(alias_generator=to_camel, populate_by_name=True)
    name: str
    geometry: Dict


class DataRequest(BaseModel):
    model_config = ConfigDict(alias_generator=to_camel, populate_by_name=True)
    id: str
    aoi_id: str
    dataset_id: str
    created_at: datetime.datetime
    created_by: str


class DataRequestCreate(BaseModel):
    model_config = ConfigDict(alias_generator=to_camel, populate_by_name=True)
    aoi_id: str
    dataset_id: str


class OrganisationCreate(BaseModel):
    model_config = ConfigDict(alias_generator=to_camel, populate_by_name=True)
    name: str


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


class SnowflakeCredentials(BaseModel):
    model_config = ConfigDict(alias_generator=to_camel, populate_by_name=True)
    account: SecretStr
    user: SecretStr
    password: SecretStr


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


class SignUpRequest(BaseModel):
    model_config = ConfigDict(alias_generator=to_camel, populate_by_name=True)
    organisation: OrganisationCreate
    user: UserCreate


class SignUpResponse(BaseModel):
    model_config = ConfigDict(alias_generator=to_camel, populate_by_name=True)
    message: str
