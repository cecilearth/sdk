import os

import pandas as pd
import requests
import snowflake.connector
import xarray

from pydantic import BaseModel
from requests import auth
from cryptography.hazmat.primitives import serialization
from typing import Dict, List, Optional
from warnings import warn

from .errors import (
    Error,
    _handle_bad_request,
    _handle_method_not_allowed,
    _handle_not_found,
    _handle_too_many_requests,
    _handle_unprocessable_entity,
)
from .models import (
    AOI,
    AOIRecord,
    AOICreate,
    DataRequest,
    DataRequestCreate,
    OrganisationSettings,
    RecoverAPIKey,
    RecoverAPIKeyRequest,
    RotateAPIKey,
    RotateAPIKeyRequest,
    SnowflakeUserCredentials,
    Transformation,
    TransformationCreate,
    User,
    UserCreate,
    DataRequestMetadata,
    DataRequestParquetFiles,
)
from .version import __version__
from .xarray import load_xarray


class Client:
    def __init__(self, env: str = None) -> None:
        self._api_auth = None
        self._base_url = (
            "https://api.cecil.earth" if env is None else f"https://{env}.cecil.earth"
        )
        self._snowflake_user_creds = None

    def create_aoi(self, geometry: Dict, external_ref: Optional[str] = None) -> AOI:
        # TODO: validate geometry
        res = self._post(
            url="/v0/aois",
            model=AOICreate(geometry=geometry, external_ref=external_ref),
        )
        return AOI(**res)

    def get_aoi(self, id: str) -> AOI:
        res = self._get(url=f"/v0/aois/{id}")
        return AOI(**res)

    def list_aois(self) -> List[AOIRecord]:
        res = self._get(url="/v0/aois")
        return [AOIRecord(**record) for record in res["records"]]

    def create_data_request(
        self, aoi_id: str, dataset_id: str, external_ref: Optional[str] = None
    ) -> DataRequest:
        res = self._post(
            url="/v0/data-requests",
            model=DataRequestCreate(
                aoi_id=aoi_id, dataset_id=dataset_id, external_ref=external_ref
            ),
        )
        return DataRequest(**res)

    def get_data_request(self, id: str) -> DataRequest:
        res = self._get(url=f"/v0/data-requests/{id}")
        return DataRequest(**res)

    def list_data_requests(self) -> List[DataRequest]:
        res = self._get(url="/v0/data-requests")
        return [DataRequest(**record) for record in res["records"]]

    def load_xarray(self, data_request_id: str) -> xarray.Dataset:
        res = self._get(url=f"/v0/data-requests/{data_request_id}/metadata")
        metadata = DataRequestMetadata(**res)
        return load_xarray(metadata)

    def load_dataframe(self, data_request_id: str) -> pd.DataFrame:
        res = self._get(url=f"/v0/data-requests/{data_request_id}/parquet-files")
        metadata = DataRequestParquetFiles(**res)
        df = pd.concat((pd.read_parquet(f) for f in metadata.files))
        return df[
            [col for col in df.columns if col not in ("organisation_id", "created_at")]
        ]

    def create_transformation(
        self, data_request_id: str, crs: str, spatial_resolution: float
    ) -> Transformation:
        warn(
            "create_transformation() is deprecated, refer to https://github.com/cecilearth/examples",
            DeprecationWarning,
            stacklevel=2,
        )
        res = self._post(
            url="/v0/transformations",
            model=TransformationCreate(
                data_request_id=data_request_id,
                crs=crs,
                spatial_resolution=spatial_resolution,
            ),
        )
        return Transformation(**res)

    def get_transformation(self, id: str) -> Transformation:
        warn(
            "get_transformation() is deprecated.",
            DeprecationWarning,
            stacklevel=2,
        )
        res = self._get(url=f"/v0/transformations/{id}")
        return Transformation(**res)

    def list_transformations(self) -> List[Transformation]:
        warn(
            "list_transformations() is deprecated.",
            DeprecationWarning,
            stacklevel=2,
        )
        res = self._get(url="/v0/transformations")
        return [Transformation(**record) for record in res["records"]]

    def query(self, sql: str) -> pd.DataFrame:
        warn(
            "query() is deprecated, use load_xarray() or load_dataframe() instead.",
            DeprecationWarning,
            stacklevel=2,
        )
        return self._query(sql)

    def _query(self, sql: str) -> pd.DataFrame:
        if self._snowflake_user_creds is None:
            res = self._get(url="/v0/snowflake-user-credentials")
            self._snowflake_user_creds = SnowflakeUserCredentials(**res)

        private_key = serialization.load_pem_private_key(
            self._snowflake_user_creds.private_key.get_secret_value().encode(),
            password=None,
        )

        with snowflake.connector.connect(
            account=self._snowflake_user_creds.account.get_secret_value(),
            user=self._snowflake_user_creds.user.get_secret_value(),
            private_key=private_key,
        ) as conn:
            df = conn.cursor().execute(sql).fetch_pandas_all()
            df.columns = [x.lower() for x in df.columns]

            return df

    def recover_api_key(self, email: str) -> RecoverAPIKey:
        res = self._post(
            url="/v0/api-key/recover",
            model=RecoverAPIKeyRequest(email=email),
            skip_auth=True,
        )

        return RecoverAPIKey(**res)

    def rotate_api_key(self) -> RotateAPIKey:
        res = self._post(url=f"/v0/api-key/rotate", model=RotateAPIKeyRequest())

        return RotateAPIKey(**res)

    def create_user(self, first_name: str, last_name: str, email: str) -> User:
        res = self._post(
            url="/v0/users",
            model=UserCreate(
                first_name=first_name,
                last_name=last_name,
                email=email,
            ),
        )
        return User(**res)

    def get_user(self, id: str) -> User:
        res = self._get(url=f"/v0/users/{id}")
        return User(**res)

    def list_users(self) -> List[User]:
        res = self._get(url="/v0/users")
        return [User(**record) for record in res["records"]]

    def get_organisation_settings(self) -> OrganisationSettings:
        res = self._get(url="/v0/organisation/settings")
        return OrganisationSettings(**res)

    def update_organisation_settings(
        self,
        *,
        monthly_data_request_limit,
    ) -> OrganisationSettings:
        res = self._post(
            url="/v0/organisation/settings",
            model=OrganisationSettings(
                monthly_data_request_limit=monthly_data_request_limit,
            ),
        )
        return OrganisationSettings(**res)

    def _request(self, method: str, url: str, skip_auth=False, **kwargs) -> Dict:

        if skip_auth is False:
            self._set_auth()

        headers = {"cecil-python-sdk-version": __version__}

        try:
            r = requests.request(
                method=method,
                url=self._base_url + url,
                auth=self._api_auth,
                headers=headers,
                timeout=None,
                **kwargs,
            )
            r.raise_for_status()
            return r.json()

        except requests.exceptions.ConnectionError:
            raise Error("failed to connect to the Cecil Platform")

        except requests.exceptions.HTTPError as err:
            message = f"Request failed with status code {err.response.status_code}"
            if err.response.text != "":
                message += f": {err.response.text}"

            match err.response.status_code:
                case 400:
                    _handle_bad_request(err.response)
                case 401:
                    raise Error("unauthorised")
                case 404:
                    _handle_not_found(err.response)
                case 405:
                    _handle_method_not_allowed(err.response)
                case 422:
                    _handle_unprocessable_entity(err.response)
                case 429:
                    _handle_too_many_requests(err.response)
                case 500:
                    raise Error("internal server error")
                case _:
                    raise Error(
                        f"request failed with code {err.response.status_code}",
                        err.response.text,
                    )

    def _get(self, url: str, **kwargs) -> Dict:
        return self._request(method="get", url=url, **kwargs)

    def _post(self, url: str, model: BaseModel, skip_auth=False, **kwargs) -> Dict:
        return self._request(
            method="post",
            url=url,
            json=model.model_dump(by_alias=True),
            skip_auth=skip_auth,
            **kwargs,
        )

    def _set_auth(self) -> None:
        try:
            api_key = os.environ["CECIL_API_KEY"]
            self._api_auth = auth.HTTPBasicAuth(username=api_key, password="")
        except KeyError:
            raise ValueError("environment variable CECIL_API_KEY not set") from None
