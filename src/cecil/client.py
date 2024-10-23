import os
from pydantic import BaseModel
from typing import Dict, List
import requests

from requests import auth

import snowflake.connector

from .models import (
    AOI,
    AOICreate,
    DataRequest,
    DataRequestCreate,
    Reprojection,
    ReprojectionCreate,
    SnowflakeCredentials,
)

# TODO: Documentation (Google style)
# TODO: Add HTTP retries


class Client:
    def __init__(self, env="dev"):
        self._api_auth = None
        self._base_url = f"https://{env}-api.cecil.earth"  # TODO: hard-code to prod URL
        self._snowflake_creds = None

    def create_aoi(self, name: str, geometry: Dict) -> AOI:
        # TODO: validate geometry
        res = self._post(url="/v0/aois", model=AOICreate(name=name, geometry=geometry))
        return AOI(**res)

    def get_aoi(self, id: str) -> AOI:
        res = self._get(url=f"/v0/aois/{id}")
        return AOI(**res)

    def list_aois(self) -> List[AOI]:
        res = self._get(url="/v0/aois")
        return [AOI(**record) for record in res["records"]]

    def create_data_request(self, aoi_id: str, dataset_id: str) -> DataRequest:
        res = self._post(
            url="/v0/data-requests",
            model=DataRequestCreate(aoi_id=aoi_id, dataset_id=dataset_id),
        )
        return DataRequest(**res)

    def get_data_request(self, id: str) -> DataRequest:
        res = self._get(url=f"/v0/data-requests/{id}")
        return DataRequest(**res)

    def list_data_requests(self):
        res = self._get(url="/v0/data-requests")
        return [DataRequest(**record) for record in res["records"]]

    def create_reprojection(
        self, data_request_id: str, crs: str, resolution: float
    ) -> Reprojection:
        # TODO: check if data request is completed before creating reprojection
        res = self._post(
            url="/v0/reprojections",
            model=ReprojectionCreate(
                data_request_id=data_request_id,
                crs=crs,
                resolution=resolution,
            ),
        )
        return Reprojection(**res)

    def get_reprojection(self, id: str) -> Reprojection:
        res = self._get(url=f"/v0/reprojections/{id}")
        return Reprojection(**res)

    def list_reprojections(self) -> List[Reprojection]:
        res = self._get(url="/v0/reprojections")
        return [Reprojection(**record) for record in res["records"]]

    def query(self, sql):
        if self._snowflake_creds is None:
            res = self._get(url="/v0/data-access-credentials")
            self._snowflake_creds = SnowflakeCredentials(**res)

        with snowflake.connector.connect(
            account=self._snowflake_creds.account.get_secret_value(),
            user=self._snowflake_creds.user.get_secret_value(),
            password=self._snowflake_creds.password.get_secret_value(),
        ) as conn:
            df = conn.cursor().execute(sql).fetch_pandas_all()
            df.columns = [x.lower() for x in df.columns]

            return df

    def _request(self, method: str, url: str, **kwargs) -> Dict:

        self._set_auth()

        try:
            r = requests.request(
                method=method,
                url=self._base_url + url,
                auth=self._api_auth,
                timeout=None,
                **kwargs,
            )
            r.raise_for_status()
            return r.json()

        except requests.exceptions.ConnectionError as err:
            raise ValueError("Connection error") from err
        except requests.exceptions.HTTPError as err:
            if err.response.status_code == 403:
                raise ValueError("Authentication error") from err
            else:
                raise

    def _get(self, url: str, **kwargs) -> Dict:
        return self._request(method="get", url=url, **kwargs)

    def _post(self, url: str, model: BaseModel, **kwargs) -> Dict:
        return self._request(
            method="post", url=url, json=model.model_dump(by_alias=True), **kwargs
        )

    def _set_auth(self) -> None:
        try:
            api_key = os.environ["CECIL_API_KEY"]
            self._api_auth = auth.HTTPBasicAuth(username=api_key, password="")
        except KeyError:
            raise ValueError("environment variable CECIL_API_KEY not set") from None
