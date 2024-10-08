import os
from pydantic import BaseModel
from typing import Dict, List
import requests

from requests import auth

from .models import (
    AOI,
    AOICreate,
    DataRequest,
    DataRequestCreate,
    Reprojection,
    ReprojectionCreate,
    SnowflakeCredentials,
)

HTTP_TIMEOUT_SECONDS = 5

BASE_URL = f"https://dev-api.cecil.earth/v0"

# TODO: Documentation (Google style)
# TODO: Add retries


class Client:
    def __init__(self):
        self._base_url = BASE_URL
        self._auth = None

    def create_aoi(self, name: str, geometry: Dict) -> AOI:
        res = self._post(url="/aois", model=AOICreate(name=name, geometry=geometry))
        return AOI(**res)

    def get_aoi(self, id: str) -> AOI:
        res = self._get(url=f"/aois/{id}")
        return AOI(**res)

    def list_aois(self) -> List[AOI]:
        res = self._get(url="/aois")
        return [AOI(**record) for record in res["records"]]

    def create_data_request(self, aoi_id: str, dataset_id: str) -> DataRequest:
        res = self._post(
            url="/data-requests",
            model=DataRequestCreate(aoi_id=aoi_id, dataset_id=dataset_id),
        )
        return DataRequest(**res)

    def get_data_request(self, id: str) -> DataRequest:
        res = self._get(url=f"/data-requests/{id}")
        return DataRequest(**res)

    def list_data_requests(self):
        res = self._get(url="/data-requests")
        return [DataRequest(**record) for record in res["records"]]

    def create_reprojection(
        self, data_request_id: str, crs: str, resolution: float
    ) -> Reprojection:
        res = self._post(
            url="/reprojections",
            model=ReprojectionCreate(
                data_request_id=data_request_id,
                crs=crs,
                resolution=resolution,
            ),
        )
        return Reprojection(**res)

    def get_reprojection(self, id: str) -> Reprojection:
        res = self._get(url=f"/reprojections/{id}")
        return Reprojection(**res)

    def list_reprojections(self) -> List[Reprojection]:
        res = self._get(url="/reprojections")
        return [Reprojection(**record) for record in res["records"]]

    def _get_data_access_credentials(self) -> SnowflakeCredentials:
        res = self._get(url="/data-access-credentials")
        return SnowflakeCredentials(**res)

    def _request(self, method: str, url: str, **kwargs) -> Dict:

        self._set_auth()

        try:
            r = requests.request(
                method=method,
                url=self._base_url + url,
                auth=self._auth,
                timeout=HTTP_TIMEOUT_SECONDS,
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
            self._auth = auth.HTTPBasicAuth(username=api_key, password="")
        except KeyError:
            raise ValueError("environment variable CECIL_API_KEY not set") from None
