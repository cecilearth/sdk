from typing import Dict, List

import requests

from .models import AOI, DataRequest, Reprojection

HTTP_TIMEOUT_SECONDS = 3

BASE_URL = f"https://dev-api.cecil.earth/v0"
ORGANISATION_ID = "6898268f-9288-4296-8d5b-ae68517bb65e"
CECIL_ORGANISATION_ID_HEADER = "cecil-organisation-id"


# TODO: Documentation (Google style)
# TODO: Linting (black)

class Client:
    def __init__(self):
        self._base_url = BASE_URL
        self._organisation_id = ORGANISATION_ID

    def create_aoi(self, name: str, geometry: Dict) -> AOI:
        res = self._post("/aois", json={"Name": name, "Geometry": geometry})
        return AOI(**res.json())

    def get_aoi(self, id: str) -> AOI:
        res = self._get(f"/aois/{id}")
        return AOI(**res.json())

    def list_aois(self) -> List[AOI]:
        res = self._get("/aois")
        return [AOI(**record) for record in res.json()["Records"]]

    def create_data_request(self, aoi_id: str, dataset_id: str) -> DataRequest:
        res = self._post("/data-requests", json={"AOIID": aoi_id, "DatasetID": dataset_id})
        return DataRequest(**res.json())

    def get_data_request(self, id: str) -> DataRequest:
        res = self._get(f"/data-requests/{id}")
        return DataRequest(**res.json())

    def list_data_requests(self):
        res = self._get("/data-requests")
        return [DataRequest(**record) for record in res.json()["Records"]]

    def create_reprojection(self, data_request_id: str, crs: str, resolution: float) -> Reprojection:
        res = self._post(
            "/reprojections",
            json={
                "DataRequestID": data_request_id,
                "CRS": crs,
                "Resolution": resolution,
            })
        return Reprojection(**res.json())

    def get_reprojection(self, id: str) -> Reprojection:
        res = self._get(f"/reprojections/{id}")
        return Reprojection(**res.json())

    def list_reprojections(self) -> List[Reprojection]:
        res = self._get("/reprojections")
        return [Reprojection(**record) for record in res.json()["Records"]]

    def _request(self, method, url, **kwargs):

        if kwargs is None:
            kwargs = {}

        if "headers" not in kwargs:
            kwargs["headers"] = {}

        kwargs["headers"][CECIL_ORGANISATION_ID_HEADER] = self._organisation_id
        kwargs["timeout"] = HTTP_TIMEOUT_SECONDS

        try:
            r = requests.request(method, self._base_url + url, **kwargs)
            r.raise_for_status()
            return r

        except requests.exceptions.ConnectionError as err:
            raise ValueError("Connection error") from err
        except requests.exceptions.HTTPError as err:
            if err.response.status_code == 403:
                raise ValueError("Authentication error") from err
            else:
                raise

    def _get(self, url, params=None, **kwargs):
        return self._request("get", url, params=params, **kwargs)

    def _post(self, url, data=None, json=None, **kwargs):
        return self._request("post", url, data=data, json=json, **kwargs)
