import requests
from .models import DataRequest

HTTP_TIMEOUT_SECONDS = 3

API_URL = f"https://dev-api.cecil.earth/v0"


class Client:
    def __init__(self):
        self._api_url = API_URL

    def create_data_request(self, organisation_id, aoi_id, dataset_id):
        try:
            r = requests.post(
                self._api_url + "/data-requests",
                timeout=HTTP_TIMEOUT_SECONDS,
                json={
                    "organisation_id": organisation_id,
                    "aoi_id": aoi_id,
                    "dataset_id": dataset_id,
                }
            )
            r.raise_for_status()
        except requests.exceptions.ConnectionError as err:
            raise ValueError("Connection error") from err
        except requests.exceptions.HTTPError as err:
            if err.response.status_code == 403:
                raise ValueError("Authentication error") from err
            else:
                raise

        return DataRequest(**r.json())

    def list_data_requests(self):
        try:
            r = requests.get(
                self._api_url + "/data-requests",
                timeout=HTTP_TIMEOUT_SECONDS,
            )
            r.raise_for_status()
        except requests.exceptions.ConnectionError as err:
            raise ValueError("Connection error") from err
        except requests.exceptions.HTTPError as err:
            if err.response.status_code == 403:
                raise ValueError("Authentication error") from err
            else:
                raise

        return [DataRequest(**data_request) for data_request in r.json()["DataRequests"]]
