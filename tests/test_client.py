import responses

from src.cecil.client import Client
from src.cecil.models import DataRequest, DataRequestStatus

FROZEN_TIME = "2024-01-01T00:00:00.000Z"


def test_client_class():
    client = Client()
    assert client._base_url == "https://dev-api.cecil.earth/v0"


@responses.activate
def test_client_create_data_request():
    responses.add(
        responses.POST,
        "https://dev-api.cecil.earth/v0/data-requests",
        json={
            "ID": "id",
            "AOIID": "aoi_id",
            "DatasetID": "dataset_id",
            "SubRequests": [],
            "Status": "processing",
            "Created": FROZEN_TIME,
        },
        status=201,
    )

    client = Client()
    res = client.create_data_request("aoi_id", "dataset_id")

    assert res == DataRequest(
        ID="id",
        AOIID="aoi_id",
        DatasetID="dataset_id",
        SubRequests=[],
        Status="processing",
        Created="2024-01-01T00:00:00.000Z",
    )


@responses.activate
def test_client_list_data_requests():
    responses.add(
        responses.GET,
        "https://dev-api.cecil.earth/v0/data-requests",
        json={
            "Records": [
                {
                    "ID": "data_request_id_1",
                    "AOIID": "aoi_id",
                    "DatasetID": "dataset_id",
                    "SubRequests": [],  # TODO: Add some SubRequests
                    "Status": "processing",
                    "Created": "2024-09-19T04:45:57.561Z",
                },
                {
                    "ID": "data_request_id_2",
                    "AOIID": "aoi_id",
                    "DatasetID": "dataset_id",
                    "SubRequests": [],  # TODO: Add some SubRequests
                    "Status": "completed",
                    "Created": "2024-09-19T04:54:38.252Z",
                },
            ]
        },
    )

    client = Client()
    data_requests = client.list_data_requests()

    assert data_requests == [
        DataRequest(
            ID="data_request_id_1",
            AOIID="aoi_id",
            DatasetID="dataset_id",
            SubRequests=[],
            Status=DataRequestStatus.PROCESSING,
            Created="2024-09-19T04:45:57.561Z",
        ),
        DataRequest(
            ID="data_request_id_2",
            AOIID="aoi_id",
            DatasetID="dataset_id",
            SubRequests=[],
            Status=DataRequestStatus.COMPLETED,
            Created="2024-09-19T04:54:38.252Z",
        ),
    ]
