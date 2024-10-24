import responses

from src.cecil.client import Client
from src.cecil.models import DataRequest, DataRequestStatus

FROZEN_TIME = "2024-01-01T00:00:00.000Z"


def test_client_class():
    client = Client()
    assert client._base_url == "https://dev-api.cecil.earth"


@responses.activate
def test_client_create_data_request():
    responses.add(
        responses.POST,
        "https://dev-api.cecil.earth/v0/data-requests",
        json={
            "id": "id",
            "aoiId": "aoi_id",
            "datasetId": "dataset_id",
            "subRequests": [],
            "status": "processing",
            "created": FROZEN_TIME,
        },
        status=201,
    )

    client = Client()
    res = client.create_data_request("aoi_id", "dataset_id")

    assert res == DataRequest(
        id="id",
        aoiId="aoi_id",
        datasetId="dataset_id",
        subRequests=[],
        status="processing",
        created="2024-01-01T00:00:00.000Z",
    )


@responses.activate
def test_client_list_data_requests():
    responses.add(
        responses.GET,
        "https://dev-api.cecil.earth/v0/data-requests",
        json={
            "records": [
                {
                    "id": "data_request_id_1",
                    "aoiId": "aoi_id",
                    "datasetId": "dataset_id",
                    "subRequests": [],  # TODO: Add some SubRequests
                    "status": "processing",
                    "created": "2024-09-19T04:45:57.561Z",
                },
                {
                    "id": "data_request_id_2",
                    "aoiId": "aoi_id",
                    "datasetId": "dataset_id",
                    "subRequests": [],  # TODO: Add some SubRequests
                    "status": "completed",
                    "created": "2024-09-19T04:54:38.252Z",
                },
            ]
        },
    )

    client = Client()
    data_requests = client.list_data_requests()

    assert data_requests == [
        DataRequest(
            id="data_request_id_1",
            aoiId="aoi_id",
            datasetId="dataset_id",
            subRequests=[],
            status=DataRequestStatus.PROCESSING,
            created="2024-09-19T04:45:57.561Z",
        ),
        DataRequest(
            id="data_request_id_2",
            aoiId="aoi_id",
            datasetId="dataset_id",
            subRequests=[],
            status=DataRequestStatus.COMPLETED,
            created="2024-09-19T04:54:38.252Z",
        ),
    ]
