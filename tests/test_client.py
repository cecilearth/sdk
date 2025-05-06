import responses

from src.cecil.client import Client
from src.cecil.models import DataRequest

FROZEN_TIME = "2024-01-01T00:00:00.000Z"


def test_client_class():
    client = Client()
    assert client._base_url == "https://api.cecil.earth"


@responses.activate
def test_client_create_data_request():
    responses.add(
        responses.POST,
        "https://api.cecil.earth/v0/data-requests",
        json={
            "id": "id",
            "aoiId": "aoi_id",
            "datasetId": "dataset_id",
            "created_at": FROZEN_TIME,
            "created_by": "user_id",
        },
        status=201,
    )

    client = Client()
    res = client.create_data_request("aoi_id", "dataset_id")

    assert res == DataRequest(
        id="id",
        aoiId="aoi_id",
        datasetId="dataset_id",
        created_at="2024-01-01T00:00:00.000Z",
        created_by="user_id",
    )


@responses.activate
def test_client_list_data_requests():
    responses.add(
        responses.GET,
        "https://api.cecil.earth/v0/data-requests",
        json={
            "records": [
                {
                    "id": "data_request_id_1",
                    "aoiId": "aoi_id",
                    "datasetId": "dataset_id",
                    "created_at": "2024-09-19T04:45:57.561Z",
                    "created_by": "user_id",
                },
                {
                    "id": "data_request_id_2",
                    "aoiId": "aoi_id",
                    "datasetId": "dataset_id",
                    "created_at": "2024-09-19T04:54:38.252Z",
                    "created_by": "user_id",
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
            created_at="2024-09-19T04:45:57.561Z",
            created_by="user_id",
        ),
        DataRequest(
            id="data_request_id_2",
            aoiId="aoi_id",
            datasetId="dataset_id",
            created_at="2024-09-19T04:54:38.252Z",
            created_by="user_id",
        ),
    ]
