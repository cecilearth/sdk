import responses

from src.cecil.client import Client
from src.cecil.models import Subscription

FROZEN_TIME = "2024-01-01T00:00:00.000Z"


def test_client_class():
    client = Client()
    assert client._base_url == "https://api.cecil.earth"


@responses.activate
def test_client_create_subscription():
    responses.add(
        responses.POST,
        "https://api.cecil.earth/v0/subscriptions",
        json={
            "id": "id",
            "aoiId": "aoi_id",
            "datasetId": "dataset_id",
            "externalRef": "external_ref",
            "created_at": FROZEN_TIME,
            "created_by": "user_id",
        },
        status=201,
    )

    client = Client()
    res = client.create_subscription("aoi_id", "dataset_id")

    assert res == Subscription(
        id="id",
        aoiId="aoi_id",
        datasetId="dataset_id",
        externalRef="external_ref",
        created_at="2024-01-01T00:00:00.000Z",
        created_by="user_id",
    )


@responses.activate
def test_client_list_subscriptions():
    responses.add(
        responses.GET,
        "https://api.cecil.earth/v0/subscriptions",
        json={
            "records": [
                {
                    "id": "subscription_id_1",
                    "aoiId": "aoi_id",
                    "datasetId": "dataset_id",
                    "externalRef": "external_ref",
                    "created_at": "2024-09-19T04:45:57.561Z",
                    "created_by": "user_id",
                },
                {
                    "id": "subscription_id_2",
                    "aoiId": "aoi_id",
                    "datasetId": "dataset_id",
                    "externalRef": "",
                    "created_at": "2024-09-19T04:54:38.252Z",
                    "created_by": "user_id",
                },
            ]
        },
    )

    client = Client()
    subscriptions = client.list_subscriptions()

    assert subscriptions == [
        Subscription(
            id="subscription_id_1",
            aoiId="aoi_id",
            datasetId="dataset_id",
            externalRef="external_ref",
            created_at="2024-09-19T04:45:57.561Z",
            created_by="user_id",
        ),
        Subscription(
            id="subscription_id_2",
            aoiId="aoi_id",
            datasetId="dataset_id",
            externalRef="",
            created_at="2024-09-19T04:54:38.252Z",
            created_by="user_id",
        ),
    ]
