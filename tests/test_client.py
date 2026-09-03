import pytest
import responses

from src.cecil.client import Client
from src.cecil.errors import DuplicateSubscriptionError
from src.cecil.models import Subscription
from src.cecil.models.usage import Usage

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
            "aoi_id": "aoi_id",
            "dataset_id": "dataset_id",
            "external_ref": "external_ref",
            "created_at": FROZEN_TIME,
            "created_by": "user_id",
        },
        status=201,
    )

    client = Client()
    res = client.create_subscription("aoi_id", "dataset_id")

    assert res == Subscription(
        id="id",
        aoi_id="aoi_id",
        dataset_id="dataset_id",
        external_ref="external_ref",
        created_at="2024-01-01T00:00:00.000Z",
        created_by="user_id",
    )


@responses.activate
def test_client_create_subscription_duplicate():
    message = (
        "Already subscribed to Forest Carbon Diligence for this AOI "
        "(subscription 79765fe6-ce95-40bc-9dd7-672e31d70253, created 2024-01-01). "
        "Use the existing subscription, or pass allow_duplicate to create another "
        "— e.g. to pick up a new dataset version."
    )
    responses.add(
        responses.POST,
        "https://api.cecil.earth/v0/subscriptions",
        json=[message],
        status=409,
    )

    client = Client()
    with pytest.raises(DuplicateSubscriptionError) as err:
        client.create_subscription("aoi_id", "dataset_id")

    assert err.value.status_code == 409
    assert err.value.response_body == [message]


@responses.activate
def test_client_create_subscription_allow_duplicate():
    responses.add(
        responses.POST,
        "https://api.cecil.earth/v0/subscriptions",
        json={
            "id": "id",
            "aoi_id": "aoi_id",
            "dataset_id": "dataset_id",
            "external_ref": None,
            "created_at": FROZEN_TIME,
            "created_by": "user_id",
        },
        status=201,
        match=[
            responses.matchers.json_params_matcher(
                {
                    "aoi_id": "aoi_id",
                    "dataset_id": "dataset_id",
                    "external_ref": None,
                    "allow_duplicate": True,
                }
            )
        ],
    )

    client = Client()
    res = client.create_subscription("aoi_id", "dataset_id", allow_duplicate=True)
    assert res.id == "id"


@responses.activate
def test_client_list_subscriptions():
    responses.add(
        responses.GET,
        "https://api.cecil.earth/v0/subscriptions",
        json={
            "records": [
                {
                    "id": "subscription_id_1",
                    "aoi_id": "aoi_id",
                    "dataset_id": "dataset_id",
                    "external_ref": "external_ref",
                    "created_at": "2024-09-19T04:45:57.561Z",
                    "created_by": "user_id",
                },
                {
                    "id": "subscription_id_2",
                    "aoi_id": "aoi_id",
                    "dataset_id": "dataset_id",
                    "external_ref": "",
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
            aoi_id="aoi_id",
            dataset_id="dataset_id",
            external_ref="external_ref",
            created_at="2024-09-19T04:45:57.561Z",
            created_by="user_id",
        ),
        Subscription(
            id="subscription_id_2",
            aoi_id="aoi_id",
            dataset_id="dataset_id",
            external_ref="",
            created_at="2024-09-19T04:54:38.252Z",
            created_by="user_id",
        ),
    ]


@responses.activate
def test_client_get_usage():
    responses.add(
        responses.GET,
        "https://api.cecil.earth/v0/usage",
        json={
            "num_subscriptions": 12,
            "monthly_subscriptions": 3,
            "total_area_ha": 123456.7,
            "monthly_area_ha": 23456.7,
            "num_aois": 8,
            "monthly_aois": 2,
            "monthly_subscription_limit": 50000,
        },
        status=200,
    )

    client = Client()
    usage = client.get_usage()

    assert usage == Usage(
        num_subscriptions=12,
        monthly_subscriptions=3,
        total_area_ha=123456.7,
        monthly_area_ha=23456.7,
        num_aois=8,
        monthly_aois=2,
        monthly_subscription_limit=50000,
    )
    assert usage.max_subscriptions is None
    assert usage.max_total_area_ha is None


@responses.activate
def test_client_get_subscription_publication():
    responses.add(
        responses.GET,
        "https://api.cecil.earth/v0/subscriptions/subscription_id",
        json={
            "id": "subscription_id",
            "aoi_id": "aoi_id",
            "dataset_id": "dataset_id",
            "dataset_publication": "1.1",
            "dataset_current_publication": "1.3.0",
            "external_ref": None,
            "created_at": FROZEN_TIME,
            "created_by": "user_id",
            "archived_at": None,
            "archived_by": None,
        },
        status=200,
    )

    client = Client()
    subscription = client.get_subscription("subscription_id")

    assert subscription.dataset_publication == "1.1"
    assert subscription.dataset_current_publication == "1.3.0"
    # A newer publication exists when the two differ
    assert subscription.dataset_publication != subscription.dataset_current_publication


@responses.activate
def test_client_publication_attrs_omit_none():
    responses.add(
        responses.GET,
        "https://api.cecil.earth/v0/subscriptions/subscription_id",
        json={
            "id": "subscription_id",
            "aoi_id": "aoi_id",
            "dataset_id": "dataset_id",
            "dataset_publication": "6.0.0",
            "dataset_current_publication": None,
            "created_at": FROZEN_TIME,
            "created_by": "user_id",
        },
        status=200,
    )

    client = Client()
    assert client._publication_attrs("subscription_id") == {"dataset_publication": "6.0.0"}
