import json
import warnings

import pytest
import responses

from src.cecil.client import Client
from src.cecil.models import Subscription, Webhook

FROZEN_TIME = "2024-01-01T00:00:00.000Z"
BASE = "https://api.cecil.earth"


def subscription_json(**overrides):
    body = {
        "id": "subscription_id",
        "aoi_id": "aoi_id",
        "dataset_id": "dataset_id",
        "dataset_publication": "1.0",
        "dataset_current_publication": "1.0",
        "external_ref": None,
        "created_at": FROZEN_TIME,
        "created_by": "user_id",
        "archived_at": None,
        "archived_by": None,
    }
    body.update(overrides)
    return body


def mock_ibat_load(status=None, status_message=None):
    responses.add(
        responses.GET,
        f"{BASE}/v0/dataset-storage",
        json={"storage": "ibat"},
        status=200,
    )
    responses.add(
        responses.GET,
        f"{BASE}/v0/subscriptions/subscription_id/files/parquet",
        json={"files": []},
        status=200,
    )
    responses.add(
        responses.GET,
        f"{BASE}/v0/subscriptions/subscription_id",
        json=subscription_json(status=status, status_message=status_message),
        status=200,
    )


def test_subscription_model_parses_status():
    s = Subscription(**subscription_json(status="processing", status_message="No data received yet after 36 hours."))
    assert s.status == "processing"
    assert s.status_message == "No data received yet after 36 hours."


def test_subscription_model_without_status_fields():
    # API responses that predate the field still parse.
    s = Subscription(**subscription_json())
    assert s.status is None
    assert s.status_message is None


@responses.activate
def test_load_dataframe_warns_when_not_completed():
    mock_ibat_load(status="processing", status_message="No data received from the provider since 2026-08-18T19:38:45Z.")

    with pytest.warns(UserWarning) as record:
        Client().load_dataframe("subscription_id")

    assert len(record) == 1
    message = str(record[0].message)
    assert "subscription_id is processing" in message
    assert "No data received from the provider since 2026-08-18T19:38:45Z." in message
    assert "Data may be incomplete" in message


@responses.activate
def test_load_dataframe_warns_on_partial_without_message():
    mock_ibat_load(status="partial")

    with pytest.warns(UserWarning, match=r"subscription_id is partial\. Data may be incomplete"):
        Client().load_dataframe("subscription_id")


@responses.activate
def test_load_dataframe_silent_when_completed():
    mock_ibat_load(status="completed")

    with warnings.catch_warnings():
        warnings.simplefilter("error")  # any warning fails the test
        Client().load_dataframe("subscription_id")


@responses.activate
def test_load_dataframe_silent_when_status_absent():
    mock_ibat_load()  # older API: no status field

    with warnings.catch_warnings():
        warnings.simplefilter("error")
        Client().load_dataframe("subscription_id")


@responses.activate
def test_create_webhook_with_events():
    responses.add(
        responses.POST,
        f"{BASE}/v0/webhooks",
        json={
            "id": "webhook_id",
            "url": "https://example.com/hook",
            "events": ["subscription.completed"],
            "created_at": FROZEN_TIME,
            "created_by": "user_id",
        },
        status=201,
    )

    webhook = Client().create_webhook("https://example.com/hook", events=["subscription.completed"])

    assert webhook.events == ["subscription.completed"]
    assert json.loads(responses.calls[0].request.body) == {
        "url": "https://example.com/hook",
        "secret": None,
        "events": ["subscription.completed"],
    }


@responses.activate
def test_create_webhook_without_events_omits_the_key():
    responses.add(
        responses.POST,
        f"{BASE}/v0/webhooks",
        json={
            "id": "webhook_id",
            "url": "https://example.com/hook",
            "created_at": FROZEN_TIME,
            "created_by": "user_id",
        },
        status=201,
    )

    webhook = Client().create_webhook("https://example.com/hook", secret="s")

    assert webhook == Webhook(
        id="webhook_id", url="https://example.com/hook", created_at=FROZEN_TIME, created_by="user_id"
    )
    assert "events" not in json.loads(responses.calls[0].request.body)
