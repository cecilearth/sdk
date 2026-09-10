import http.client
import json

import requests.exceptions


class Error(Exception):
    pass


class HTTPError(Error):
    def __init__(self, err: requests.exceptions.HTTPError):
        super().__init__(err)
        self.status_code = err.response.status_code
        self.status = http.client.responses[self.status_code]
        try:
            self.response_body = json.loads(err.response.text)
        except ValueError:
            self.response_body = {"message": err.response.text}

    def __str__(self):
        message = f"{self.status} ({self.status_code})"

        if self.status_code >= 500:
            return message

        return (
            message
            + "\n"
            + json.dumps(
                self.response_body,
                indent=2,
            )
        )


class DuplicateSubscriptionError(HTTPError):
    """An active subscription already exists for this AOI and dataset.

    The API refuses silent duplicates — they bill twice. Use the existing
    subscription (its id is in the message), or pass allow_duplicate=True
    to create another deliberately, e.g. to pick up a new dataset version.
    """

    def __init__(self, err: HTTPError):
        Error.__init__(self, *err.args)
        self.status_code = err.status_code
        self.status = err.status
        self.response_body = err.response_body


class SDKError(Error):
    pass


class SubscriptionFailedError(Error):
    """The subscription has failed, so there is no usable data to load.

    Unlike pending / processing / partial, failed is terminal: nothing more
    will arrive. Loading such a subscription is almost always a mistake, so
    this is raised rather than warned. status_message carries the provider's
    own explanation where one was given.
    """

    def __init__(self, subscription_id: str, status_message: str):
        super().__init__(
            f"Subscription {subscription_id} has failed: {status_message.rstrip('.')}. "
            "No data will be delivered; check get_subscription().status_message "
            "and create a new subscription once the cause is addressed."
        )
        self.subscription_id = subscription_id
        self.status_message = status_message
