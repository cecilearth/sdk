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


class SDKError(Error):
    pass
