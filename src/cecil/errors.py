import json


class Error(Exception):
    def __init__(self, message: str, details=None):
        self.message = message
        self.details = details

        if self.details is not None:
            super().__init__(f"{self.message} \n{json.dumps(self.details, indent=2)}")
            return

        super().__init__(self.message)


def _format_json_key(value: str):
    return "".join(["_" + i.lower() if i.isupper() else i for i in value]).lstrip("_")


def _is_json(value: str):
    try:
        json.loads(value)
        return True
    except ValueError:
        return False


def _handle_bad_request(response):
    if not _is_json(response.text):
        raise Error("bad request")

    details = {}
    for key, value in response.json().items():
        details[_format_json_key(key)] = value

    raise Error("bad request", details)


def _handle_not_found(response):
    if not _is_json(response.text):
        raise Error("resource not found")

    details = {}
    for key, value in response.json().items():
        details[_format_json_key(key)] = value

    raise Error("resource not found", details)


def _handle_too_many_requests(response):
    if not _is_json(response.text):
        raise Error("too many requests")

    details = {}

    for key, value in response.json().items():
        details[_format_json_key(key)] = value

    raise Error("too many requests", details)


def _handle_unprocessable_entity(response):
    if not _is_json(response.text):
        raise Error(f"failed to process request")

    res_body = response.json()
    if "params" not in res_body:
        raise Error(f"failed to process request")

    details = {}
    for key, value in res_body["params"].items():
        details[_format_json_key(key)] = value

    raise Error(f"failed to process request", details)
