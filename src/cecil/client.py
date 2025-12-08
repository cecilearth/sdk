import os
from typing import Dict, List, Optional

import pandas as pd
import requests

from pydantic import BaseModel
from requests import auth

import xarray
from .errors import (
    Error,
    _handle_bad_request,
    _handle_method_not_allowed,
    _handle_not_found,
    _handle_too_many_requests,
    _handle_unprocessable_entity,
)
from .models import (
    AOI,
    AOIRecord,
    AOICreate,
    OrganisationSettings,
    RecoverAPIKey,
    RecoverAPIKeyRequest,
    RotateAPIKey,
    RotateAPIKeyRequest,
    User,
    UserCreate,
    SubscriptionParquetFiles,
    SubscriptionListFiles,
    Subscription,
    SubscriptionCreate,
)
from .version import __version__
from .xarray import load_xarray


class Client:
    def __init__(self, env: str = None) -> None:
        self._api_auth = None
        self._base_url = (
            "https://api.cecil.earth" if env is None else f"https://{env}.cecil.earth"
        )

    def create_aoi(self, geometry: Dict, external_ref: Optional[str] = None) -> AOI:
        # TODO: validate geometry
        res = self._post(
            url="/v0/aois",
            model=AOICreate(geometry=geometry, external_ref=external_ref),
        )
        return AOI(**res)

    def get_aoi(self, id: str) -> AOI:
        res = self._get(url=f"/v0/aois/{id}")
        return AOI(**res)

    def list_aois(self) -> List[AOIRecord]:
        res = self._get(url="/v0/aois")
        return [AOIRecord(**record) for record in res["records"]]

    def list_subscriptions(self) -> List[Subscription]:
        res = self._get(url="/v0/subscriptions")
        return [Subscription(**record) for record in res["records"]]

    def create_subscription(
        self, aoi_id: str, dataset_id: str, external_ref: Optional[str] = None
    ) -> Subscription:
        res = self._post(
            url="/v0/subscriptions",
            model=SubscriptionCreate(
                aoi_id=aoi_id, dataset_id=dataset_id, external_ref=external_ref
            ),
        )

        return Subscription(**res)

    def get_subscription(self, id: str) -> Subscription:
        res = self._get(url=f"/v0/subscriptions/{id}")
        return Subscription(**res)

    def load_xarray(self, subscription_id: str) -> xarray.Dataset:
        res = SubscriptionListFiles(
            **self._get(url=f"/v0/subscriptions/{subscription_id}/files/tiff")
        )
        return load_xarray(res)

    def load_dataframe(self, subscription_id: str) -> pd.DataFrame:
        res = SubscriptionParquetFiles(
            **self._get(url=f"/v0/subscriptions/{subscription_id}/parquet-files")
        )

        if not res.files:
            return pd.DataFrame()

        return pd.concat((pd.read_parquet(f) for f in res.files))

    def recover_api_key(self, email: str) -> RecoverAPIKey:
        res = self._post(
            url="/v0/api-key/recover",
            model=RecoverAPIKeyRequest(email=email),
            skip_auth=True,
        )

        return RecoverAPIKey(**res)

    def rotate_api_key(self) -> RotateAPIKey:
        res = self._post(url=f"/v0/api-key/rotate", model=RotateAPIKeyRequest())

        return RotateAPIKey(**res)

    def create_user(self, first_name: str, last_name: str, email: str) -> User:
        res = self._post(
            url="/v0/users",
            model=UserCreate(
                first_name=first_name,
                last_name=last_name,
                email=email,
            ),
        )
        return User(**res)

    def get_user(self, id: str) -> User:
        res = self._get(url=f"/v0/users/{id}")
        return User(**res)

    def list_users(self) -> List[User]:
        res = self._get(url="/v0/users")
        return [User(**record) for record in res["records"]]

    def get_organisation_settings(self) -> OrganisationSettings:
        res = self._get(url="/v0/organisation/settings")
        return OrganisationSettings(**res)

    def update_organisation_settings(
        self,
        *,
        monthly_subscription_limit,
    ) -> OrganisationSettings:
        res = self._post(
            url="/v0/organisation/settings",
            model=OrganisationSettings(
                monthly_subscription_limit=monthly_subscription_limit,
            ),
        )
        return OrganisationSettings(**res)

    def _request(self, method: str, url: str, skip_auth=False, **kwargs) -> Dict:

        if not skip_auth:
            self._set_auth()

        headers = {"cecil-python-sdk-version": __version__}

        try:
            r = requests.request(
                method=method,
                url=self._base_url + url,
                auth=self._api_auth,
                headers=headers,
                timeout=None,
                **kwargs,
            )
            r.raise_for_status()
            return r.json()

        except requests.exceptions.ConnectionError:
            raise Error("failed to connect to the Cecil Platform")

        except requests.exceptions.HTTPError as err:
            message = f"Request failed with status code {err.response.status_code}"
            if err.response.text != "":
                message += f": {err.response.text}"

            match err.response.status_code:
                case 400:
                    _handle_bad_request(err.response)
                case 401:
                    raise Error("unauthorised")
                case 404:
                    _handle_not_found(err.response)
                case 405:
                    _handle_method_not_allowed(err.response)
                case 422:
                    _handle_unprocessable_entity(err.response)
                case 429:
                    _handle_too_many_requests(err.response)
                case 500:
                    raise Error("internal server error")
                case _:
                    raise Error(
                        f"request failed with code {err.response.status_code}",
                        err.response.text,
                    )

    def _get(self, url: str, **kwargs) -> Dict:
        return self._request(method="get", url=url, **kwargs)

    def _post(self, url: str, model: BaseModel, skip_auth=False, **kwargs) -> Dict:
        return self._request(
            method="post",
            url=url,
            json=model.model_dump(by_alias=True),
            skip_auth=skip_auth,
            **kwargs,
        )

    def _set_auth(self) -> None:
        try:
            api_key = os.environ["CECIL_API_KEY"]
            self._api_auth = auth.HTTPBasicAuth(username=api_key, password="")
        except KeyError:
            raise ValueError("environment variable CECIL_API_KEY not set") from None
