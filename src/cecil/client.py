import os
import requests
from typing import Dict, List

import pandas as pd
import xarray
from pydantic import BaseModel
from requests import auth

from .errors import HTTPError, SDKError
from .models import (
    AOI,
    AOICreate,
    Dataset,
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
    Webhook,
    WebhookConfigure,
)
from .version import __version__
from .xarray import load_xarray


class Client:
    def __init__(self, env: str = None) -> None:
        self._api_auth = None
        self._base_url = (
            "https://api.cecil.earth" if env is None else f"https://{env}.cecil.earth"
        )

    def create_aoi(self, geometry: Dict, external_ref: str = "") -> AOI:
        try:
            res = self._post(
                url="/v0/aois",
                model=AOICreate(geometry=geometry, external_ref=external_ref),
            )
            return AOI(**res)

        except Exception as e:
            raise e.with_traceback(None) from None

    def get_aoi(self, id: str) -> AOI:
        try:
            res = self._get(url=f"/v0/aois/{id}")
            return AOI(**res)

        except Exception as e:
            raise e.with_traceback(None) from None

    def list_aois(self) -> List[AOI]:
        try:
            res = self._get(url="/v0/aois")
            return [AOI(**record) for record in res["records"]]

        except Exception as e:
            raise e.with_traceback(None) from None

    def list_subscriptions(self) -> List[Subscription]:
        try:
            res = self._get(url="/v0/subscriptions")
            return [Subscription(**record) for record in res["records"]]

        except Exception as e:
            raise e.with_traceback(None) from None

    def create_subscription(
        self, aoi_id: str, dataset_id: str, external_ref: str = ""
    ) -> Subscription:
        try:
            res = self._post(
                url="/v0/subscriptions",
                model=SubscriptionCreate(
                    aoi_id=aoi_id, dataset_id=dataset_id, external_ref=external_ref
                ),
            )
            return Subscription(**res)

        except Exception as e:
            raise e.with_traceback(None) from None

    def get_subscription(self, id: str) -> Subscription:
        try:
            res = self._get(url=f"/v0/subscriptions/{id}")
            return Subscription(**res)

        except Exception as e:
            raise e.with_traceback(None) from None

    def load_xarray(self, subscription_id: str) -> xarray.Dataset:
        try:
            res = SubscriptionListFiles(
                **self._get(url=f"/v0/subscriptions/{subscription_id}/files/tiff")
            )
            return load_xarray(res)

        except Exception as e:
            raise e.with_traceback(None) from None

    def load_dataframe(self, subscription_id: str) -> pd.DataFrame:
        try:
            res = SubscriptionParquetFiles(
                **self._get(url=f"/v0/subscriptions/{subscription_id}/parquet-files")
            )

            if not res.files:
                return pd.DataFrame()

            return pd.concat((pd.read_parquet(f) for f in res.files)).reset_index(
                drop=True
            )

        except Exception as e:
            raise e.with_traceback(None) from None

    def recover_api_key(self, email: str) -> RecoverAPIKey:
        try:
            res = self._post(
                url="/v0/api-key/recover",
                model=RecoverAPIKeyRequest(email=email),
                skip_auth=True,
            )
            return RecoverAPIKey(**res)

        except Exception as e:
            raise e.with_traceback(None) from None

    def rotate_api_key(self) -> RotateAPIKey:
        try:
            res = self._post(url=f"/v0/api-key/rotate", model=RotateAPIKeyRequest())
            return RotateAPIKey(**res)

        except Exception as e:
            raise e.with_traceback(None) from None

    def create_user(self, first_name: str, last_name: str, email: str) -> User:
        try:
            res = self._post(
                url="/v0/users",
                model=UserCreate(
                    first_name=first_name,
                    last_name=last_name,
                    email=email,
                ),
            )
            return User(**res)

        except Exception as e:
            raise e.with_traceback(None) from None

    def get_user(self, id: str) -> User:
        try:
            res = self._get(url=f"/v0/users/{id}")
            return User(**res)

        except Exception as e:
            raise e.with_traceback(None) from None

    def list_users(self) -> List[User]:
        try:
            res = self._get(url="/v0/users")
            return [User(**record) for record in res["records"]]

        except Exception as e:
            raise e.with_traceback(None) from None

    def get_organisation_settings(self) -> OrganisationSettings:
        try:
            res = self._get(url="/v0/organisation/settings")
            return OrganisationSettings(**res)

        except Exception as e:
            raise e.with_traceback(None) from None

    def update_organisation_settings(
        self,
        *,
        monthly_subscription_limit,
    ) -> OrganisationSettings:
        try:
            res = self._post(
                url="/v0/organisation/settings",
                model=OrganisationSettings(
                    monthly_subscription_limit=monthly_subscription_limit,
                ),
            )
            return OrganisationSettings(**res)

        except Exception as e:
            raise e.with_traceback(None) from None

    def configure_webhook(self, url: str, secret: str = None) -> Webhook:
        try:
            res = self._post(
                url="/v0/webhooks",
                model=WebhookConfigure(url=url, secret=secret),
            )
            return Webhook(**res)

        except Exception as e:
            raise e.with_traceback(None) from None

    def delete_webhook(self):
        try:
            self._delete(url="/v0/webhooks")
            return

        except Exception as e:
            raise e.with_traceback(None) from None

    def list_datasets(self) -> List[Dataset]:
        try:
            res = self._get(url="/v0/datasets")
            return [Dataset(**record) for record in res["records"]]

        except Exception as e:
            raise e.with_traceback(None) from None

    def _request(self, method: str, url: str, skip_auth=False, **kwargs) -> Dict|str:

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

        except requests.exceptions.HTTPError as err:
            raise HTTPError(err)

        try:
            return r.json()
        except ValueError:
            return r.text

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

    def _delete(self, url: str, skip_auth=False, **kwargs) -> Dict:
        return self._request(
            method="delete",
            url=url,
            skip_auth=skip_auth,
            **kwargs,
        )

    def _set_auth(self) -> None:
        try:
            api_key = os.environ["CECIL_API_KEY"]
            self._api_auth = auth.HTTPBasicAuth(username=api_key, password="")
        except KeyError:
            raise SDKError("Environment variable CECIL_API_KEY not set")
