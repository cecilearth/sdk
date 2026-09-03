import os
from typing import Dict, List, Optional

import geopandas
import requests
import requests.auth
import xarray

from .dataframe import load_dataframe, load_self_hosted_dataframe
from .errors import DuplicateSubscriptionError, HTTPError, SDKError
from .models.aoi import AOI
from .models.dataset import Dataset
from .models.settings import Settings
from .models.subscription import (
    Subscription,
    SubscriptionFormat,
    SubscriptionParquet,
    SubscriptionSelfHostedParquet,
    SubscriptionStorage,
    SubscriptionTIFF,
    SubscriptionZarr,
)
from .models.usage import Usage
from .models.user import User
from .models.webhook import Webhook
from .version import __version__
from .xarray import load_xarray_from_tiff, load_xarray_from_zarr


class Client:
    def __init__(self, env: str = None) -> None:
        self._api_auth = None
        self._base_url = (
            "https://api.cecil.earth" if env is None else f"https://{env}.cecil.earth"
        )

    def archive_aoi(self, id: str) -> None:
        try:
            self._post(url=f"/v0/aois/{id}/archive")

        except Exception as e:
            raise e.with_traceback(None) from None

    def archive_subscription(self, id: str) -> None:
        try:
            self._post(url=f"/v0/subscriptions/{id}/archive")

        except Exception as e:
            raise e.with_traceback(None) from None

    def create_aoi(self, geometry: Dict, external_ref: str = None) -> AOI:
        try:
            res = self._post(
                url="/v0/aois",
                json=dict(geometry=geometry, external_ref=external_ref),
            )
            return AOI(**res)

        except Exception as e:
            raise e.with_traceback(None) from None

    def create_subscription(
        self,
        aoi_id: str,
        dataset_id: str,
        external_ref: str = None,
        allow_duplicate: bool = False,
    ) -> Subscription:
        try:
            res = self._post(
                url="/v0/subscriptions",
                json=dict(
                    aoi_id=aoi_id,
                    dataset_id=dataset_id,
                    external_ref=external_ref,
                    allow_duplicate=allow_duplicate,
                ),
            )
            return Subscription(**res)

        except HTTPError as e:
            if e.status_code == 409:
                raise DuplicateSubscriptionError(e).with_traceback(None) from None
            raise e.with_traceback(None) from None
        except Exception as e:
            raise e.with_traceback(None) from None

    def create_user(self, first_name: str, last_name: str, email: str) -> User:
        try:
            res = self._post(
                url="/v0/users",
                json=dict(
                    first_name=first_name,
                    last_name=last_name,
                    email=email,
                ),
            )
            return User(**res)

        except Exception as e:
            raise e.with_traceback(None) from None

    def create_webhook(self, url: str, secret: str = None) -> Webhook:
        try:
            res = self._post(
                url="/v0/webhooks",
                json=dict(url=url, secret=secret),
            )
            return Webhook(**res)

        except Exception as e:
            raise e.with_traceback(None) from None

    def delete_webhook(self, id: str) -> None:
        try:
            self._delete(url=f"/v0/webhooks/{id}")
        except Exception as e:
            raise e.with_traceback(None) from None

    def get_aoi(self, id: str) -> AOI:
        try:
            res = self._get(url=f"/v0/aois/{id}")
            return AOI(**res)

        except Exception as e:
            raise e.with_traceback(None) from None

    def get_dataset(self, id) -> Dataset:
        try:
            res = self._get(url=f"/v0/datasets/{id}")
            return Dataset(**res)
        except Exception as e:
            raise e.with_traceback(None) from None

    def get_settings(self) -> Settings:
        try:
            res = self._get(url="/v0/settings")
            return Settings(**res)

        except Exception as e:
            raise e.with_traceback(None) from None

    def get_subscription(self, id: str) -> Subscription:
        try:
            res = self._get(url=f"/v0/subscriptions/{id}")
            return Subscription(**res)

        except Exception as e:
            raise e.with_traceback(None) from None

    def get_usage(self) -> Usage:
        try:
            res = self._get(url="/v0/usage")
            return Usage(**res)

        except Exception as e:
            raise e.with_traceback(None) from None

    def get_user(self, id: str) -> User:
        try:
            res = self._get(url=f"/v0/users/{id}")
            return User(**res)

        except Exception as e:
            raise e.with_traceback(None) from None

    def get_webhook(self, id: str) -> Webhook:
        try:
            res = self._get(url=f"/v0/webhooks/{id}")
            return Webhook(**res)

        except Exception as e:
            raise e.with_traceback(None) from None

    def list_aois(self, archived: bool = False) -> List[AOI]:
        try:
            res = self._get(url="/v0/aois", params={"archived": archived})
            return [AOI(**record) for record in res["records"]]

        except Exception as e:
            raise e.with_traceback(None) from None

    def list_datasets(self) -> List[Dataset]:
        try:
            res = self._get(url="/v0/datasets")
            return [Dataset(**record) for record in res["records"]]

        except Exception as e:
            raise e.with_traceback(None) from None

    def list_subscriptions(self, archived: bool = False) -> List[Subscription]:
        try:
            res = self._get(url="/v0/subscriptions", params={"archived": archived})
            return [Subscription(**record) for record in res["records"]]

        except Exception as e:
            raise e.with_traceback(None) from None

    def list_users(self) -> List[User]:
        try:
            res = self._get(url="/v0/users")
            return [User(**record) for record in res["records"]]

        except Exception as e:
            raise e.with_traceback(None) from None

    def list_webhooks(self) -> List[Webhook]:
        try:
            res = self._get(url=f"/v0/webhooks")
            return [Webhook(**record) for record in res["records"]]

        except Exception as e:
            raise e.with_traceback(None) from None

    def load_dataframe(
        self, subscription_id: str, columns: Optional[List[str]] = None
    ) -> geopandas.GeoDataFrame:
        try:
            res = SubscriptionStorage(
                **self._get(
                    url=f"/v0/dataset-storage",
                    params={"subscription_id": subscription_id},
                )
            )

            if res.storage == "ibat":
                if columns is not None:
                    raise SDKError(
                        "The columns parameter is not supported for this dataset"
                    )

                res_parquet = SubscriptionParquet(
                    **self._get(
                        url=f"/v0/subscriptions/{subscription_id}/files/parquet"
                    )
                )
                gdf = load_dataframe(res_parquet)
                gdf.attrs.update(self._publication_attrs(subscription_id))
                return gdf

            if res.storage == "self-hosted":
                res_parquet = SubscriptionSelfHostedParquet(
                    **self._get(
                        url=f"/v0/subscriptions/{subscription_id}/files/parquet/self-hosted"
                    )
                )
                gdf = load_self_hosted_dataframe(res_parquet, columns=columns)
                gdf.attrs.update(self._publication_attrs(subscription_id))
                return gdf

            raise SDKError("Unexpected dataset storage")

        except Exception as e:
            raise e
            # raise e.with_traceback(None) from None

    def load_xarray(
        self, subscription_id: str, mask_and_scale: bool = True
    ) -> xarray.Dataset:
        try:
            res = SubscriptionFormat(
                **self._get(
                    url=f"/v0/dataset-format",
                    params={"subscription_id": subscription_id},
                )
            )

            if res.format == "tiff":
                tiff_files = SubscriptionTIFF(
                    **self._get(url=f"/v0/subscriptions/{subscription_id}/files/tiff")
                )
                ds = load_xarray_from_tiff(tiff_files, mask_and_scale=mask_and_scale)
                ds.attrs.update(self._publication_attrs(subscription_id))
                return ds

            if res.format == "zarr":
                zarr_files = SubscriptionZarr(
                    **self._get(url=f"/v0/subscriptions/{subscription_id}/files/zarr")
                )
                ds = load_xarray_from_zarr(zarr_files)
                ds.attrs.update(self._publication_attrs(subscription_id))
                return ds

            raise SDKError("Unexpected dataset format")

        except Exception as e:
            raise e.with_traceback(None) from None

    def recover_api_key(self, email: str) -> str:
        try:
            return self._post(
                url="/v0/api-key/recover",
                json=dict(email=email),
                skip_auth=True,
            )

        except Exception as e:
            raise e.with_traceback(None) from None

    def restore_aoi(self, id: str) -> None:
        try:
            self._post(url=f"/v0/aois/{id}/restore")

        except Exception as e:
            raise e.with_traceback(None) from None

    def restore_subscription(self, id: str) -> None:
        try:
            self._post(url=f"/v0/subscriptions/{id}/restore")

        except Exception as e:
            raise e.with_traceback(None) from None

    def rotate_api_key(self) -> str:
        try:
            return self._post(url=f"/v0/api-key/rotate")

        except Exception as e:
            raise e.with_traceback(None) from None

    def update_settings(
        self,
        *,
        monthly_subscription_limit,
    ) -> None:
        try:
            self._post(
                url="/v0/settings",
                json=dict(monthly_subscription_limit=monthly_subscription_limit),
            )

        except Exception as e:
            raise e.with_traceback(None) from None

    def _publication_attrs(self, subscription_id: str) -> Dict[str, str]:
        # The pin lives on the subscription record, not on the files endpoints,
        # so one extra lightweight request lets every loaded object say which
        # publication it holds and whether a newer one exists. None values are
        # omitted: attrs must stay serialisable (e.g. to_netcdf) and pandas
        # .attrs is best-effort anyway — the subscription record is the source
        # of truth.
        subscription = self.get_subscription(subscription_id)
        attrs = {
            "dataset_publication": subscription.dataset_publication,
            "dataset_current_publication": subscription.dataset_current_publication,
        }
        return {k: v for k, v in attrs.items() if v is not None}

    def _delete(self, url: str, skip_auth=False, **kwargs) -> Dict | str:
        return self._request(
            method="delete",
            url=url,
            skip_auth=skip_auth,
            **kwargs,
        )

    def _get(self, url: str, **kwargs) -> Dict | str:
        return self._request(method="get", url=url, **kwargs)

    def _post(self, url: str, skip_auth=False, **kwargs) -> Dict | str:
        return self._request(
            method="post",
            url=url,
            skip_auth=skip_auth,
            **kwargs,
        )

    def _request(self, method: str, url: str, skip_auth=False, **kwargs) -> Dict | str:

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

    def _set_auth(self) -> None:
        try:
            api_key = os.environ["CECIL_API_KEY"]
            self._api_auth = requests.auth.HTTPBasicAuth(username=api_key, password="")
        except KeyError:
            raise SDKError("Environment variable CECIL_API_KEY not set")
