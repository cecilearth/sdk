import os
from datetime import datetime
from typing import Dict, List

import pandas as pd
import rasterio
import requests
import snowflake.connector
from pydantic import BaseModel
from requests import auth
from cryptography.hazmat.primitives import serialization
import xarray as xr
import rioxarray as rio
from rasterio.io import MemoryFile

from .errors import (
    Error,
    _handle_bad_request,
    _handle_not_found,
    _handle_too_many_requests,
    _handle_unprocessable_entity,
)
from .models import (
    AOI,
    AOIRecord,
    AOICreate,
    DataRequest,
    DataRequestCreate,
    OrganisationSettings,
    RecoverAPIKey,
    RecoverAPIKeyRequest,
    RotateAPIKey,
    RotateAPIKeyRequest,
    SnowflakeUserCredentials,
    Transformation,
    TransformationCreate,
    User,
    UserCreate,
    DataRequestInfo,
)
from .version import __version__


class Client:
    def __init__(self, env: str = None) -> None:
        self._api_auth = None
        self._base_url = (
            "https://api.cecil.earth" if env is None else f"https://{env}.cecil.earth"
        )
        self._snowflake_user_creds = None

    def create_aoi(self, name: str, geometry: Dict) -> AOI:
        # TODO: validate geometry
        res = self._post(url="/v0/aois", model=AOICreate(name=name, geometry=geometry))
        return AOI(**res)

    def get_aoi(self, id: str) -> AOI:
        res = self._get(url=f"/v0/aois/{id}")
        return AOI(**res)

    def list_aois(self) -> List[AOIRecord]:
        res = self._get(url="/v0/aois")
        return [AOIRecord(**record) for record in res["records"]]

    def create_data_request(self, aoi_id: str, dataset_id: str) -> DataRequest:
        res = self._post(
            url="/v0/data-requests",
            model=DataRequestCreate(aoi_id=aoi_id, dataset_id=dataset_id),
        )
        return DataRequest(**res)

    def get_data_request(self, id: str) -> DataRequest:
        res = self._get(url=f"/v0/data-requests/{id}")
        return DataRequest(**res)

    def list_data_requests(self) -> List[DataRequest]:
        res = self._get(url="/v0/data-requests")
        return [DataRequest(**record) for record in res["records"]]

    def get_xarray(self, id: str) -> str:
        res = self._get(url=f"/v0/data-requests/{id}/metadata")
        object = DataRequestInfo(**res)

        all_variables = set()
        for file_info in object.files:
            for band_info in file_info.bands.values():
                all_variables.add(band_info.variable_name)

        var_to_files = {}

        for file_info in object.files:
            for band_num, band_info in file_info.bands.items():
                var_name = band_info.variable_name

                if var_name not in var_to_files:
                    var_to_files[var_name] = []

                var_to_files[var_name].append(
                    {
                        "file_info": file_info,
                        "band_number": int(band_num),
                        "band_info": band_info,
                    }
                )

        datasets = {}

        for var_name, file_band_list in var_to_files.items():

            time_series = []

            def sort_key(x):
                time_str = x["band_info"].time
                if time_str in ["n/a", None]:
                    return datetime.min  # Put n/a times first
                try:
                    return datetime.strptime(time_str, "%Y")
                except:
                    try:
                        return datetime.strptime(time_str, "%Y-%m-%d")
                    except:
                        return datetime.min

            file_band_list.sort(key=sort_key)

            for item in file_band_list:
                file_info = item["file_info"]
                band_num = item["band_number"]
                band_info = item["band_info"]

                try:
                    da = rio.open_rasterio(file_info.url, chunks={"x": 2000, "y": 2000})
                    da_band = da.sel(band=band_num, drop=True)
                    time_coord = None

                    time_str = band_info.time
                    if time_str:
                        try:
                            for fmt in ["%Y-%m-%d", "%Y"]:
                                try:
                                    time_coord = datetime.strptime(time_str, fmt)
                                    break
                                except ValueError:
                                    continue
                        except Exception as e:
                            raise Error

                    if time_coord is not None:
                        da_band = da_band.expand_dims("time")
                        da_band = da_band.assign_coords(time=[time_coord])

                    da_band.name = var_name

                    # Add this band to the time_series list
                    time_series.append(da_band)

                except Exception as e:
                    raise Error

            if time_series:
                has_time_dims = [ts for ts in time_series if "time" in ts.dims]
                no_time_dims = [ts for ts in time_series if "time" not in ts.dims]

                processed_series = []

                # Handle arrays with time dimensions
                if has_time_dims:

                    # Concatenate along time dimension if multiple timesteps
                    if len(has_time_dims) > 1:
                        time_data = xr.concat(has_time_dims, dim="time")
                    else:
                        time_data = has_time_dims[0]

                    processed_series.append(time_data)

                # Handle static data
                if no_time_dims:
                    processed_series.extend(no_time_dims)

                # Use the first processed series as the variable data
                var_data = processed_series[0]

                datasets[var_name] = var_data
                print(f"  Successfully loaded {var_name}")

            # If no time series for this variable
            else:
                print(f"  Warning: No data successfully loaded for {var_name}")

        try:
            combined_ds = xr.Dataset(datasets)
        except Exception as e:
            print(f"Warning: Could not combine all variables into single dataset: {e}")
            print("Creating dataset with compatible variables only...")

            # Try to group variables by compatible dimensions
            compatible_vars = {}
            for var_name, var_data in datasets.items():
                # Create a key based on spatial dimensions
                spatial_dims = tuple(
                    sorted(
                        [
                            dim
                            for dim in var_data.dims
                            if dim in ["x", "y", "lat", "lon"]
                        ]
                    )
                )
                spatial_shape = tuple(
                    var_data.sizes[dim] for dim in spatial_dims if dim in var_data.sizes
                )
                key = (spatial_dims, spatial_shape)

                if key not in compatible_vars:
                    compatible_vars[key] = {}
                compatible_vars[key][var_name] = var_data

            # Use the group with the most variables, or the first one
            if compatible_vars:
                best_group = max(compatible_vars.values(), key=len)
                combined_ds = xr.Dataset(best_group)

                excluded_vars = set(datasets.keys()) - set(best_group.keys())
                if excluded_vars:
                    print(
                        f"Note: Excluded variables due to incompatible dimensions: {excluded_vars}"
                    )
            else:
                # Fallback: just use the first variable
                first_var = list(datasets.keys())[0]
                combined_ds = xr.Dataset({first_var: datasets[first_var]})
                print(f"Fallback: Using only {first_var}")

        combined_ds.attrs.update(
            {
                "dataset_name": object.dataset_name,
                "dataset_id": object.dataset_id,
                "aoi_id": object.aoi_id,
                "data_request_id": object.data_request_id,
                "crs": object.dataset_crs,
            }
        )

        return combined_ds

    def create_transformation(
        self, data_request_id: str, crs: str, spatial_resolution: float
    ) -> Transformation:
        res = self._post(
            url="/v0/transformations",
            model=TransformationCreate(
                data_request_id=data_request_id,
                crs=crs,
                spatial_resolution=spatial_resolution,
            ),
        )
        return Transformation(**res)

    def get_transformation(self, id: str) -> Transformation:
        res = self._get(url=f"/v0/transformations/{id}")
        return Transformation(**res)

    def list_transformations(self) -> List[Transformation]:
        res = self._get(url="/v0/transformations")
        return [Transformation(**record) for record in res["records"]]

    def query(self, sql: str) -> pd.DataFrame:
        if self._snowflake_user_creds is None:
            res = self._get(url="/v0/snowflake-user-credentials")
            self._snowflake_user_creds = SnowflakeUserCredentials(**res)

        private_key = serialization.load_pem_private_key(
            self._snowflake_user_creds.private_key.get_secret_value().encode(),
            password=None,
        )

        with snowflake.connector.connect(
            account=self._snowflake_user_creds.account.get_secret_value(),
            user=self._snowflake_user_creds.user.get_secret_value(),
            private_key=private_key,
        ) as conn:
            df = conn.cursor().execute(sql).fetch_pandas_all()
            df.columns = [x.lower() for x in df.columns]

            return df

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
        monthly_data_request_limit,
    ) -> OrganisationSettings:
        res = self._post(
            url="/v0/organisation/settings",
            model=OrganisationSettings(
                monthly_data_request_limit=monthly_data_request_limit,
            ),
        )
        return OrganisationSettings(**res)

    def _request(self, method: str, url: str, skip_auth=False, **kwargs) -> Dict:

        if skip_auth is False:
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
