import os
import re
import time

import boto3
import rioxarray
import xarray

from datetime import datetime

from .errors import Error
from .models import DataRequestMetadata, DataRequestLoadXarray, Bucket

os.environ["GDAL_DISABLE_READDIR_ON_OPEN"] = "TRUE"


def align_pixel_grids(time_series):
    # Use the first timestep as reference
    reference_da = time_series[0]
    aligned_series = [reference_da]

    # Align all other timesteps to the reference grid
    for i, da in enumerate(time_series[1:], 1):
        try:
            aligned_da = da.rio.reproject_match(reference_da)
            aligned_series.append(aligned_da)
        except Exception:
            raise Error

    return aligned_series


def retry_with_exponential_backoff(
    func, retries, start_delay, multiplier, *args, **kwargs
):
    delay = start_delay
    for attempt in range(1, retries + 1):
        try:
            return func(*args, **kwargs)
        except Exception as e:
            if attempt == retries:
                raise e
            time.sleep(delay)
            delay *= multiplier
    return None


def load_file(url: str):
    return rioxarray.open_rasterio(
        url,
        chunks={"x": 2000, "y": 2000},
    )


def load_xarray(metadata: DataRequestMetadata) -> xarray.Dataset:
    data_vars = {}

    for f in metadata.files:
        try:
            dataset = retry_with_exponential_backoff(load_file, 5, 1, 2, f.url)
        except Exception as e:
            raise ValueError(f"failed to load file: {e}")

        for b in f.bands:
            band = dataset.sel(band=b.number, drop=True)

            if b.time and b.time_pattern:
                t = datetime.strptime(b.time, b.time_pattern)
                band = band.expand_dims("time")
                band = band.assign_coords(time=[t])

            band.name = b.variable_name

            if b.variable_name not in data_vars:
                data_vars[b.variable_name] = []

            data_vars[b.variable_name].append(band)

    for variable_name, time_series in data_vars.items():
        if "time" in time_series[0].dims:
            # time_series = align_pixel_grids(time_series)
            data_vars[variable_name] = xarray.concat(
                time_series, dim="time", join="exact"
            )
        else:
            data_vars[variable_name] = time_series[0]

    return xarray.Dataset(
        data_vars=data_vars,
        attrs={
            "provider_name": metadata.provider_name,
            "dataset_id": metadata.dataset_id,
            "dataset_name": metadata.dataset_name,
            "dataset_crs": metadata.dataset_crs,
            "aoi_id": metadata.aoi_id,
            "data_request_id": metadata.data_request_id,
        },
    )


def load_xarray_v2(load_xarray_info: DataRequestLoadXarray) -> xarray.Dataset:
    data_vars = {}

    keys = _get_xarray_keys(load_xarray_info.bucket)
    for key in keys:
        timestamp_pattern = re.compile(r"\d{4}/\d{2}/\d{2}/\d{2}/\d{2}/\d{2}")
        timestamp_str = timestamp_pattern.search(key).group()

        variable_name = key.split("/")[14]
        filename = f"s3://{load_xarray_info.bucket.name}/{key}"
        dataset = rioxarray.open_rasterio(filename, chunks={"x": 2000, "y": 2000})
        band = dataset.sel(band=1, drop=True)
        band.name = variable_name

        # Dataset without time information
        if timestamp_str != "0000/00/00/00/00/00":
            time = datetime.strptime(timestamp_str, "%Y/%m/%d/%H/%M/%S")
            band = band.expand_dims("time")
            band = band.assign_coords(time=[time])

        if variable_name not in data_vars:
            data_vars[variable_name] = []

        data_vars[variable_name].append(band)

    for variable_name, time_series in data_vars.items():
        if "time" in time_series[0].dims:
            data_vars[variable_name] = xarray.concat(
                time_series, dim="time", join="exact"
            )
        else:
            data_vars[variable_name] = time_series[0]

    return xarray.Dataset(
        data_vars=data_vars,
        attrs={
            "provider_name": load_xarray_info.provider_name,
            "dataset_id": load_xarray_info.dataset_id,
            "dataset_name": load_xarray_info.dataset_name,
            "dataset_crs": load_xarray_info.dataset_crs,
            "aoi_id": load_xarray_info.aoi_id,
            "data_request_id": load_xarray_info.data_request_id,
        },
    )


def _get_xarray_keys(bucket: Bucket) -> list[str]:
    os.environ["AWS_ACCESS_KEY_ID"] = bucket.access_key_id
    os.environ["AWS_SECRET_ACCESS_KEY"] = bucket.secret_access_key
    os.environ["AWS_SESSION_TOKEN"] = bucket.session_token

    s3_client = boto3.client("s3")

    paginator = s3_client.get_paginator("list_objects_v2")
    page_iterator = paginator.paginate(
        Bucket=bucket.name,
        Prefix=bucket.prefix,
    )

    keys = []
    for page in page_iterator:
        for obj in page.get("Contents", []):
            keys.append(obj["Key"])

    return keys
