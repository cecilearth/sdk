import re
from datetime import datetime

import boto3
import dask
import rasterio
import rasterio.session
import rioxarray
import xarray
import numpy as np

from .models import SubscriptionListFiles


def load_xarray(res: SubscriptionListFiles) -> xarray.Dataset:
    session = boto3.session.Session(
        aws_access_key_id=res.credentials.access_key_id,
        aws_secret_access_key=res.credentials.secret_access_key,
        aws_session_token=res.credentials.session_token,
        region_name=res.credentials.region,
    )

    keys = _list_keys(session, res.bucket.name, res.bucket.prefix)

    if not keys:
        return xarray.Dataset()

    timestamp_pattern = re.compile(r"\d{4}/\d{2}/\d{2}/\d{2}/\d{2}/\d{2}")
    data_vars = {}

    with rasterio.env.Env(
        session=rasterio.session.AWSSession(session),
    ):
        first_file = rioxarray.open_rasterio(
            f"s3://{res.bucket.name}/{keys[0]}", chunks="auto"
        )

    for key in keys:
        filename = key.split("/")[-1]

        file_info = res.file_mapping.get(filename)
        if not file_info:
            continue

        timestamp_str = timestamp_pattern.search(key).group()

        for band_info in file_info.bands:
            lazy_array = dask.array.from_delayed(
                dask.delayed(_load_file)(
                    session, f"s3://{res.bucket.name}/{key}", band_info.number
                ),
                shape=(
                    first_file.rio.height,
                    first_file.rio.width,
                ),
                dtype=band_info.dtype,
            )

            nodata = band_info.nodata if band_info.nodata is not None else np.nan

            band_da = xarray.DataArray(
                lazy_array,
                dims=("y", "x"),
                coords={
                    "y": first_file.y.values,
                    "x": first_file.x.values,
                },
                attrs={
                    "AREA_OR_POINT": first_file.attrs["AREA_OR_POINT"],
                    "_FillValue": np.dtype(band_info.dtype).type(nodata),
                    "scale_factor": first_file.attrs["scale_factor"],
                    "add_offset": first_file.attrs["add_offset"],
                },
            )
            # band_da.encoding = first_file.encoding.copy() # TODO: is it the same for all files?
            band_da.rio.write_crs(first_file.rio.crs, inplace=True)
            band_da.rio.write_transform(first_file.rio.transform(), inplace=True)

            band_da.name = band_info.name

            # Dataset with time dimension
            if timestamp_str != "0000/00/00/00/00/00":
                t = datetime.strptime(timestamp_str, "%Y/%m/%d/%H/%M/%S")
                band_da = band_da.expand_dims("time")
                band_da = band_da.assign_coords(time=[t])

            if band_info.name not in data_vars:
                data_vars[band_info.name] = []

            data_vars[band_info.name].append(band_da)

    for var_name, time_series in data_vars.items():
        if "time" in time_series[0].dims:
            data_vars[var_name] = xarray.concat(time_series, dim="time", join="exact")
        else:
            data_vars[var_name] = time_series[0]

    return xarray.Dataset(
        data_vars=data_vars,
        attrs={
            "provider_name": res.provider_name,
            "dataset_name": res.dataset_name,
            "dataset_id": res.dataset_id,
            "aoi_id": res.aoi_id,
            "subscription_id": res.subscription_id,
        },
    )


def _load_file(aws_session: boto3.session.Session, url: str, band_num: int):
    with rasterio.env.Env(
        session=rasterio.session.AWSSession(aws_session),
    ):
        with rasterio.open(url) as src:
            return src.read(band_num)


def _list_keys(session: boto3.session.Session, bucket_name, prefix) -> list[str]:
    s3_client = session.client("s3")
    paginator = s3_client.get_paginator("list_objects_v2")
    page_iterator = paginator.paginate(
        Bucket=bucket_name,
        Prefix=prefix,
    )

    keys = []
    for page in page_iterator:
        for obj in page.get("Contents", []):
            keys.append(obj["Key"])

    return keys
