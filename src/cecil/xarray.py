import os
import re
import time
from datetime import datetime

import boto3
import dask
import rasterio
import rioxarray
import xarray

from .errors import Error
from .models import DataRequestMetadata, DataRequestListFiles

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


def _list_s3_keys(res: DataRequestListFiles) -> list[str]:
    os.environ["AWS_ACCESS_KEY_ID"] = res.credentials.access_key_id
    os.environ["AWS_SECRET_ACCESS_KEY"] = res.credentials.secret_access_key
    os.environ["AWS_SESSION_TOKEN"] = res.credentials.session_token

    s3_client = boto3.client("s3")
    paginator = s3_client.get_paginator("list_objects_v2")
    page_iterator = paginator.paginate(
        Bucket=res.bucket.name,
        Prefix=res.bucket.prefix,
    )

    keys = []
    for page in page_iterator:
        for obj in page.get("Contents", []):
            keys.append(obj["Key"])

    return keys


def _get_file_metadata(bucket: str, path: str):
    da = xarray.open_dataarray(f"s3://{bucket}/{path}", engine="rasterio")
    return {
        "crs": da.rio.crs,
        "height": da.rio.height,
        "width": da.rio.width,
        "x": da.x.values,
        "y": da.y.values,
    }


def _create_lazy_dask_array(
    file_path: str, band_num: int, height: int, width: int, dtype: str
):
    def read_chunk():
        with rasterio.open(file_path) as src:
            return src.read(band_num)

    return dask.array.from_delayed(
        dask.delayed(read_chunk)(), shape=(height, width), dtype=dtype
    )


def load_xarray_v2(res: DataRequestListFiles) -> xarray.Dataset:
    keys = _list_s3_keys(res)
    if not keys:
        return xarray.Dataset()

    first_file_metadata = _get_file_metadata(res.bucket.name, keys[0])
    timestamp_pattern = re.compile(r"\d{4}/\d{2}/\d{2}/\d{2}/\d{2}/\d{2}")

    data_vars = {}
    for key in keys:
        filename = key.split("/")[-1].rsplit(".", 1)[0]

        file_info = res.file_mapping.get(filename)
        if not file_info:
            continue

        timestamp_str = timestamp_pattern.search(key).group()

        for band_num, band_name in enumerate(file_info.bands, start=1):
            array = _create_lazy_dask_array(
                f"s3://{res.bucket.name}/{key}",
                band_num,
                first_file_metadata["height"],
                first_file_metadata["width"],
                file_info.type,
            )
            da = xarray.DataArray(
                array,
                dims=("y", "x"),
            )
            da.name = band_name

            # Dataset with time dimension
            if timestamp_str != "0000/00/00/00/00/00":
                time = datetime.strptime(timestamp_str, "%Y/%m/%d/%H/%M/%S")
                da = da.expand_dims("time")
                da = da.assign_coords(time=[time])

            if band_name not in data_vars:
                data_vars[band_name] = []

            data_vars[band_name].append(da)

    for variable_name, time_series in data_vars.items():
        if "time" in time_series[0].dims:
            data_vars[variable_name] = xarray.concat(
                time_series,
                dim="time",
                join="exact",
            )
        else:
            data_vars[variable_name] = time_series[0]

    ds = xarray.Dataset(
        data_vars=data_vars,
        coords={
            "y": first_file_metadata["y"],
            "x": first_file_metadata["x"],
        },
        attrs={
            "provider_name": res.provider_name,
            "id": res.dataset_id,
            "name": res.dataset_name,
            # TODO: confirm if we need: res, is_tiled, blockxsize, transform, bands
            "aoi_id": res.aoi_id,
            # TODO: add aoi_external_ref
            "data_request_id": res.data_request_id,
            # TODO: add data_request_external_ref
        },
    )
    ds = ds.rio.write_crs(first_file_metadata["crs"])

    return ds
