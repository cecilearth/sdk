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
from .models import DataRequestMetadata, DataRequestListTIFF

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


def load_xarray_v2(api_metadata: DataRequestListTIFF) -> xarray.Dataset:
    keys = _list_files(api_metadata)
    raster_metadata = _get_raster_metadata(api_metadata.bucket.name, keys[0])

    data_vars = {}
    for key in keys:
        variable_name = key.split("/")[14]
        timestamp_pattern = re.compile(r"\d{4}/\d{2}/\d{2}/\d{2}/\d{2}/\d{2}")
        timestamp_str = timestamp_pattern.search(key).group()

        file_info = api_metadata.fileBandMapping[variable_name]
        num_bands = len(file_info.bands.keys())

        array = _create_lazy_dask_array(
            f"s3://{api_metadata.bucket.name}/{key}",
            num_bands,
            raster_metadata["height"],
            raster_metadata["width"],
            file_info.dtype,
        )
        da = xarray.DataArray(
            array,
            dims=("band", "y", "x"),
        )
        da.name = variable_name

        if num_bands == 1:
            da = da.squeeze("band")

        # Dataset without time information
        if timestamp_str != "0000/00/00/00/00/00":
            time = datetime.strptime(timestamp_str, "%Y/%m/%d/%H/%M/%S")
            da = da.expand_dims("time")
            da = da.assign_coords(time=[time])

        if variable_name not in data_vars:
            data_vars[variable_name] = []

        data_vars[variable_name].append(da)

    for variable_name, time_series in data_vars.items():
        if "time" in time_series[0].dims:
            data_vars[variable_name] = xarray.concat(
                time_series, dim="time", join="exact"
            )
        else:
            data_vars[variable_name] = time_series[0]

    ds = xarray.Dataset(
        data_vars=data_vars,
        coords={
            "y": raster_metadata["y"],
            "x": raster_metadata["x"],
        },
        attrs={
            "provider_name": api_metadata.provider_name,
            "dataset_id": api_metadata.dataset_id,
            "dataset_name": api_metadata.dataset_name,
            "dataset_crs": api_metadata.dataset_crs,
            "aoi_id": api_metadata.aoi_id,
            "data_request_id": api_metadata.data_request_id,
        },
    )
    ds = ds.rio.write_crs(api_metadata.dataset_crs)

    return ds


def _list_files(api_metadata: DataRequestListTIFF) -> list[str]:
    os.environ["AWS_ACCESS_KEY_ID"] = api_metadata.credentials.access_key_id
    os.environ["AWS_SECRET_ACCESS_KEY"] = api_metadata.credentials.secret_access_key
    os.environ["AWS_SESSION_TOKEN"] = api_metadata.credentials.session_token

    s3_client = boto3.client("s3")
    paginator = s3_client.get_paginator("list_objects_v2")
    page_iterator = paginator.paginate(
        Bucket=api_metadata.bucket.name,
        Prefix=api_metadata.bucket.prefix,
    )

    keys = []
    for page in page_iterator:
        for obj in page.get("Contents", []):
            keys.append(obj["Key"])

    return keys


def _get_raster_metadata(bucket: str, path: str):
    da = xarray.open_dataarray(f"s3://{bucket}/{path}", engine="rasterio")
    return {
        "height": da.rio.height,
        "width": da.rio.width,
        "x": da.x.values,
        "y": da.y.values,
    }


def _create_lazy_dask_array(
    file_path: str, num_bands: int, height: int, width: int, dtype: str
):
    def read_chunk():
        with rasterio.open(file_path) as src:
            return src.read()

    return dask.array.from_delayed(
        dask.delayed(read_chunk)(), shape=(num_bands, height, width), dtype=dtype
    )
