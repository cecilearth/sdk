import re
import time
from datetime import datetime

import boto3
import dask
import rasterio
import rasterio.session
import rioxarray
import xarray

from .errors import Error
from .models import SubscriptionMetadata, SubscriptionListFiles

# v1


def load_xarray(metadata: SubscriptionMetadata) -> xarray.Dataset:
    data_vars = {}

    for f in metadata.files:
        try:
            dataset = _retry_with_exponential_backoff(_load_file, 5, 1, 2, f.url)
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
            data_vars[variable_name] = xarray.concat(
                time_series, dim="time", join="exact"
            )
        else:
            data_vars[variable_name] = time_series[0]

    return xarray.Dataset(
        data_vars=data_vars,
        attrs={
            "provider_name": metadata.provider_name,
            "dataset_name": metadata.dataset_name,
            "dataset_id": metadata.dataset_id,
            "aoi_id": metadata.aoi_id,
            "subscription_id": metadata.data_request_id,
        },
    )


def _retry_with_exponential_backoff(
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


def _load_file(url: str):
    return rioxarray.open_rasterio(
        url,
        chunks={"x": 2000, "y": 2000},
    )


# v2


def load_xarray_v2(res: SubscriptionListFiles) -> xarray.Dataset:
    session = boto3.session.Session(
        aws_access_key_id=res.credentials.access_key_id,
        aws_secret_access_key=res.credentials.secret_access_key,
        aws_session_token=res.credentials.session_token,
    )

    keys = _list_keys_v2(session, res.bucket.name, res.bucket.prefix)

    if not keys:
        return xarray.Dataset()

    timestamp_pattern = re.compile(r"\d{4}/\d{2}/\d{2}/\d{2}/\d{2}/\d{2}")
    data_vars = {}

    for key in keys:
        try:
            file_da = _retry_with_exponential_backoff(
                _load_file_v2,
                5,
                1,
                2,
                session,
                f"s3://{res.bucket.name}/{key}",
            )
        except Exception as e:
            raise ValueError(f"failed to load file: {e}")

        filename = key.split("/")[-1]

        file_info = res.file_mapping.get(filename)
        if not file_info:
            continue

        timestamp_str = timestamp_pattern.search(key).group()

        for band_num, var_name in enumerate(file_info.bands, start=1):
            band_da = file_da.sel(band=band_num, drop=True)
            band_da.name = var_name

            # Dataset with time dimension
            if timestamp_str != "0000/00/00/00/00/00":
                t = datetime.strptime(timestamp_str, "%Y/%m/%d/%H/%M/%S")
                band_da = band_da.expand_dims("time")
                band_da = band_da.assign_coords(time=[t])

            if var_name not in data_vars:
                data_vars[var_name] = []

            data_vars[var_name].append(band_da)

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
            "subscription_id": res.data_request_id,
        },
    )


def _list_keys_v2(session: boto3.session.Session, bucket_name, prefix) -> list[str]:
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


def _load_file_v2(aws_session: boto3.session.Session, url: str):
    with rasterio.env.Env(
        session=rasterio.session.AWSSession(aws_session),
        GDAL_DISABLE_READDIR_ON_OPEN=True,
    ):
        return rioxarray.open_rasterio(
            url,
            chunks={"x": 2000, "y": 2000},
        )


# v3


def load_xarray_v3(res: SubscriptionListFiles) -> xarray.Dataset:
    session = boto3.session.Session(
        aws_access_key_id=res.credentials.access_key_id,
        aws_secret_access_key=res.credentials.secret_access_key,
        aws_session_token=res.credentials.session_token,
    )

    keys = _list_keys_v3(session, res.bucket.name, res.bucket.prefix)

    if not keys:
        return xarray.Dataset()

    timestamp_pattern = re.compile(r"\d{4}/\d{2}/\d{2}/\d{2}/\d{2}/\d{2}")
    data_vars = {}

    with rasterio.env.Env(
        session=rasterio.session.AWSSession(session),
        GDAL_DISABLE_READDIR_ON_OPEN=True,
    ):
        first_file = rioxarray.open_rasterio(
            f"s3://{res.bucket.name}/{keys[0]}", chunks="auto"
        )

    for key in keys:
        filename = key.split("/")[-1]

        file_info = res.file_mapping.get(filename)
        if not file_info:
            continue

        lazy_array = dask.array.from_delayed(
            dask.delayed(_load_file_v3)(session, f"s3://{res.bucket.name}/{key}"),
            shape=first_file.shape,
            dtype=file_info.type,
        )
        lazy_da = xarray.DataArray(
            lazy_array,
            dims=first_file.dims,
            coords=dict(first_file.coords),
            # attrs=first_file.attrs.copy() # TODO: not the same for all files
        )
        # lazy_da.encoding = first_file.encoding.copy()
        # lazy_da.rio.write_crs(first_file.rio.crs, inplace=True)
        # lazy_da.rio.write_transform(first_file.rio.transform(), inplace=True)

        timestamp_str = timestamp_pattern.search(key).group()

        for band_num, var_name in enumerate(file_info.bands, start=1):
            band_da = lazy_da.sel(band=band_num, drop=True)
            band_da.name = var_name

            # Dataset with time dimension
            if timestamp_str != "0000/00/00/00/00/00":
                t = datetime.strptime(timestamp_str, "%Y/%m/%d/%H/%M/%S")
                band_da = band_da.expand_dims("time")
                band_da = band_da.assign_coords(time=[t])

            if var_name not in data_vars:
                data_vars[var_name] = []

            data_vars[var_name].append(band_da)

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
            "subscription_id": res.data_request_id,
        },
    )


def _load_file_v3(aws_session: boto3.session.Session, url: str):
    with rasterio.env.Env(
        session=rasterio.session.AWSSession(aws_session),
        GDAL_DISABLE_READDIR_ON_OPEN=True,
    ):
        return rioxarray.open_rasterio(
            url,
            chunks="auto",
        ).values
        # ).sel(band=num_band, drop=True)
        # ).sel(band=num_band, drop=True).values
        # ).isel(band=num_band-1).values


def _list_keys_v3(session: boto3.session.Session, bucket_name, prefix) -> list[str]:
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


# v4


def load_xarray_v4(res: SubscriptionListFiles) -> xarray.Dataset:

    session = boto3.session.Session(
        aws_access_key_id=res.credentials.access_key_id,
        aws_secret_access_key=res.credentials.secret_access_key,
        aws_session_token=res.credentials.session_token,
    )

    keys = _list_keys_v2(session, res.bucket.name, res.bucket.prefix)

    if not keys:
        return xarray.Dataset()

    first_file_metadata = _get_file_metadata_v4(session, res.bucket.name, keys[0])

    timestamp_pattern = re.compile(r"\d{4}/\d{2}/\d{2}/\d{2}/\d{2}/\d{2}")

    data_vars = {}
    for key in keys:
        filename = key.split("/")[-1].rsplit(".", 1)[0]

        file_info = res.file_mapping.get(filename)
        if not file_info:
            continue

        timestamp_str = timestamp_pattern.search(key).group()

        for band_num, band_name in enumerate(file_info.bands, start=1):
            array = _create_dask_array_v4(
                session,
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
            "dataset_name": res.dataset_name,
            "dataset_id": res.dataset_id,
            "aoi_id": res.aoi_id,
            "subscription_id": res.data_request_id,
        },
    )
    ds = ds.rio.write_crs(first_file_metadata["crs"])

    return ds


def _get_file_metadata_v4(session, bucket: str, path: str):
    with rasterio.env.Env(
        rasterio.session.AWSSession(session), GDAL_DISABLE_READDIR_ON_OPEN=True
    ):
        da = xarray.open_dataarray(f"s3://{bucket}/{path}", engine="rasterio")

    return {
        "crs": da.rio.crs,
        "height": da.rio.height,
        "width": da.rio.width,
        "x": da.x.values,
        "y": da.y.values,
    }


def _create_dask_array_v4(
    session: boto3.session.Session,
    file_path: str,
    band_num: int,
    height: int,
    width: int,
    dtype: str,
):
    rasterio_session = rasterio.session.AWSSession(session)

    def read_chunk():
        with rasterio.env.Env(
            session=rasterio_session, GDAL_DISABLE_READDIR_ON_OPEN=True
        ):
            with rasterio.open(file_path) as src:
                return src.read(band_num)

    return dask.array.from_delayed(
        dask.delayed(read_chunk)(), shape=(height, width), dtype=dtype
    )
