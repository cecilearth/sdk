import os
import rioxarray
import xarray

from datetime import datetime

from .errors import Error
from .models import DataRequestMetadata

os.environ["GDAL_NUM_THREADS"] = "1"
os.environ["GDAL_DISABLE_READDIR_ON_OPEN"] = "FALSE"


def align_pixel_grids(time_series):
    # Use the first timestep as reference
    reference_da = time_series[0]
    aligned_series = [reference_da]

    # Align all other timesteps to the reference grid
    for i, da in enumerate(time_series[1:], 1):
        try:
            aligned_da = da.rio.reproject_match(reference_da)
            aligned_series.append(aligned_da)
        except Exception as e:
            raise Error
            continue

    return aligned_series


def load_xarray(metadata: DataRequestMetadata) -> xarray.Dataset:
    data_vars = {}

    for f in metadata.files:
        dataset = rioxarray.open_rasterio(
            f.url,
            chunks={"x": 2000, "y": 2000},
        )

        for b in f.bands:
            band = dataset.sel(band=b.number, drop=True)

            if b.time and b.time_pattern:
                time = datetime.strptime(b.time, b.time_pattern)
                band = band.expand_dims("time")
                band = band.assign_coords(time=[time])

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
