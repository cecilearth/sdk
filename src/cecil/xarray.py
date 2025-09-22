import rioxarray
import xarray

from datetime import datetime

from .errors import Error
from .models import DataRequestMetadata

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

def load_xarray_v2(metadata: DataRequestMetadata, align=False) -> xarray.Dataset:
    data_vars = {}

    for f in metadata.files:
        dataset = rioxarray.open_rasterio(f.url, chunks={"x": 2000, "y": 2000})

        for b in f.bands:
            band = dataset.sel(band=b.number, drop=True)

            if b['time'] and b['time_pattern']:
                time = datetime.strptime(b.time, b.time_pattern)
                band = band.expand_dims("time")
                band = band.assign_coords(time=[time])

            band.name = b.variable_name

            if b.variable_name not in data_vars:
                data_vars[b.variable_name] = []

            data_vars[b.variable_name].append(band)

    for variable_name, time_series in data_vars.items():
        if 'time' in time_series[0].dims:
            if align:
                time_series = align_pixel_grids(time_series)
            data_vars[variable_name] = xarray.concat(time_series, dim="time", join="exact")
        else:
            data_vars[variable_name] = time_series[0]

    return xarray.Dataset(
        data_vars=data_vars,
        attrs={
            "dataset_name": metadata.dataset_name,
            "dataset_id": metadata.dataset_id,
            "aoi_id": metadata.aoi_id,
            "data_request_id": metadata.data_request_id,
            "crs": metadata.dataset_crs,
        }
    )


def load_xarray(data_request_metadata: DataRequestMetadata) -> xarray.Dataset:
    var_to_files = {}

    for file_info in data_request_metadata.files:
        for band_info in file_info.bands:
            var_name = band_info.variable_name

            if var_name not in var_to_files:
                var_to_files[var_name] = []

            var_to_files[var_name].append(
                {
                    "file_info": file_info,
                    "band_number": band_info.number,
                    "band_info": band_info,
                }
            )

    datasets = {}

    for var_name, file_band_list in var_to_files.items():

        time_series = []

        def sort_key(x):
            time = x["band_info"].time
            pattern = x["band_info"].time_pattern
            # validate why we need this min value
            return datetime.min if time is None else datetime.strptime(time, pattern)

        file_band_list.sort(key=sort_key)

        for item in file_band_list:
            file_info = item["file_info"]
            band_num = item["band_number"]
            band_info = item["band_info"]

            try:
                da = rioxarray.open_rasterio(
                    file_info.url, chunks={"x": 2000, "y": 2000}
                )
                da_band = da.sel(band=band_num, drop=True)
                time_coord = None

                time_str = band_info.time
                # review and remove duplicated time logic
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

                time_series.append(da_band)

            except Exception as e:
                raise Error

        # validate if we can always provide a time dimension
        if time_series:
            has_time_dims = [ts for ts in time_series if "time" in ts.dims]
            no_time_dims = [ts for ts in time_series if "time" not in ts.dims]

            processed_series = []

            if has_time_dims:
                if len(has_time_dims) > 1:
                    time_data = xarray.concat(has_time_dims, dim="time")
                else:
                    time_data = has_time_dims[0]

                processed_series.append(time_data)

            if no_time_dims:
                processed_series.extend(no_time_dims)
            var_data = processed_series[0]

            datasets[var_name] = var_data
        else:
            print(f"  Warning: No data successfully loaded for {var_name}")

    try:
        combined_ds = xarray.Dataset(datasets)
        # validate if we can assume all variables will be compatible across datasets
    except Exception as e:
        print(f"Warning: Could not combine all variables into single dataset: {e}")
        print("Creating dataset with compatible variables only...")

        compatible_vars = {}
        for var_name, var_data in datasets.items():
            spatial_dims = tuple(
                sorted(
                    [dim for dim in var_data.dims if dim in ["x", "y", "lat", "lon"]]
                )
            )
            spatial_shape = tuple(
                var_data.sizes[dim] for dim in spatial_dims if dim in var_data.sizes
            )
            key = (spatial_dims, spatial_shape)

            if key not in compatible_vars:
                compatible_vars[key] = {}
            compatible_vars[key][var_name] = var_data

        if compatible_vars:
            best_group = max(compatible_vars.values(), key=len)
            combined_ds = xarray.Dataset(best_group)

            excluded_vars = set(datasets.keys()) - set(best_group.keys())
            if excluded_vars:
                print(
                    f"Note: Excluded variables due to incompatible dimensions: {excluded_vars}"
                )
        else:
            first_var = list(datasets.keys())[0]
            combined_ds = xarray.Dataset({first_var: datasets[first_var]})
            print(f"Fallback: Using only {first_var}")

    combined_ds.attrs.update(
        {
            "dataset_name": data_request_metadata.dataset_name,
            "dataset_id": data_request_metadata.dataset_id,
            "aoi_id": data_request_metadata.aoi_id,
            "data_request_id": data_request_metadata.data_request_id,
            "crs": data_request_metadata.dataset_crs,
        }
    )

    return combined_ds
