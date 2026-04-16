import json
import time

import geopandas
import pandas
import pyarrow.compute
import pyarrow.dataset
import pyarrow.fs
import rasterio.features

from .models.subscription import SubscriptionParquet, SubscriptionSelfHostedParquet


def load_self_hosted_dataframe(
    res: SubscriptionSelfHostedParquet,
) -> geopandas.GeoDataFrame:
    aoi_gdf = geopandas.GeoDataFrame.from_features(
        [{"type": "Feature", "geometry": res.geometry, "properties": {}}],
        crs="EPSG:4326",
    )

    minx, miny, maxx, maxy = rasterio.features.bounds(res.geometry)

    pa_fs = pyarrow.fs.S3FileSystem(
        access_key=res.credentials.access_key_id,
        secret_key=res.credentials.secret_access_key,
        session_token=res.credentials.session_token,
        scheme="https",
    )

    prefix = f"{res.bucket.name}/{res.bucket.prefix}"
    bbox_filter = (
        (pyarrow.compute.field("bbox", "xmin") <= maxx)
        & (pyarrow.compute.field("bbox", "xmax") >= minx)
        & (pyarrow.compute.field("bbox", "ymin") <= maxy)
        & (pyarrow.compute.field("bbox", "ymax") >= miny)
    )

    file_infos = pa_fs.get_file_info(pyarrow.fs.FileSelector(prefix))
    parquet_files = [f.path for f in file_infos if f.path.endswith(".parquet")]

    gdfs = []
    for path in parquet_files:
        dataset = pyarrow.dataset.dataset(path, filesystem=pa_fs)
        table = dataset.to_table(filter=bbox_filter)
        _gdf = geopandas.GeoDataFrame.from_arrow(table)
        gdfs.append(_gdf)

    gdf = pandas.concat(gdfs, ignore_index=True)
    gdf = geopandas.GeoDataFrame(gdf, crs=gdfs[0].crs)

    aoi_gdf = aoi_gdf.to_crs(gdf.crs)

    gdf_clipped = gdf[gdf.intersects(aoi_gdf.union_all())].copy()
    gdf_clipped.geometry = gdf_clipped.geometry.make_valid()

    gdf_clipped["subscription_id"] = res.subscription_id
    gdf_clipped["aoi_id"] = res.aoi_id

    return gdf_clipped


def load_dataframe(res: SubscriptionParquet) -> geopandas.GeoDataFrame:
    if not res.files:
        return geopandas.GeoDataFrame()

    # geopandas does not have a solution to concat geodataframes, official documentation
    # suggest the use of pandas, it will return a geodataframe instead of a dataframe
    # since it's just concatenating existing geodataframes
    return pandas.concat(
        (
            _retry_with_exponential_backoff(_parquet_to_geodataframe, 5, 1, 2, f)
            for f in res.files
        )
    ).reset_index(drop=True)


def _parquet_to_geodataframe(file):
    df = pandas.read_parquet(file)
    if "geojson" in df.columns:
        features = [
            {"type": "Feature", "geometry": json.loads(g), "properties": {}}
            for g in df["geojson"]
        ]
        geometry = geopandas.GeoDataFrame.from_features(
            features, crs="EPSG:4326"
        ).geometry
        gdf = geopandas.GeoDataFrame(
            df.drop(columns=["geojson"]), geometry=geometry, crs="EPSG:4326"
        )
    else:
        geometry = geopandas.GeoSeries.from_wkt(
            ["MULTIPOLYGON EMPTY"] * len(df), crs="EPSG:4326"
        )
        gdf = geopandas.GeoDataFrame(df, geometry=geometry, crs="EPSG:4326")

    return gdf


def _retry_with_exponential_backoff(
    func, retries: int, start_delay: int, multiplier: float, *args, **kwargs
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
