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
    columns: list[str] | None = None,
) -> geopandas.GeoDataFrame:
    aoi_gdf_default = geopandas.GeoDataFrame.from_features(
        [{"type": "Feature", "geometry": res.geometry, "properties": {}}],
        crs="EPSG:4326",
    )

    pa_fs = pyarrow.fs.S3FileSystem(
        access_key=res.credentials.access_key_id,
        secret_key=res.credentials.secret_access_key,
        session_token=res.credentials.session_token,
        scheme="https",
    )
    prefix = f"{res.bucket.name}/{res.bucket.prefix}"

    file_infos = pa_fs.get_file_info(pyarrow.fs.FileSelector(prefix))
    parquet_files = [f.path for f in file_infos if f.path.endswith(".parquet")]

    # Opening the dataset reads the parquet footer, which the filtered scan
    # below needs anyway; taking the CRS from its schema avoids a second
    # footer fetch per file (the footer alone is several MB on wide datasets)
    datasets = [
        pyarrow.dataset.dataset(path, filesystem=pa_fs) for path in parquet_files
    ]
    parquet_crs, primary_column = _get_geo_metadata(datasets[0].schema)
    aoi_gdf = aoi_gdf_default.to_crs(parquet_crs)

    minx, miny, maxx, maxy = aoi_gdf.total_bounds
    bbox_filter = (
        (pyarrow.compute.field("bbox", "xmin") <= maxx)
        & (pyarrow.compute.field("bbox", "xmax") >= minx)
        & (pyarrow.compute.field("bbox", "ymin") <= maxy)
        & (pyarrow.compute.field("bbox", "ymax") >= miny)
    )

    read_columns = None
    if columns is not None:
        read_columns = list(dict.fromkeys(["bbox", primary_column, *columns]))

    gdfs = []
    for dataset in datasets:
        table = dataset.to_table(filter=bbox_filter, columns=read_columns)
        _gdf = geopandas.GeoDataFrame.from_arrow(table)
        gdfs.append(_gdf)

    gdf = pandas.concat(gdfs, ignore_index=True)
    gdf = geopandas.GeoDataFrame(gdf, geometry="geometry", crs=parquet_crs)

    aoi_geom = aoi_gdf.union_all()

    gdf_clipped = gdf[gdf.intersects(aoi_geom)].copy()
    gdf_clipped.geometry = gdf_clipped.geometry.make_valid()

    gdf_clipped = gdf_clipped.drop(columns=["bbox"], errors="ignore")
    gdf_clipped.insert(0, "aoi_id", res.aoi_id)
    gdf_clipped.insert(0, "subscription_id", res.subscription_id)

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


def _get_geo_metadata(schema) -> tuple[str, str]:
    geo_metadata = json.loads(schema.metadata[b"geo"].decode())

    primary_column = geo_metadata.get("primary_column", "geometry")
    crs = geo_metadata["columns"][primary_column]["crs"]

    return crs, primary_column


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
