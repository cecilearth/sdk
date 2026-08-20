# Changelog

## 0.1.13 - 2026-08-20
- Improved `load_dataframe()` performance for self-hosted datasets: the parquet footer is now read once per file instead of twice (up to ~3s faster per call on wide datasets).
- Added optional `columns` parameter to `load_dataframe()` to load a subset of columns (self-hosted datasets only); `geometry` is always included.
- `load_xarray()` now applies `scale_factor`/`add_offset` for bands that declare them in the dataset file schema: values are returned as physical float32 with NaN nodata, and the packing metadata moves to each variable's `.encoding`. Pass `mask_and_scale=False` to get raw packed values with the true `scale_factor`/`add_offset` as attributes instead. Bands without a declared scale/offset are unaffected.
- Fixed `load_xarray()` stamping the first file's `scale_factor`/`add_offset` attributes onto every variable; variables without a declared scale/offset no longer carry these attributes.
- Added `geometry_type` and `location_count` to the `AOI` model (returned by the API since 2026-08-20).

## 0.1.12 - 2026-05-11
- Improves `load_dataframe()` compatibility with more vector dataset types across different coordinate reference systems.

## 0.1.11 - 2026-04-27
- Changed `load_dataframe()` to return GeoDataframe format.
- Upgraded Python required version to `3.11`.

## 0.1.10 - 2026-03-30

- Added `get_dataset()`.
- Added all details to `Dataset`.
- Improved error handling.
- Made `external_ref` nullable for `Subscription` and `AOI`.
- Renamed `get_organisation_settings()` to `get_settings()`.
- Renamed `update_organisation_settings()` to `update_settings()`.

## 0.1.9 - 2026-01-27

- Changed `load_dataframe()` to retry when failing to load files from bucket.

## 0.1.8 - 2026-01-20

### Webhook improvements

- Added `list_webhooks()`.
- Added `get_webhook()`.
- Renamed `webhook_configure()` to `create_webhook()`.
- Changed `delete_webhook()` to receive an `id` param.

### Archive/restore AOI

- Added `archive_aoi()`.
- Added `restore_aoi()`.
- Changed `list_aois()` to accept an optional `archived` param.

### Archive/restore subscription

- Added `archive_subscription()`.
- Added `restore_subscription()` within the grace period.
- Changed `list_subscriptions()` to accept an optional `archived` param.

## 0.1.7 - 2026-01-13

- Added `configure_webhook()`.
- Added `delete_webhook()`.
- Added `HTTPError` and `SDKError`.
- Improved error handling.

## 0.1.6 - 2026-01-06

- Added `list_datasets()`.

## 0.1.5 - 2025-12-16

- Changed `load_dataframe()` to reset dataframe index after concatenation.

## 0.1.4 - 2025-12-15

- Updated error handling to handle HTTP 403 Forbidden errors.

## 0.1.3 - 2025-12-15

- Renamed `AOIRecord` to `AOI`.

## 0.1.2 - 2025-12-05

- Changed `load_dataframe()` to return empty dataframe when no data is available.

## 0.1.1 - 2025-12-04

- Updated dependencies.

## 0.1.0 - 2025-12-04

- First release.
