# Changelog

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
