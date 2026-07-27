# Cecil SDK

Python SDK for the Cecil API. Manage AOIs, subscriptions, users, and webhooks; load subscription outputs directly into `xarray` (Zarr, TIFF) and `geopandas` (parquet).

Full API reference and product docs: [docs.cecil.earth](https://docs.cecil.earth).

## Setup

**Requirements**

- Python `>=3.11`
- A Cecil API key (get one via the Cecil web app or `Client.recover_api_key`)

**Install**

```
pip install cecil
```

**Authenticate**

The SDK reads your API key from the `CECIL_API_KEY` environment variable:

```
export CECIL_API_KEY=<your-cecil-api-key>
```

## Development

**First request**

```python
import os
from cecil import Client

client = Client()

# List subscriptions in your organisation
subscriptions = client.list_subscriptions()
for s in subscriptions:
    print(s.id, s.dataset_id)
```

**Create an AOI and subscribe it to a dataset**

```python
aoi = client.create_aoi(
    geometry={
        "type": "Polygon",
        "coordinates": [[[-0.15, 51.50], [-0.10, 51.50], [-0.10, 51.55], [-0.15, 51.55], [-0.15, 51.50]]],
    },
    external_ref="my-aoi-1",
)

subscription = client.create_subscription(
    aoi_id=aoi.id,
    dataset_id="<dataset-uuid>",
    external_ref="my-sub-1",
)
```

**Load subscription output into `xarray`**

```python
ds = client.load_xarray(subscription_id=subscription.id)
```

Under the hood this fetches the subscription's file listing and reads Zarr or TIFF as appropriate.

**Load a self-hosted dataset as a GeoDataFrame**

```python
gdf = client.load_dataframe(subscription_id=subscription.id)
```

**Point the client at a non-prod environment**

```python
client = Client(env="dev")     # https://dev.cecil.earth
client = Client(env="staging") # https://staging.cecil.earth
# omit env for prod → https://api.cecil.earth
```

### Local development

If you're editing the SDK itself, see [CONTRIBUTING.md](CONTRIBUTING.md) for the editable-install and PyPI test-upload flow. Short version:

**Clone**

```
git clone git@github.com:cecilearth/sdk.git
cd sdk
```

**Install packaging tools + editable mode**

```
pip install hatch twine black
pip install -e .
```

**Test**

```
make test
```

Runs `hatch test -v` (uses the `responses` library to stub HTTP; no live API calls).

**Lint / format**

```
black src tests
```

## Deploy

The SDK is published to PyPI, not deployed to AWS.

**Publish a release candidate to Test PyPI**

```
make publish.test
```

**Publish to production PyPI**

```
make publish.prod
```

Both commands run `clean`, `build` (via `hatch build`), then `twine upload`. Put your Test PyPI and PyPI tokens in `~/.pypirc` first (see [CONTRIBUTING.md](CONTRIBUTING.md)). Bump `src/cecil/version.py` and add a `CHANGELOG.md` entry before publishing.

## Architecture

Single-package Python project built with `hatch`.

- `src/cecil/`
  - `__init__.py` — public surface: `Client`, `Error`, `__version__`.
  - `client.py` — the main `Client` class. Constructor picks a base URL from the `env` arg (defaults to `https://api.cecil.earth`). Every method delegates through `_request()`, which pulls `CECIL_API_KEY` from the environment on demand and attaches HTTP Basic Auth (username = key, no password).
  - `dataframe.py` — `load_dataframe()` / `load_self_hosted_dataframe()` — retrieves parquet URLs from the API and reads them via `pyarrow` + `geopandas`.
  - `xarray.py` — `load_xarray_from_tiff()` / `load_xarray_from_zarr()` — reads raster subscription outputs via `rioxarray` / `zarr` (with `s3fs`/`dask` for large Zarr).
  - `models/` — Pydantic models: `AOI`, `Dataset`, `Settings`, `Subscription` (+ `SubscriptionTIFF`, `SubscriptionZarr`, `SubscriptionParquet`, `SubscriptionSelfHostedParquet`, `SubscriptionFormat`, `SubscriptionStorage`), `User`, `Webhook`.
  - `errors.py` — `Error`, `HTTPError`, `SDKError`.
  - `version.py` — `__version__` (`hatch` reads this dynamically).
- `tests/` — unit tests using `responses` to stub HTTP.
- `pyproject.toml` — declares the `cecil` package, deps (`boto3`, `dask`, `geopandas`, `pyarrow`, `pydantic`, `requests`, `rioxarray`, `s3fs`, `xarray`, `zarr`), Python `>=3.11`.
- `Makefile` — `build`, `clean`, `test`, `publish.test`, `publish.prod`.
- `CHANGELOG.md` — release notes.
- `CONTRIBUTING.md` — dev install + PyPI setup.
- `LICENSE.txt` — MIT.

**How it fits with sibling repos**

- Every method on `Client` maps to a `/v0/*` route served by [`cecilearth/api`](https://github.com/cecilearth/api) (see `handler/api/`).
- Subscription output files (Zarr, TIFF, geoparquet) are read straight from the `subscription-tiff` and dataset S3 buckets provisioned in `api`'s Terraform — the client fetches presigned URLs via the API, then reads the bytes directly.
- [`cecilearth/examples`](https://github.com/cecilearth/examples) contains Jupyter notebooks that use this SDK end-to-end.

## Cross-links

- Docs: [docs.cecil.earth](https://docs.cecil.earth)
- Onboarding: https://github.com/cecilearth/onboarding/blob/main/docs/platform-architecture.md
- Sibling repos:
  - [cecilearth/api](https://github.com/cecilearth/api) — the backend this SDK targets
  - [cecilearth/examples](https://github.com/cecilearth/examples) — end-to-end notebooks using this SDK
  - [cecilearth/cecil-assistant](https://github.com/cecilearth/cecil-assistant) — CLI assistant that also uses `CECIL_API_KEY`
