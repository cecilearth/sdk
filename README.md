# Cecil SDK

[![PyPI - Version](https://img.shields.io/pypi/v/cecil-sdk.svg)](https://pypi.org/project/cecil-sdk)
[![PyPI - Python Version](https://img.shields.io/pypi/pyversions/cecil-sdk.svg)](https://pypi.org/project/cecil-sdk)

-----

## Table of Contents

- [Installation](#installation)
- [Authentication](#authentication)
- [License](#license)
- [Examples](#examples)

## Installation

```shell
pip install cecil
```

## Authentication

Set `CECIL_API_KEY` environment variable to your Cecil API key.

## Examples

### Create an AOI and data request using the Cecil client

```python
import cecil

client = cecil.Client()

my_aoi = client.create_aoi(
    name="My AOI",
    geometry={
        "type": "Polygon",
        "coordinates": [
            [
                [145.410408835, -42.004083838],
                [145.410408835, -42.004203978],
                [145.410623191, -42.004203978],
                [145.410623191, -42.004083838],
                [145.410408835, -42.004083838],
            ]
        ],
    },
)

# Get dataset ID from docs.cecil.earth -> Datasets
planet_forest_carbon_diligence_id = "c2dd4f55-56f6-4d05-aae3-ba7c1dcd812f"

my_data_request = client.create_data_request(
    aoi_id=my_aoi.id,
    dataset_id=planet_forest_carbon_diligence_id,
)

print(client.get_data_request(my_data_request.id))
```

### Create a transformation using the Cecil client

```python
my_transformation = client.create_transformation(
    data_request_id=my_data_request.id,
    crs="EPSG:4326",
    spatial_resolution=0.005,
)

print(client.get_transformation(my_transformation.id))
```

### Query data (once transformation is completed)

```python
df = client.query(f'''
    SELECT *
    FROM
        planet.forest_carbon_diligence
    WHERE
        transformation_id = '{my_transformation.id}'
''')
```

### Other client methods:

```python
client.list_aois()

client.get_aoi(my_aoi.id)

client.list_data_requests()

client.get_data_request(my_data_request.id)

client.list_transformations()

client.get_transformation(my_transformation.id)
```

## License

`cecil` is distributed under the terms of the [MIT](https://spdx.org/licenses/MIT.html) license.
