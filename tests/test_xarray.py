import numpy as np

from src.cecil.models import Band
from src.cecil.xarray import _band_fill_value


def test_band_fill_value_declared_nodata_uses_storage_dtype():
    fill_value = _band_fill_value(Band(number=1, name="start_day", dtype="int16", nodata=-1))

    assert fill_value == -1
    assert fill_value.dtype == np.dtype("int16")


def test_band_fill_value_float_without_nodata_defaults_to_nan():
    fill_value = _band_fill_value(Band(number=1, name="canopy_height", dtype="float32"))

    assert np.isnan(fill_value)
    assert fill_value.dtype == np.dtype("float32")


def test_band_fill_value_integer_without_nodata_is_none():
    # A boolean mask stored as uint8 has no missing values; NaN is not
    # representable as an integer and must not be forced into one.
    assert _band_fill_value(Band(number=1, name="computed", dtype="uint8")) is None


def test_band_fill_value_scaled_integer_with_nodata():
    fill_value = _band_fill_value(
        Band(number=1, name="blue", dtype="uint16", nodata=0, scale=0.0000275, offset=-0.2)
    )

    assert fill_value == 0
    assert fill_value.dtype == np.dtype("uint16")
