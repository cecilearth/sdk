from typing import List, Optional

from .base import Base


class Dataset(Base):
    id: str
    name: str
    categories: List[str]
    type: str
    provider: "Provider"
    licence: "Licence"
    crs: Optional[str] = None
    spatial_coverage: "SpatialCoverage"
    spatial_resolution: Optional["SpatialResolution"] = None
    temporal_coverage: "TemporalCoverage"
    temporal_resolution: Optional["TemporalResolution"] = None
    version: "Version"


class Licence(Base):
    type: str


class Provider(Base):
    name: str


class SpatialCoverage(Base):
    nominal: str


class SpatialResolution(Base):
    x: Optional[float]
    y: Optional[float]
    units: Optional[str]
    nominal: str


class TemporalCoverage(Base):
    nominal: str


class TemporalResolution(Base):
    nominal: str


class Version(Base):
    date: Optional[str]
    number: Optional[str]
