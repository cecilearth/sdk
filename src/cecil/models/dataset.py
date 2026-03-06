from typing import Any, Dict, List, Optional

from .base import Base


class Dataset(Base):
    id: str
    name: str
    type: str
    crs: Optional[str] = None
    description: List[str]
    usage_notes: List[str]
    categories: List[str]
    spatial_coverage: "SpatialCoverage"
    spatial_resolution: Optional["SpatialResolution"] = None
    temporal_coverage: "TemporalCoverage"
    temporal_resolution: Optional["TemporalResolution"] = None
    licence: "Licence"
    version: "Version"
    constraints: "Constraints"
    variables: List["Variable"]
    pricing: "Pricing"
    resources: List["Resource"]
    providers: List["Provider"]


class SpatialCoverage(Base):
    nominal: str


class SpatialResolution(Base):
    x: Optional[float] = None
    y: Optional[float] = None
    units: Optional[str] = None
    nominal: str


class TemporalCoverage(Base):
    nominal: str


class TemporalResolution(Base):
    nominal: str


class Licence(Base):
    type: str


class Version(Base):
    date: Optional[str] = None
    number: Optional[str] = None


class Constraints(Base):
    aoi_min_hectares: Optional[float] = None
    aoi_max_hectares: Optional[float] = None
    aoi_min_latitude: Optional[float] = None
    aoi_max_latitude: Optional[float] = None
    aoi_max_vertices: Optional[int] = None
    aoi_geometry_types: Optional[List[str]] = None
    organisation_verified_only: bool


class Variable(Base):
    name: str
    data_type: str
    no_data: Optional[str]
    units: Optional[str]
    description: List[str]
    usage_notes: List[str]
    reference_table: List[Dict[str, Any]]


class Pricing(Base):
    description: List[str]
    tiers: Optional[List["PricingTier"]]


class PricingTier(Base):
    volume: str
    price: "Price"


class Price(Base):
    amount: float
    nominal: str


class Resource(Base):
    type: str
    title: str
    href: str


class Provider(Base):
    name: str
