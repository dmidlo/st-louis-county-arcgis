from __future__ import annotations

from typing import Any, Literal, Optional

from pydantic import BaseModel, Field, ConfigDict


JsonDict = dict[str, Any]
MatchMethod = Literal["primary", "attribute", "spatial"]


class ArcGISFeature(BaseModel):
    model_config = ConfigDict(extra="ignore")

    attributes: JsonDict = Field(default_factory=dict)
    geometry: Optional[JsonDict] = None


class QueryPage(BaseModel):
    layer_id: int
    offset: int
    page_size: int
    features: list[ArcGISFeature]
    exceeded_transfer_limit: Optional[bool] = None
    next_offset: Optional[int] = None
    raw: JsonDict = Field(default_factory=dict)


class ServiceInfo(BaseModel):
    base_url: str
    spatial_reference_wkid: Optional[int] = None
    max_record_count: Optional[int] = None
    layers: list[JsonDict] = Field(default_factory=list)
    tables: list[JsonDict] = Field(default_factory=list)
    raw: JsonDict = Field(default_factory=dict)


class LayerInfo(BaseModel):
    layer_id: int
    name: str
    type: Optional[str] = None
    geometry_type: Optional[str] = None
    object_id_field: str = "OBJECTID"
    fields: list[str] = Field(default_factory=list)

    max_record_count: Optional[int] = None

    supports_pagination: Optional[bool] = None
    supports_order_by: Optional[bool] = None
    supports_spatial_filter: Optional[bool] = None

    raw: JsonDict = Field(default_factory=dict)


class LayerQueryResult(BaseModel):
    layer_id: int
    layer_name: str
    match_method: MatchMethod
    features: list[ArcGISFeature]
    layer_info: LayerInfo


class ParcelBundle(BaseModel):
    parcel_key: str
    primary_layer_id: int
    primary_feature: ArcGISFeature
    service_info: ServiceInfo
    layer_catalog: dict[int, LayerInfo]
    matches: list[LayerQueryResult]
    address_points: list[ArcGISFeature] = Field(default_factory=list)


class AddressBundle(BaseModel):
    address_key: str
    primary_layer_id: int
    primary_feature: ArcGISFeature
    service_info: ServiceInfo
    layer_catalog: dict[int, LayerInfo]
    matches: list[LayerQueryResult]
    linked_parcel: Optional[ParcelBundle] = None
