from __future__ import annotations

import asyncio
from typing import Any, Optional

from .arcgis import ArcGISRESTAsyncClient
from .http import AsyncHTTP
from .models import (
    AddressBundle,
    ArcGISFeature,
    LayerInfo,
    LayerQueryResult,
    ParcelBundle,
)
from .settings import ArcGISClientSettings
from .utils import (
    ADDRESS_FIELD_CANDIDATES,
    PARCEL_ID_FIELD_CANDIDATES,
    pick_first_existing_field,
    sql_quote,
)


class StLouisCountyOpenDataClient:
    """
    High-level client for St. Louis County (MN) Open_Data MapServer.
    - async httpx
    - pydantic models
    - robust pagination / ids-only fallback
    """

    def __init__(
        self,
        settings: ArcGISClientSettings | None = None,
        *,
        http_client=None,  # optional injected httpx.AsyncClient
    ) -> None:
        self.settings = settings or ArcGISClientSettings()

        self._http = AsyncHTTP(
            user_agent=self.settings.user_agent,
            timeout_s=self.settings.timeout_s,
            max_retries=self.settings.max_retries,
            backoff_base_s=self.settings.backoff_base_s,
            backoff_max_s=self.settings.backoff_max_s,
            client=http_client,
        )
        self._arc = ArcGISRESTAsyncClient(self.settings.base_url, self._http)

    async def aclose(self) -> None:
        await self._http.aclose()

    async def __aenter__(self) -> "StLouisCountyOpenDataClient":
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        await self.aclose()

    # ---------------------------
    # Discovery / metadata
    # ---------------------------
    async def service_info(self):
        return await self._arc.get_service_info()

    async def layer_catalog(self) -> dict[int, LayerInfo]:
        return await self._arc.build_layer_catalog()

    async def find_layer_id_by_name_contains(self, token: str) -> int | None:
        token_l = token.lower()
        svc = await self._arc.get_service_info()
        for item in list(svc.layers) + list(svc.tables):
            if isinstance(item, dict) and isinstance(item.get("id"), int) and isinstance(item.get("name"), str):
                if token_l in item["name"].lower():
                    return int(item["id"])
        return None

    async def _parcels_layer_id(self) -> int:
        lid = await self.find_layer_id_by_name_contains("parcels")
        if lid is None:
            raise RuntimeError("Could not locate a Parcels layer in the service.")
        return lid

    async def _address_points_layer_id(self) -> int:
        # some services name it "Address Points"
        lid = await self.find_layer_id_by_name_contains("address")
        if lid is None:
            raise RuntimeError("Could not locate an Address Points layer in the service.")
        return lid

    # ---------------------------
    # Paging APIs
    # ---------------------------
    async def first_page(
        self,
        *,
        layer_id: int,
        where: str = "1=1",
        out_fields: str = "*",
        return_geometry: bool = False,
        page_size: Optional[int] = None,
    ):
        ps = page_size or self.settings.default_page_size
        ps = min(ps, self.settings.max_page_size_cap)
        return await self._arc.query_page(
            layer_id=layer_id,
            where=where,
            out_fields=out_fields,
            return_geometry=return_geometry,
            offset=0,
            page_size=ps,
        )

    async def iter_all(
        self,
        *,
        layer_id: int,
        where: str = "1=1",
        out_fields: str = "*",
        return_geometry: bool = False,
        page_size: Optional[int] = None,
        max_features: int | None = None,
    ):
        ps = page_size or self.settings.default_page_size
        ps = min(ps, self.settings.max_page_size_cap)
        async for feat in self._arc.iter_features(
            layer_id=layer_id,
            where=where,
            out_fields=out_fields,
            return_geometry=return_geometry,
            page_size=ps,
            max_features=max_features,
        ):
            yield feat

    async def list_parcels_first_page(self, *, page_size: Optional[int] = None):
        lid = await self._parcels_layer_id()
        return await self.first_page(layer_id=lid, page_size=page_size, return_geometry=False)

    async def list_address_points_first_page(self, *, page_size: Optional[int] = None):
        lid = await self._address_points_layer_id()
        return await self.first_page(layer_id=lid, page_size=page_size, return_geometry=False)

    # ---------------------------
    # Parcel / Address bundles
    # ---------------------------
    async def parcel_bundle(
        self,
        *,
        parcel_number: str,
        include_attribute_joins: bool = True,
        include_spatial_intersects: bool = True,
        max_features_per_layer: int | None = None,
        return_geometries_in_matches: bool = False,
        concurrency: int = 8,
    ) -> ParcelBundle:
        svc = await self._arc.get_service_info()
        catalog = await self._arc.build_layer_catalog()

        parcels_lid = await self._parcels_layer_id()
        parcels_info = catalog[parcels_lid]

        parcel_id_field = pick_first_existing_field(parcels_info.fields, PARCEL_ID_FIELD_CANDIDATES)
        if parcel_id_field is None:
            # Fallback for St. Louis County specific naming
            parcel_id_field = pick_first_existing_field(parcels_info.fields, ["PIN_NUM", "PIN_NUMBER"])
            
        if parcel_id_field is None:
            available_fields = [f.name if hasattr(f, "name") else str(f) for f in parcels_info.fields]
            raise RuntimeError(f"Parcels layer has no recognized parcel-id field. Available fields: {available_fields}")

        pn = parcel_number.strip()
        where = f"{parcel_id_field} = '{sql_quote(pn)}'"

        # primary parcel (need geometry for spatial joins)
        primary_page = await self._arc.query_page(
            layer_id=parcels_lid,
            where=where,
            out_fields="*",
            return_geometry=True,
            offset=0,
            page_size=1,
        )
        if not primary_page.features:
            raise ValueError(f"No parcel found for {parcel_id_field}='{pn}'")
        primary_feature = primary_page.features[0]

        if not primary_feature.geometry:
            include_spatial_intersects = False

        # Address points linked to this parcel (attribute join preferred)
        address_points: list[ArcGISFeature] = []
        addr_lid = await self._address_points_layer_id()
        addr_info = catalog[addr_lid]
        addr_parcel_field = pick_first_existing_field(addr_info.fields, PARCEL_ID_FIELD_CANDIDATES)

        if addr_parcel_field is not None:
            aw = f"{addr_parcel_field} = '{sql_quote(pn)}'"
            feats = []
            async for f in self._arc.iter_features(
                layer_id=addr_lid,
                where=aw,
                out_fields="*",
                return_geometry=False,
                page_size=self.settings.default_page_size,
                max_features=max_features_per_layer,
            ):
                feats.append(f)
            address_points = feats
        elif include_spatial_intersects and primary_feature.geometry and addr_info.geometry_type:
            feats = []
            async for f in self._arc.iter_features(
                layer_id=addr_lid,
                where="1=1",
                out_fields="*",
                return_geometry=False,
                page_size=self.settings.default_page_size,
                geometry=primary_feature.geometry,
                geometry_type="esriGeometryPolygon",
                max_features=max_features_per_layer,
            ):
                feats.append(f)
            address_points = feats

        # Evaluate each layer (attribute-join then spatial)
        sem = asyncio.Semaphore(max(1, concurrency))

        async def _match_layer(lid: int, info: LayerInfo) -> LayerQueryResult | None:
            if lid == parcels_lid:
                return LayerQueryResult(
                    layer_id=lid,
                    layer_name=info.name,
                    match_method="primary",
                    features=[primary_feature],
                    layer_info=info,
                )

            async with sem:
                # Attribute join
                if include_attribute_joins:
                    join_field = pick_first_existing_field(info.fields, PARCEL_ID_FIELD_CANDIDATES)
                    if join_field is not None:
                        w = f"{join_field} = '{sql_quote(pn)}'"
                        feats: list[ArcGISFeature] = []
                        async for f in self._arc.iter_features(
                            layer_id=lid,
                            where=w,
                            out_fields="*",
                            return_geometry=return_geometries_in_matches,
                            page_size=self.settings.default_page_size,
                            max_features=max_features_per_layer,
                        ):
                            feats.append(f)
                        if feats:
                            return LayerQueryResult(
                                layer_id=lid,
                                layer_name=info.name,
                                match_method="attribute",
                                features=feats,
                                layer_info=info,
                            )

                # Spatial intersect
                if include_spatial_intersects and primary_feature.geometry and info.geometry_type:
                    if info.supports_spatial_filter is False:
                        return None
                    feats = []
                    async for f in self._arc.iter_features(
                        layer_id=lid,
                        where="1=1",
                        out_fields="*",
                        return_geometry=return_geometries_in_matches,
                        page_size=self.settings.default_page_size,
                        geometry=primary_feature.geometry,
                        geometry_type="esriGeometryPolygon",
                        max_features=max_features_per_layer,
                    ):
                        feats.append(f)
                    if feats:
                        return LayerQueryResult(
                            layer_id=lid,
                            layer_name=info.name,
                            match_method="spatial",
                            features=feats,
                            layer_info=info,
                        )
                return None

        tasks = [_match_layer(lid, info) for lid, info in catalog.items()]
        results = await asyncio.gather(*tasks)
        matches = [r for r in results if r is not None]

        return ParcelBundle(
            parcel_key=pn,
            primary_layer_id=parcels_lid,
            primary_feature=primary_feature,
            service_info=svc,
            layer_catalog=catalog,
            matches=matches,
            address_points=address_points,
        )

    async def address_bundle(
        self,
        *,
        objectid: int | None = None,
        full_address: str | None = None,
        select_first_if_multiple: bool = True,
        include_attribute_joins: bool = True,
        include_spatial_intersects: bool = True,
        max_features_per_layer: int | None = None,
        return_geometries_in_matches: bool = False,
        also_fetch_linked_parcel: bool = True,
        concurrency: int = 8,
    ) -> AddressBundle:
        svc = await self._arc.get_service_info()
        catalog = await self._arc.build_layer_catalog()

        addr_lid = await self._address_points_layer_id()
        addr_info = catalog[addr_lid]

        # Resolve primary address point
        primary_feature: ArcGISFeature
        address_key: str

        if objectid is not None:
            where = f"{addr_info.object_id_field} = {int(objectid)}"
            page = await self._arc.query_page(
                layer_id=addr_lid, where=where, out_fields="*", return_geometry=True, offset=0, page_size=1
            )
            if not page.features:
                raise ValueError(f"No address point found for {addr_info.object_id_field}={objectid}")
            primary_feature = page.features[0]
            address_key = f"{addr_info.object_id_field}={int(objectid)}"
        else:
            if not full_address or not full_address.strip():
                raise ValueError("Provide either objectid or full_address.")
            addr_field = pick_first_existing_field(addr_info.fields, ADDRESS_FIELD_CANDIDATES)
            if addr_field is None:
                raise RuntimeError("Address layer has no recognized address field.")

            fa = full_address.strip()
            exact = f"{addr_field} = '{sql_quote(fa)}'"
            page = await self._arc.query_page(layer_id=addr_lid, where=exact, out_fields="*", return_geometry=True, offset=0, page_size=10)
            candidates = page.features

            if not candidates:
                like = f"{addr_field} LIKE '%{sql_quote(fa)}%'"
                page2 = await self._arc.query_page(layer_id=addr_lid, where=like, out_fields="*", return_geometry=True, offset=0, page_size=25)
                candidates = page2.features

            if not candidates:
                raise ValueError(f"No address points matched '{fa}' (exact/like).")

            if len(candidates) > 1 and not select_first_if_multiple:
                raise ValueError(f"Multiple address points matched '{fa}' (count={len(candidates)}). Use objectid.")

            primary_feature = candidates[0]
            address_key = f"{addr_field}~'{fa}'"

        if not primary_feature.geometry:
            include_spatial_intersects = False

        # Resolve parcel number from address point if present
        parcel_number: str | None = None
        addr_parcel_field = pick_first_existing_field(addr_info.fields, PARCEL_ID_FIELD_CANDIDATES)
        if addr_parcel_field is not None:
            v = primary_feature.attributes.get(addr_parcel_field)
            if v is not None and str(v).strip():
                parcel_number = str(v).strip()

        # Match layers
        sem = asyncio.Semaphore(max(1, concurrency))

        async def _match_layer(lid: int, info: LayerInfo) -> LayerQueryResult | None:
            if lid == addr_lid:
                return LayerQueryResult(
                    layer_id=lid,
                    layer_name=info.name,
                    match_method="primary",
                    features=[primary_feature],
                    layer_info=info,
                )

            async with sem:
                # Attribute join via parcel number if available
                if include_attribute_joins and parcel_number:
                    join_field = pick_first_existing_field(info.fields, PARCEL_ID_FIELD_CANDIDATES)
                    if join_field is not None:
                        w = f"{join_field} = '{sql_quote(parcel_number)}'"
                        feats = []
                        async for f in self._arc.iter_features(
                            layer_id=lid,
                            where=w,
                            out_fields="*",
                            return_geometry=return_geometries_in_matches,
                            page_size=self.settings.default_page_size,
                            max_features=max_features_per_layer,
                        ):
                            feats.append(f)
                        if feats:
                            return LayerQueryResult(
                                layer_id=lid,
                                layer_name=info.name,
                                match_method="attribute",
                                features=feats,
                                layer_info=info,
                            )

                # Spatial intersects via point
                if include_spatial_intersects and primary_feature.geometry and info.geometry_type:
                    if info.supports_spatial_filter is False:
                        return None
                    feats = []
                    async for f in self._arc.iter_features(
                        layer_id=lid,
                        where="1=1",
                        out_fields="*",
                        return_geometry=return_geometries_in_matches,
                        page_size=self.settings.default_page_size,
                        geometry=primary_feature.geometry,
                        geometry_type="esriGeometryPoint",
                        max_features=max_features_per_layer,
                    ):
                        feats.append(f)
                    if feats:
                        return LayerQueryResult(
                            layer_id=lid,
                            layer_name=info.name,
                            match_method="spatial",
                            features=feats,
                            layer_info=info,
                        )
                return None

        tasks = [_match_layer(lid, info) for lid, info in catalog.items()]
        results = await asyncio.gather(*tasks)
        matches = [r for r in results if r is not None]

        linked_parcel = None
        if also_fetch_linked_parcel and parcel_number:
            try:
                linked_parcel = await self.parcel_bundle(
                    parcel_number=parcel_number,
                    include_attribute_joins=True,
                    include_spatial_intersects=True,
                    max_features_per_layer=max_features_per_layer,
                    return_geometries_in_matches=return_geometries_in_matches,
                )
            except Exception:
                linked_parcel = None

        return AddressBundle(
            address_key=address_key,
            primary_layer_id=addr_lid,
            primary_feature=primary_feature,
            service_info=svc,
            layer_catalog=catalog,
            matches=matches,
            linked_parcel=linked_parcel,
        )
