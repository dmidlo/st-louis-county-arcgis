from __future__ import annotations

import asyncio
from typing import Any, AsyncIterator, Iterable

from .exceptions import ArcGISError
from .http import AsyncHTTP
from .models import ArcGISFeature, LayerInfo, QueryPage, ServiceInfo
from .utils import to_esri_json_str


class ArcGISRESTAsyncClient:
    """
    ArcGIS Server REST client with:
    - service/layer metadata caching
    - robust iteration over all results (pagination OR ids-only fallback)
    """

    def __init__(self, base_url: str, http: AsyncHTTP) -> None:
        self.base = base_url.rstrip("/")
        self._http = http
        self._service_cache: ServiceInfo | None = None
        self._layer_cache: dict[int, LayerInfo] = {}

    def _check_error(self, payload: dict[str, Any], *, url: str) -> None:
        err = payload.get("error")
        if isinstance(err, dict):
            msg = err.get("message") or "ArcGIS REST error"
            raise ArcGISError(msg, details={"url": url, **err})

    async def get_service_info(self) -> ServiceInfo:
        if self._service_cache is not None:
            return self._service_cache

        url = f"{self.base}"
        payload = await self._http.request_json(url, method="GET", params={"f": "pjson"})
        self._check_error(payload, url=url)

        sr = payload.get("spatialReference") or {}
        wkid = sr.get("latestWkid") or sr.get("wkid")
        max_rc = payload.get("maxRecordCount")

        info = ServiceInfo(
            base_url=self.base,
            spatial_reference_wkid=int(wkid) if isinstance(wkid, int) else None,
            max_record_count=int(max_rc) if isinstance(max_rc, int) else None,
            layers=list(payload.get("layers") or []),
            tables=list(payload.get("tables") or []),
            raw=payload,
        )
        self._service_cache = info
        return info

    async def list_layer_ids(self) -> list[int]:
        svc = await self.get_service_info()
        out: set[int] = set()
        for item in list(svc.layers) + list(svc.tables):
            if isinstance(item, dict) and isinstance(item.get("id"), int):
                out.add(int(item["id"]))
        return sorted(out)

    async def get_layer_info(self, layer_id: int) -> LayerInfo:
        if layer_id in self._layer_cache:
            return self._layer_cache[layer_id]

        svc = await self.get_service_info()
        url = f"{self.base}/{layer_id}"
        payload = await self._http.request_json(url, method="GET", params={"f": "pjson"})
        self._check_error(payload, url=url)

        fields = [f.get("name") for f in payload.get("fields", []) if isinstance(f, dict) and f.get("name")]
        object_id_field = payload.get("objectIdField") or "OBJECTID"

        aqc = payload.get("advancedQueryCapabilities") or {}
        supports_pagination = aqc.get("supportsPagination") if isinstance(aqc, dict) else None
        supports_order_by = aqc.get("supportsOrderBy") if isinstance(aqc, dict) else None

        supports_spatial_filter = payload.get("supportsSpatialFilter")
        if supports_spatial_filter is None:
            supports_spatial_filter = svc.raw.get("supportsSpatialFilter")

        max_record_count = payload.get("maxRecordCount")

        info = LayerInfo(
            layer_id=int(payload.get("id", layer_id)),
            name=str(payload.get("name", f"layer_{layer_id}")),
            type=str(payload.get("type")) if payload.get("type") is not None else None,
            geometry_type=str(payload.get("geometryType")) if payload.get("geometryType") is not None else None,
            object_id_field=str(object_id_field),
            fields=[str(x) for x in fields],
            max_record_count=int(max_record_count) if isinstance(max_record_count, int) else None,
            supports_pagination=bool(supports_pagination) if supports_pagination is not None else None,
            supports_order_by=bool(supports_order_by) if supports_order_by is not None else None,
            supports_spatial_filter=bool(supports_spatial_filter) if supports_spatial_filter is not None else None,
            raw=payload,
        )
        self._layer_cache[layer_id] = info
        return info

    async def build_layer_catalog(self) -> dict[int, LayerInfo]:
        catalog: dict[int, LayerInfo] = {}
        for lid in await self.list_layer_ids():
            catalog[lid] = await self.get_layer_info(lid)
        return catalog

    async def query_page(
        self,
        *,
        layer_id: int,
        where: str = "1=1",
        out_fields: str = "*",
        return_geometry: bool = False,
        order_by: str | None = None,
        offset: int = 0,
        page_size: int = 200,
        geometry: dict[str, Any] | None = None,
        geometry_type: str | None = None,  # esriGeometryPolygon, esriGeometryPoint, ...
        spatial_rel: str = "esriSpatialRelIntersects",
        f: str = "json",
    ) -> QueryPage:
        info = await self.get_layer_info(layer_id)
        svc = await self.get_service_info()

        if order_by is None:
            order_by = f"{info.object_id_field} ASC"

        # respect maxRecordCount where known
        max_rc = info.max_record_count or svc.max_record_count
        if max_rc is not None:
            page_size = min(page_size, max_rc)

        url = f"{self.base}/{layer_id}/query"
        data: dict[str, Any] = {
            "f": f,
            "where": where,
            "outFields": out_fields,
            "returnGeometry": "true" if return_geometry else "false",
            "resultOffset": offset,
            "resultRecordCount": page_size,
        }

        # orderByFields only if supported (but usually harmless)
        if info.supports_order_by is not False:
            data["orderByFields"] = order_by

        if geometry is not None:
            if geometry_type is None:
                raise ValueError("geometry_type is required when geometry is provided")
            if info.supports_spatial_filter is False:
                # explicit no-op
                return QueryPage(layer_id=layer_id, offset=offset, page_size=page_size, features=[], raw={})
            data["geometry"] = to_esri_json_str(geometry)
            data["geometryType"] = geometry_type
            data["spatialRel"] = spatial_rel

            wkid = svc.spatial_reference_wkid
            if wkid is not None:
                data["inSR"] = str(wkid)
                data["outSR"] = str(wkid)

        payload = await self._http.request_json(url, method="POST", data=data)
        self._check_error(payload, url=url)

        raw_features = payload.get("features") or []
        features: list[ArcGISFeature] = []
        for feat in raw_features:
            if not isinstance(feat, dict):
                continue
            attrs = feat.get("attributes") if isinstance(feat.get("attributes"), dict) else {}
            geom = feat.get("geometry") if isinstance(feat.get("geometry"), dict) else None
            features.append(ArcGISFeature(attributes=dict(attrs), geometry=geom))

        exceeded = payload.get("exceededTransferLimit")
        next_offset = offset + page_size if len(features) == page_size else None

        return QueryPage(
            layer_id=layer_id,
            offset=offset,
            page_size=page_size,
            features=features,
            exceeded_transfer_limit=bool(exceeded) if exceeded is not None else None,
            next_offset=next_offset,
            raw=payload,
        )

    async def query_object_ids(
        self,
        *,
        layer_id: int,
        where: str = "1=1",
        geometry: dict[str, Any] | None = None,
        geometry_type: str | None = None,
        spatial_rel: str = "esriSpatialRelIntersects",
    ) -> list[int]:
        svc = await self.get_service_info()
        info = await self.get_layer_info(layer_id)

        url = f"{self.base}/{layer_id}/query"
        data: dict[str, Any] = {
            "f": "json",
            "where": where,
            "returnIdsOnly": "true",
            "returnGeometry": "false",
        }

        if geometry is not None:
            if geometry_type is None:
                raise ValueError("geometry_type is required when geometry is provided")
            if info.supports_spatial_filter is False:
                return []
            data["geometry"] = to_esri_json_str(geometry)
            data["geometryType"] = geometry_type
            data["spatialRel"] = spatial_rel
            wkid = svc.spatial_reference_wkid
            if wkid is not None:
                data["inSR"] = str(wkid)

        payload = await self._http.request_json(url, method="POST", data=data)
        self._check_error(payload, url=url)

        ids = payload.get("objectIds") or []
        if not isinstance(ids, list):
            return []
        out = [int(x) for x in ids if isinstance(x, int)]
        out.sort()
        return out

    async def query_by_object_ids(
        self,
        *,
        layer_id: int,
        object_ids: Iterable[int],
        out_fields: str = "*",
        return_geometry: bool = False,
        f: str = "json",
    ) -> list[ArcGISFeature]:
        url = f"{self.base}/{layer_id}/query"
        oid_str = ",".join(str(int(x)) for x in object_ids)

        data: dict[str, Any] = {
            "f": f,
            "objectIds": oid_str,
            "outFields": out_fields,
            "returnGeometry": "true" if return_geometry else "false",
        }

        payload = await self._http.request_json(url, method="POST", data=data)
        self._check_error(payload, url=url)

        raw_features = payload.get("features") or []
        features: list[ArcGISFeature] = []
        for feat in raw_features:
            if not isinstance(feat, dict):
                continue
            attrs = feat.get("attributes") if isinstance(feat.get("attributes"), dict) else {}
            geom = feat.get("geometry") if isinstance(feat.get("geometry"), dict) else None
            features.append(ArcGISFeature(attributes=dict(attrs), geometry=geom))
        return features

    async def iter_features(
        self,
        *,
        layer_id: int,
        where: str = "1=1",
        out_fields: str = "*",
        return_geometry: bool = False,
        order_by: str | None = None,
        page_size: int = 200,
        geometry: dict[str, Any] | None = None,
        geometry_type: str | None = None,
        spatial_rel: str = "esriSpatialRelIntersects",
        max_features: int | None = None,
    ) -> AsyncIterator[ArcGISFeature]:
        """
        Iterate *all* results robustly:
        - if supports pagination: walk offsets
        - else: use returnIdsOnly + chunked objectIds queries
        """
        info = await self.get_layer_info(layer_id)
        svc = await self.get_service_info()
        max_rc = info.max_record_count or svc.max_record_count
        if max_rc is not None:
            page_size = min(page_size, max_rc)
        page_size = max(1, page_size)

        # Prefer pagination when explicitly supported.
        if info.supports_pagination is not False:
            offset = 0
            emitted = 0
            while True:
                page = await self.query_page(
                    layer_id=layer_id,
                    where=where,
                    out_fields=out_fields,
                    return_geometry=return_geometry,
                    order_by=order_by,
                    offset=offset,
                    page_size=page_size,
                    geometry=geometry,
                    geometry_type=geometry_type,
                    spatial_rel=spatial_rel,
                )
                if not page.features:
                    break
                for feat in page.features:
                    yield feat
                    emitted += 1
                    if max_features is not None and emitted >= max_features:
                        return
                if page.next_offset is None:
                    break
                offset = page.next_offset
            return

        # Fallback: ids-only
        oids = await self.query_object_ids(
            layer_id=layer_id,
            where=where,
            geometry=geometry,
            geometry_type=geometry_type,
            spatial_rel=spatial_rel,
        )
        if not oids:
            return

        emitted = 0
        for i in range(0, len(oids), page_size):
            chunk = oids[i : i + page_size]
            feats = await self.query_by_object_ids(
                layer_id=layer_id,
                object_ids=chunk,
                out_fields=out_fields,
                return_geometry=return_geometry,
            )
            for feat in feats:
                yield feat
                emitted += 1
                if max_features is not None and emitted >= max_features:
                    return
