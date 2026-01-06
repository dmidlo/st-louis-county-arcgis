from __future__ import annotations

import json
from typing import Any, Iterable


PARCEL_ID_FIELD_CANDIDATES = ("PRCL_NBR", "PARCEL", "PARCELID", "PARCEL_ID", "PRCL", "PID")
ADDRESS_FIELD_CANDIDATES = ("FULLADDR", "FULL_ADDRESS", "ADDRESS", "ADDR", "SITEADDR", "SITUSADDR")


def sql_quote(value: str) -> str:
    """Escape a string for ArcGIS SQL where clauses."""
    return value.replace("'", "''")


def pick_first_existing_field(fields: Iterable[str], candidates: Iterable[str]) -> str | None:
    """Return the first candidate that exists in fields (case-insensitive), preserving actual field casing."""
    field_map = {f.upper(): f for f in fields}
    for c in candidates:
        if c.upper() in field_map:
            return field_map[c.upper()]
    return None


def to_esri_json_str(obj: dict[str, Any]) -> str:
    """ArcGIS expects geometry param as a compact JSON string."""
    return json.dumps(obj, separators=(",", ":"))
