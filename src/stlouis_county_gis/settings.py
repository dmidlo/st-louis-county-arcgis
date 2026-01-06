from __future__ import annotations

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class ArcGISClientSettings(BaseSettings):
    """
    Configuration for ArcGIS REST access.

    You can override via env vars, e.g.:
      STLCO_GIS_BASE_URL=...
    """

    model_config = SettingsConfigDict(env_prefix="STLCO_GIS_", extra="ignore")

    base_url: str = Field(
        default="https://gis.stlouiscountymn.gov/server2/rest/services/GeneralUse/Open_Data/MapServer",
        description="Base MapServer URL",
    )

    user_agent: str = Field(
        default="stlouis-county-gis/0.1.0",
        description="User-Agent header",
    )

    timeout_s: float = Field(default=30.0, description="Request timeout seconds")

    # retry/backoff
    max_retries: int = Field(default=6, description="Max retries for transient errors")
    backoff_base_s: float = Field(default=0.6, description="Base backoff seconds")
    backoff_max_s: float = Field(default=10.0, description="Max backoff seconds")

    # query defaults
    default_page_size: int = Field(default=200, description="Default page size (capped by service limits)")
    max_page_size_cap: int = Field(default=2000, description="Hard cap to avoid huge payloads")
