from __future__ import annotations


class ArcGISError(RuntimeError):
    """ArcGIS REST returned an error payload."""

    def __init__(self, message: str, *, details: dict | None = None) -> None:
        super().__init__(message)
        self.details = details or {}


class TransportError(RuntimeError):
    """Network/HTTP failures after retries."""
