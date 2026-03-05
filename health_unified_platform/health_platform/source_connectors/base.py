"""
Abstract base class for health data source connectors.

All source connectors (Oura, Apple Health, Withings, etc.) should inherit
from BaseConnector to ensure a consistent interface for the pipeline runner.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from pathlib import Path
from typing import Optional


class BaseConnector(ABC):
    """Abstract base class for health data source connectors.

    Subclasses must implement:
        - source_name: unique identifier for the data source
        - authenticate: obtain or refresh API credentials
        - fetch: retrieve records from a specific endpoint
        - get_endpoints: list available data endpoints
    """

    @property
    @abstractmethod
    def source_name(self) -> str:
        """Unique identifier for this data source (e.g. 'oura', 'withings')."""
        ...

    @abstractmethod
    def authenticate(self) -> None:
        """Obtain or refresh credentials needed for API access.

        Should be idempotent — calling it when already authenticated is a no-op.
        """
        ...

    @abstractmethod
    def fetch(self, endpoint: str, start_date: str, end_date: str) -> list[dict]:
        """Fetch records from a specific endpoint for the given date range.

        Args:
            endpoint: Name of the data endpoint (e.g. 'daily_sleep').
            start_date: ISO 8601 date string (YYYY-MM-DD).
            end_date: ISO 8601 date string (YYYY-MM-DD).

        Returns:
            List of record dicts as returned by the source API.
        """
        ...

    @abstractmethod
    def get_endpoints(self) -> list[str]:
        """Return a list of available endpoint names for this source."""
        ...

    def run(
        self,
        data_lake_root: Optional[Path] = None,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
    ) -> dict[str, int]:
        """Standard pipeline: authenticate -> fetch all endpoints -> return record counts.

        This default implementation iterates over get_endpoints() and calls fetch()
        for each one. Subclasses can override for source-specific orchestration
        (e.g. state tracking, partitioned writes).

        Args:
            data_lake_root: Optional root path for data output.
            start_date: ISO 8601 date string. If None, connector decides default.
            end_date: ISO 8601 date string. If None, connector decides default.

        Returns:
            Dict mapping endpoint name to number of records fetched.
        """
        self.authenticate()
        results: dict[str, int] = {}
        for endpoint in self.get_endpoints():
            records = self.fetch(
                endpoint,
                start_date or "",
                end_date or "",
            )
            results[endpoint] = len(records)
        return results
