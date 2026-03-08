"""Data quality check result models."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Optional


@dataclass
class CheckResult:
    """Result of a single data quality check."""

    table_name: str
    check_type: str
    column: Optional[str]
    passed: bool
    message: str
    value: Optional[float] = None
    threshold: Optional[float] = None
    checked_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))


@dataclass
class QualityReport:
    """Aggregate report from a full quality check run."""

    results: list[CheckResult] = field(default_factory=list)
    started_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    finished_at: Optional[datetime] = None

    @property
    def total(self) -> int:
        return len(self.results)

    @property
    def passed(self) -> int:
        return sum(1 for r in self.results if r.passed)

    @property
    def failed(self) -> int:
        return sum(1 for r in self.results if not r.passed)

    @property
    def pass_rate(self) -> float:
        if self.total == 0:
            return 0.0
        return round(self.passed / self.total * 100, 1)

    def finish(self) -> None:
        self.finished_at = datetime.now(timezone.utc)
