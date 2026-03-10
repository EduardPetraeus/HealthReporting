"""Trend forecasting for health metrics.

Uses linear regression and exponential smoothing to predict
short-term metric trajectories (7-14 day forecasts).
"""

from __future__ import annotations

import sys
from dataclasses import dataclass
from datetime import date, timedelta
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from health_platform.utils.logging_config import get_logger

logger = get_logger("trend_forecaster")


@dataclass
class Forecast:
    """A metric forecast result."""

    metric: str
    current_value: float
    forecast_values: list[tuple[date, float]]  # (date, predicted_value)
    trend_direction: str  # "improving", "declining", "stable"
    confidence: float  # 0.0-1.0
    method: str  # "linear_regression" or "exponential_smoothing"
    r_squared: float  # Goodness of fit
    description: str


_HIGHER_IS_WORSE: set[str] = {
    "daily_stress",
    "stress_high",
    "resting_heart_rate",
    "body_fat_percentage",
}


class TrendForecaster:
    """Forecast health metric trajectories."""

    def __init__(self, con) -> None:
        self.con = con

    def forecast_metric(
        self,
        table: str,
        column: str,
        date_column: str = "day",
        lookback_days: int = 90,
        forecast_days: int = 7,
    ) -> Forecast:
        """Forecast a metric using linear regression.

        1. Query last lookback_days of data
        2. Fit linear regression (using DuckDB's REGR_* functions)
        3. Project forward forecast_days
        4. Classify trend direction
        """
        end_date = date.today()
        start_date = end_date - timedelta(days=lookback_days)

        # Get data points
        sql = f"""
            SELECT {date_column} AS day, AVG({column}) AS value
            FROM {table}
            WHERE {date_column} BETWEEN ? AND ?
              AND {column} IS NOT NULL
            GROUP BY {date_column}
            ORDER BY {date_column}
        """

        try:
            result = self.con.execute(sql, [start_date, end_date])
            rows = result.fetchall()
        except Exception as exc:
            logger.error("Failed to fetch data for forecasting: %s", exc)
            return self._empty_forecast(f"{table}.{column}")

        if len(rows) < 7:
            logger.warning("Insufficient data for forecast: %d points", len(rows))
            return self._empty_forecast(f"{table}.{column}")

        # Compute linear regression using DuckDB
        reg_sql = f"""
            WITH numbered AS (
                SELECT
                    ROW_NUMBER() OVER (ORDER BY {date_column}) AS x,
                    {column} AS y
                FROM {table}
                WHERE {date_column} BETWEEN ? AND ?
                  AND {column} IS NOT NULL
            )
            SELECT
                REGR_SLOPE(y, x) AS slope,
                REGR_INTERCEPT(y, x) AS intercept,
                REGR_R2(y, x) AS r_squared,
                COUNT(*) AS n,
                MAX(x) AS max_x
            FROM numbered
        """

        try:
            reg_row = self.con.execute(reg_sql, [start_date, end_date]).fetchone()
        except Exception as exc:
            logger.error("Regression failed: %s", exc)
            return self._empty_forecast(f"{table}.{column}")

        if reg_row is None or reg_row[0] is None:
            return self._empty_forecast(f"{table}.{column}")

        slope, intercept, r_squared, n, max_x = reg_row

        # Current value (most recent)
        current_value = rows[-1][1]

        # Project forward
        forecast_values = []
        for i in range(1, forecast_days + 1):
            future_x = max_x + i
            predicted = intercept + slope * future_x
            forecast_date = end_date + timedelta(days=i)
            forecast_values.append((forecast_date, round(predicted, 2)))

        # Classify trend — invert for metrics where higher is worse
        daily_change = slope
        metric_name = f"{table}.{column}"
        invert = table in _HIGHER_IS_WORSE or column in _HIGHER_IS_WORSE
        if abs(daily_change) < 0.5:
            direction = "stable"
        elif daily_change > 0:
            direction = "declining" if invert else "improving"
        else:
            direction = "improving" if invert else "declining"

        confidence = min(r_squared, 0.95) if r_squared and r_squared > 0 else 0.0

        desc_parts = [
            f"{metric_name}: {direction} trend",
            f"(slope: {slope:+.2f}/day, R\u00b2: {r_squared:.3f})",
            f"Current: {current_value:.1f}",
            f"7-day forecast: {forecast_values[-1][1] if forecast_values else 'N/A'}",
        ]

        return Forecast(
            metric=metric_name,
            current_value=current_value,
            forecast_values=forecast_values,
            trend_direction=direction,
            confidence=confidence,
            method="linear_regression",
            r_squared=r_squared or 0.0,
            description=" | ".join(desc_parts),
        )

    def _empty_forecast(self, metric: str) -> Forecast:
        return Forecast(
            metric=metric,
            current_value=0.0,
            forecast_values=[],
            trend_direction="unknown",
            confidence=0.0,
            method="insufficient_data",
            r_squared=0.0,
            description=f"{metric}: insufficient data for forecast",
        )


def format_forecast(forecast: Forecast) -> str:
    """Format a forecast as markdown."""
    parts = [f"## Forecast: {forecast.metric}\n"]
    parts.append(f"- **Direction:** {forecast.trend_direction}")
    parts.append(f"- **Current:** {forecast.current_value:.1f}")
    parts.append(
        f"- **Confidence:** {forecast.confidence:.0%} (R\u00b2 = {forecast.r_squared:.3f})"
    )
    parts.append(f"- **Method:** {forecast.method}\n")

    if forecast.forecast_values:
        parts.append("| Date | Predicted |")
        parts.append("|------|-----------|")
        for d, v in forecast.forecast_values:
            parts.append(f"| {d} | {v:.1f} |")

    return "\n".join(parts)
