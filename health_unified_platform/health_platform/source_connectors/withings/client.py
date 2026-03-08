"""
Withings API client.
Wraps each endpoint in a typed method with automatic date handling.

API reference: https://developer.withings.com/api-reference
"""

from __future__ import annotations

from datetime import date, datetime

import requests

BASE_URL = "https://wbsapi.withings.net"

# Withings measure type IDs
MEASURE_TYPES = {
    "weight_kg": 1,
    "height_m": 4,
    "fat_free_mass_kg": 5,
    "fat_ratio_pct": 6,
    "fat_mass_kg": 8,
    "diastolic_mmhg": 9,
    "systolic_mmhg": 10,
    "pulse_bpm": 11,
    "temperature_c": 12,
    "spo2_pct": 54,
    "bone_mass_kg": 88,
    "muscle_mass_kg": 76,
    "hydration_kg": 77,
}

# Group measure types by category
BODY_MEASURE_TYPES = [1, 5, 6, 8, 76, 77, 88]  # weight, fat, muscle, bone, hydration
BLOOD_PRESSURE_TYPES = [9, 10, 11]  # diastolic, systolic, pulse
TEMPERATURE_TYPES = [12]


class WithingsClient:
    def __init__(self, access_token: str) -> None:
        self.session = requests.Session()
        self.access_token = access_token

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _post(self, path: str, params: dict) -> dict:
        """Sends a POST request with the access token in the body.

        Withings API uses POST for all data endpoints and returns
        data in a nested 'body' key.
        """
        params["access_token"] = self.access_token
        response = self.session.post(f"{BASE_URL}{path}", data=params)
        response.raise_for_status()
        result = response.json()
        status = result.get("status", -1)
        if status != 0:
            raise RuntimeError(
                f"Withings API error: status={status}, "
                f"error={result.get('error', 'unknown')}"
            )
        return result.get("body", {})

    def _date_to_epoch(self, d: date) -> int:
        """Convert a date to Unix epoch timestamp."""
        return int(datetime.combine(d, datetime.min.time()).timestamp())

    # ------------------------------------------------------------------
    # Endpoints — original (Measure v1)
    # ------------------------------------------------------------------

    def fetch_measure(
        self,
        start: date,
        end: date,
        measure_types: list[int] | None = None,
    ) -> list[dict]:
        """Fetch body measurements (weight, body composition, etc.).

        Returns flattened records with one row per measurement group,
        each containing all measure types present in that group.
        """
        params = {
            "action": "getmeas",
            "startdate": self._date_to_epoch(start),
            "enddate": self._date_to_epoch(end) + 86400,  # include end date
            "category": 1,  # real measurements only (not user objectives)
        }
        if measure_types:
            params["meastypes"] = ",".join(str(t) for t in measure_types)

        body = self._post("/measure", params)
        measure_groups = body.get("measuregrps", [])

        # Build a reverse lookup: type_id -> field name
        type_to_name = {v: k for k, v in MEASURE_TYPES.items()}

        records = []
        for grp in measure_groups:
            record = {
                "grpid": grp.get("grpid"),
                "datetime": datetime.fromtimestamp(grp.get("date", 0)).isoformat(),
                "category": grp.get("category"),
            }
            for measure in grp.get("measures", []):
                type_id = measure.get("type")
                field_name = type_to_name.get(type_id, f"type_{type_id}")
                # Withings stores values as value * 10^unit
                value = measure.get("value", 0) * (10 ** measure.get("unit", 0))
                record[field_name] = round(value, 4)
            records.append(record)
        return records

    def fetch_weight(self, start: date, end: date) -> list[dict]:
        """Fetch weight and body composition measurements."""
        return self.fetch_measure(start, end, measure_types=BODY_MEASURE_TYPES)

    def fetch_blood_pressure(self, start: date, end: date) -> list[dict]:
        """Fetch blood pressure readings (systolic, diastolic, pulse)."""
        return self.fetch_measure(start, end, measure_types=BLOOD_PRESSURE_TYPES)

    def fetch_temperature(self, start: date, end: date) -> list[dict]:
        """Fetch body temperature measurements."""
        return self.fetch_measure(start, end, measure_types=TEMPERATURE_TYPES)

    # ------------------------------------------------------------------
    # Endpoints — DS1 expansion (Sleep v2, Heart v2)
    # ------------------------------------------------------------------

    def fetch_sleep_summary(self, start: date, end: date) -> list[dict]:
        """Fetch sleep summary data (duration, phases, scores).

        Uses Sleep v2 API with action=getsummary.
        """
        body = self._post(
            "/v2/sleep",
            {
                "action": "getsummary",
                "startdate": self._date_to_epoch(start),
                "enddate": self._date_to_epoch(end) + 86400,
            },
        )
        series = body.get("series", [])
        records = []
        for entry in series:
            record = {
                "startdate": entry.get("startdate"),
                "enddate": entry.get("enddate"),
                "date": entry.get("date"),
                "model": entry.get("model"),
                "model_id": entry.get("model_id"),
            }
            for key, value in entry.get("data", {}).items():
                record[key] = value
            records.append(record)
        return records

    def fetch_sleep_raw(self, start: date, end: date) -> list[dict]:
        """Fetch raw sleep sensor data (HR, RR, snoring, SDNN, RMSSD).

        Uses Sleep v2 API with action=get.
        Returns one record per sleep session with time-series arrays.
        """
        body = self._post(
            "/v2/sleep",
            {
                "action": "get",
                "startdate": self._date_to_epoch(start),
                "enddate": self._date_to_epoch(end) + 86400,
            },
        )
        series = body.get("series", [])
        records = []
        for entry in series:
            record = {
                "startdate": entry.get("startdate"),
                "enddate": entry.get("enddate"),
                "model": entry.get("model"),
                "model_id": entry.get("model_id"),
                "hr": entry.get("hr"),
                "rr": entry.get("rr"),
                "snoring": entry.get("snoring"),
                "sdnn_1": entry.get("sdnn_1"),
                "rmssd": entry.get("rmssd"),
            }
            records.append(record)
        return records

    def fetch_heart_list(self, start: date, end: date) -> list[dict]:
        """Fetch ECG recording metadata (signal IDs, classification, HR).

        Uses Heart v2 API with action=list.
        """
        body = self._post(
            "/v2/heart",
            {
                "action": "list",
                "startdate": self._date_to_epoch(start),
                "enddate": self._date_to_epoch(end) + 86400,
            },
        )
        series = body.get("series", [])
        records = []
        for entry in series:
            record = {
                "signalid": entry.get("signalid"),
                "ecg": entry.get("ecg", {}),
                "bloodpressure": entry.get("bloodpressure", {}),
                "heart_rate": entry.get("heart_rate"),
                "model": entry.get("model"),
                "deviceid": entry.get("deviceid"),
                "timestamp": entry.get("timestamp"),
            }
            records.append(record)
        return records

    def fetch_heart_signal(self, signal_id: int) -> dict:
        """Fetch a single ECG waveform by signal ID.

        Uses Heart v2 API with action=get.
        Returns the raw signal data including waveform array.
        """
        body = self._post(
            "/v2/heart",
            {
                "action": "get",
                "signalid": signal_id,
            },
        )
        return body

    # ------------------------------------------------------------------
    # Interface
    # ------------------------------------------------------------------

    def get_endpoints(self) -> list[str]:
        """Return available endpoint names for this source."""
        return [
            "weight",
            "blood_pressure",
            "temperature",
            "sleep_summary",
            "sleep_raw",
            "heart_list",
        ]
