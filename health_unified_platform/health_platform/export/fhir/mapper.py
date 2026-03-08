"""FHIR R4 resource mapper for health platform gold layer data.

Maps internal health data structures to FHIR R4-compliant resources.
Uses LOINC codes for vital sign observations.

Reference: https://www.hl7.org/fhir/R4/
"""

from __future__ import annotations

import uuid
from datetime import date, datetime
from typing import Any, Optional

from health_platform.utils.logging_config import get_logger

logger = get_logger("export.fhir.mapper")

# LOINC codes for common vital signs
LOINC_CODES: dict[str, dict[str, str]] = {
    "heart_rate": {
        "code": "8867-4",
        "display": "Heart rate",
        "unit": "/min",
        "system": "http://loinc.org",
    },
    "resting_heart_rate": {
        "code": "8867-4",
        "display": "Heart rate",
        "unit": "/min",
        "system": "http://loinc.org",
    },
    "blood_pressure_systolic": {
        "code": "8480-6",
        "display": "Systolic blood pressure",
        "unit": "mmHg",
        "system": "http://loinc.org",
    },
    "blood_pressure_diastolic": {
        "code": "8462-4",
        "display": "Diastolic blood pressure",
        "unit": "mmHg",
        "system": "http://loinc.org",
    },
    "spo2": {
        "code": "2708-6",
        "display": "Oxygen saturation in Arterial blood",
        "unit": "%",
        "system": "http://loinc.org",
    },
    "body_temperature": {
        "code": "8310-5",
        "display": "Body temperature",
        "unit": "Cel",
        "system": "http://loinc.org",
    },
    "respiratory_rate": {
        "code": "9279-1",
        "display": "Respiratory rate",
        "unit": "/min",
        "system": "http://loinc.org",
    },
    "weight": {
        "code": "29463-7",
        "display": "Body weight",
        "unit": "kg",
        "system": "http://loinc.org",
    },
    "sleep_score": {
        "code": "93832-4",
        "display": "Sleep duration",
        "unit": "{score}",
        "system": "http://loinc.org",
    },
    "readiness_score": {
        "code": "LA30129-3",
        "display": "Readiness score",
        "unit": "{score}",
        "system": "http://loinc.org",
    },
    "activity_score": {
        "code": "LA30130-1",
        "display": "Activity score",
        "unit": "{score}",
        "system": "http://loinc.org",
    },
    "steps": {
        "code": "55423-8",
        "display": "Number of steps in unspecified time Pedometer",
        "unit": "{steps}",
        "system": "http://loinc.org",
    },
}

# FHIR vital sign category
_VITAL_SIGNS_CATEGORY = {
    "coding": [
        {
            "system": "http://terminology.hl7.org/CodeSystem/observation-category",
            "code": "vital-signs",
            "display": "Vital Signs",
        }
    ]
}

_SURVEY_CATEGORY = {
    "coding": [
        {
            "system": "http://terminology.hl7.org/CodeSystem/observation-category",
            "code": "survey",
            "display": "Survey",
        }
    ]
}

# Patient reference placeholder — replaced with actual patient ID at bundle time
_DEFAULT_PATIENT_REF = "Patient/1"


class FhirMapper:
    """Maps health platform data to FHIR R4 resources."""

    def __init__(self, patient_id: str = "1"):
        self.patient_id = patient_id
        self.patient_ref = f"Patient/{patient_id}"

    def map_patient(self, profile_data: dict[str, dict[str, str]]) -> dict[str, Any]:
        """Map agent.patient_profile data to a FHIR R4 Patient resource.

        Args:
            profile_data: Nested dict {category: {key: value}} from patient_profile.

        Returns:
            FHIR R4 Patient resource dict.
        """
        demographics = profile_data.get("demographics", {})
        patient: dict[str, Any] = {
            "resourceType": "Patient",
            "id": self.patient_id,
            "status": "active" if demographics else "unknown",
        }

        # Name
        name_parts: dict[str, str] = {}
        if "first_name" in demographics:
            name_parts["given"] = [demographics["first_name"]]
        if "last_name" in demographics:
            name_parts["family"] = demographics["last_name"]
        if "name" in demographics:
            name_parts["text"] = demographics["name"]
        if name_parts:
            patient["name"] = [name_parts]

        # Gender
        sex = demographics.get("biological_sex", "").lower()
        gender_map = {"male": "male", "female": "female", "m": "male", "f": "female"}
        if sex in gender_map:
            patient["gender"] = gender_map[sex]

        # Birth date
        if "date_of_birth" in demographics:
            patient["birthDate"] = demographics["date_of_birth"]
        elif "age" in demographics:
            try:
                age = int(demographics["age"])
                birth_year = date.today().year - age
                patient["birthDate"] = f"{birth_year}"
            except (ValueError, TypeError):
                pass

        return patient

    def map_observation(
        self,
        metric_name: str,
        value: float | int,
        observation_date: str | date | datetime,
        unit: Optional[str] = None,
        observation_id: Optional[str] = None,
    ) -> dict[str, Any]:
        """Map a single metric value to a FHIR R4 Observation resource.

        Args:
            metric_name: Name of the metric (must match LOINC_CODES keys).
            value: Numeric value of the observation.
            observation_date: Date/datetime of the observation.
            unit: Override unit (uses LOINC default if not provided).
            observation_id: Optional UUID; auto-generated if not provided.

        Returns:
            FHIR R4 Observation resource dict.
        """
        loinc = LOINC_CODES.get(metric_name)
        if not loinc:
            logger.warning("No LOINC code for metric: %s", metric_name)
            loinc = {
                "code": "unknown",
                "display": metric_name.replace("_", " ").title(),
                "unit": unit or "{unit}",
                "system": "http://loinc.org",
            }

        # Normalize date to ISO string
        if isinstance(observation_date, datetime):
            effective_dt = observation_date.isoformat()
        elif isinstance(observation_date, date):
            effective_dt = observation_date.isoformat()
        else:
            effective_dt = str(observation_date)

        obs_id = observation_id or str(uuid.uuid4())

        # Determine category based on metric
        is_vital = metric_name in {
            "heart_rate",
            "resting_heart_rate",
            "blood_pressure_systolic",
            "blood_pressure_diastolic",
            "spo2",
            "body_temperature",
            "respiratory_rate",
            "weight",
        }
        category = _VITAL_SIGNS_CATEGORY if is_vital else _SURVEY_CATEGORY

        observation: dict[str, Any] = {
            "resourceType": "Observation",
            "id": obs_id,
            "status": "final",
            "category": [category],
            "code": {
                "coding": [
                    {
                        "system": loinc["system"],
                        "code": loinc["code"],
                        "display": loinc["display"],
                    }
                ],
                "text": loinc["display"],
            },
            "subject": {"reference": self.patient_ref},
            "effectiveDateTime": effective_dt,
            "valueQuantity": {
                "value": value,
                "unit": unit or loinc["unit"],
                "system": "http://unitsofmeasure.org",
                "code": unit or loinc["unit"],
            },
        }

        return observation

    def map_vital_signs(self, vitals_row: dict[str, Any]) -> list[dict[str, Any]]:
        """Map a row from fct_daily_vitals_summary (or silver tables) to Observations.

        Expects a dict with keys like: day, sleep_score, readiness_score, steps,
        resting_heart_rate, spo2_avg_pct, weight_kg, etc.

        Args:
            vitals_row: Dict with vital sign values and a date field.

        Returns:
            List of FHIR R4 Observation resources.
        """
        obs_date = vitals_row.get("day") or vitals_row.get("date") or date.today()
        observations: list[dict[str, Any]] = []

        # Mapping from row keys to (metric_name, unit_override)
        field_map: dict[str, tuple[str, Optional[str]]] = {
            "resting_heart_rate": ("resting_heart_rate", "/min"),
            "heart_rate": ("heart_rate", "/min"),
            "bpm": ("heart_rate", "/min"),
            "spo2_avg_pct": ("spo2", "%"),
            "weight_kg": ("weight", "kg"),
            "body_temperature": ("body_temperature", "Cel"),
            "respiratory_rate": ("respiratory_rate", "/min"),
            "sleep_score": ("sleep_score", "{score}"),
            "readiness_score": ("readiness_score", "{score}"),
            "activity_score": ("activity_score", "{score}"),
            "steps": ("steps", "{steps}"),
        }

        for field, (metric_name, unit) in field_map.items():
            value = vitals_row.get(field)
            if value is not None:
                try:
                    numeric_val = float(value)
                    obs = self.map_observation(metric_name, numeric_val, obs_date, unit)
                    observations.append(obs)
                except (ValueError, TypeError):
                    logger.debug("Skipping non-numeric value for %s: %s", field, value)

        return observations

    def map_health_score(self, score_row: dict[str, Any]) -> dict[str, Any]:
        """Map a health score row to a FHIR R4 Observation.

        Args:
            score_row: Dict with keys: day, readiness_score (or health_score).

        Returns:
            FHIR R4 Observation resource dict.
        """
        obs_date = score_row.get("day") or score_row.get("date") or date.today()
        score = score_row.get("readiness_score") or score_row.get("health_score", 0)

        return self.map_observation(
            metric_name="readiness_score",
            value=float(score),
            observation_date=obs_date,
            unit="{score}",
        )

    def map_lab_result(self, lab_row: dict[str, Any]) -> dict[str, Any]:
        """Map a lab biomarker row to a FHIR R4 Observation.

        Args:
            lab_row: Dict with keys: test_name, value, unit, date (or day),
                     optionally reference_range_low, reference_range_high.

        Returns:
            FHIR R4 Observation resource dict.
        """
        obs_date = lab_row.get("day") or lab_row.get("date") or date.today()
        test_name = lab_row.get("test_name", "Unknown Test")
        value = lab_row.get("value", 0)
        unit = lab_row.get("unit", "")
        obs_id = str(uuid.uuid4())

        observation: dict[str, Any] = {
            "resourceType": "Observation",
            "id": obs_id,
            "status": "final",
            "category": [
                {
                    "coding": [
                        {
                            "system": "http://terminology.hl7.org/CodeSystem/observation-category",
                            "code": "laboratory",
                            "display": "Laboratory",
                        }
                    ]
                }
            ],
            "code": {
                "coding": [
                    {
                        "system": "http://loinc.org",
                        "display": test_name,
                    }
                ],
                "text": test_name,
            },
            "subject": {"reference": self.patient_ref},
            "effectiveDateTime": (
                obs_date.isoformat()
                if isinstance(obs_date, (date, datetime))
                else str(obs_date)
            ),
            "valueQuantity": {
                "value": float(value),
                "unit": unit,
                "system": "http://unitsofmeasure.org",
                "code": unit,
            },
        }

        # Add reference range if available
        ref_low = lab_row.get("reference_range_low")
        ref_high = lab_row.get("reference_range_high")
        if ref_low is not None or ref_high is not None:
            ref_range: dict[str, Any] = {}
            if ref_low is not None:
                ref_range["low"] = {"value": float(ref_low), "unit": unit}
            if ref_high is not None:
                ref_range["high"] = {"value": float(ref_high), "unit": unit}
            observation["referenceRange"] = [ref_range]

        return observation
