"""Clinician export API endpoints — FHIR R4 and PDF clinical reports.

Routes:
    GET /v1/export/fhir  — FHIR R4 Bundle JSON export
    GET /v1/export/pdf   — Clinical PDF report download
"""

from __future__ import annotations

from datetime import date, timedelta

from fastapi import APIRouter, Depends, Query
from fastapi.responses import Response, StreamingResponse

from health_platform.api.auth import verify_token
from health_platform.export.fhir.bundle import FhirBundleGenerator
from health_platform.export.pdf.clinical_report import (
    ALL_SECTIONS,
    ClinicalReportGenerator,
)
from health_platform.utils.logging_config import get_logger

logger = get_logger("export.routes")

router = APIRouter(prefix="/v1/export", tags=["export"])


@router.get("/fhir")
async def export_fhir(
    start_date: date = Query(
        ..., description="Start date (inclusive) in YYYY-MM-DD format"
    ),
    end_date: date = Query(
        ..., description="End date (inclusive) in YYYY-MM-DD format"
    ),
    resources: str = Query(
        "Patient,Observation",
        description="Comma-separated FHIR resource types to include",
    ),
    _token: str = Depends(verify_token),
):
    """Export health data as a FHIR R4 Bundle (JSON).

    Returns a FHIR R4 collection Bundle containing Patient and Observation
    resources for the specified date range. Suitable for import into any
    FHIR-compatible EHR system.
    """
    resource_list = [r.strip() for r in resources.split(",") if r.strip()]
    if not resource_list:
        resource_list = ["Patient", "Observation"]

    # Validate date range
    if end_date < start_date:
        return Response(
            content='{"error": "end_date must be >= start_date"}',
            status_code=400,
            media_type="application/json",
        )

    # Cap range to 1 year
    if (end_date - start_date).days > 365:
        end_date = start_date + timedelta(days=365)

    generator = FhirBundleGenerator()
    bundle = generator.generate_bundle(start_date, end_date, resource_list)

    return Response(
        content=_json_dumps(bundle),
        media_type="application/fhir+json",
    )


@router.get("/pdf")
async def export_pdf(
    start_date: date = Query(
        ..., description="Start date (inclusive) in YYYY-MM-DD format"
    ),
    end_date: date = Query(
        ..., description="End date (inclusive) in YYYY-MM-DD format"
    ),
    sections: str = Query(
        "",
        description=(
            "Comma-separated sections to include. "
            "Options: patient_info, vitals_summary, lab_results, "
            "trend_analysis, medication_list. Default: all."
        ),
    ),
    _token: str = Depends(verify_token),
):
    """Export a clinical PDF report for the specified date range.

    Generates a professional clinical report suitable for sharing with
    healthcare providers. Includes vitals, lab results, trends, and
    medication/supplement history.
    """
    # Parse sections
    section_list = (
        [s.strip() for s in sections.split(",") if s.strip()] if sections else None
    )

    # Validate sections
    if section_list:
        section_list = [s for s in section_list if s in ALL_SECTIONS]
        if not section_list:
            section_list = None  # Use all sections if none valid

    # Validate date range
    if end_date < start_date:
        return Response(
            content='{"error": "end_date must be >= start_date"}',
            status_code=400,
            media_type="application/json",
        )

    # Cap range to 1 year
    if (end_date - start_date).days > 365:
        end_date = start_date + timedelta(days=365)

    generator = ClinicalReportGenerator()
    pdf_bytes = generator.generate_report(start_date, end_date, section_list)

    filename = f"clinical_report_{start_date}_{end_date}.pdf"

    return StreamingResponse(
        iter([pdf_bytes]),
        media_type="application/pdf",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )


def _json_dumps(obj: dict) -> str:
    """Serialize dict to JSON string with date handling."""
    import json
    from datetime import date, datetime

    class DateEncoder(json.JSONEncoder):
        def default(self, o):
            if isinstance(o, (date, datetime)):
                return o.isoformat()
            return super().default(o)

    return json.dumps(obj, cls=DateEncoder, indent=2)
