"""Parse 23andMe health report PDFs into structured findings.

Handles three PDF report types:
- Health risk reports (Alzheimer's, Parkinson's, T2D, etc.)
- Carrier status reports (Hemochromatosis, Thrombophilia, etc.)
- Wellness/trait reports (Muscle Composition, Genetic Weight)
- Haplogroup reports (Maternal, Paternal)

These are browser-printed PDFs — text extraction via pdfplumber,
not structured table parsing.
"""

from __future__ import annotations

import re
from pathlib import Path
from typing import Optional

import pdfplumber
from health_platform.utils.logging_config import get_logger

logger = get_logger("genetics_pdf_parser")

# Report categories based on 23andMe report types
_HEALTH_RISK_KEYWORDS = [
    "alzheimer",
    "parkinson",
    "macular degeneration",
    "type 2 diabetes",
]
_CARRIER_STATUS_KEYWORDS = [
    "alpha-1 antitrypsin",
    "hemochromatosis",
    "thrombophilia",
    "mutyh",
    "limb-girdle",
    "muscular dystrophy",
]
_WELLNESS_KEYWORDS = [
    "muscle composition",
    "genetic weight",
]
_HAPLOGROUP_KEYWORDS = [
    "maternal haplogroup",
    "paternal haplogroup",
]
_SKIP_KEYWORDS = [
    "neanderthal",
    "reports summary",
    "ancestry composition",
]


class GeneticsPdfParser:
    """Parse 23andMe PDF reports into structured health findings."""

    def __init__(self) -> None:
        self.last_detected_format: str = "unknown"

    def detect_format(self, text: str) -> str:
        """Detect the 23andMe report format from extracted text.

        Returns one of:
            '23andme_health_risk', '23andme_carrier_status',
            '23andme_wellness', '23andme_haplogroup', 'skip', 'unknown'
        """
        text_lower = text.lower()

        for kw in _SKIP_KEYWORDS:
            if kw in text_lower:
                return "skip"

        for kw in _HAPLOGROUP_KEYWORDS:
            if kw in text_lower:
                return "23andme_haplogroup"

        for kw in _HEALTH_RISK_KEYWORDS:
            if kw in text_lower:
                return "23andme_health_risk"

        for kw in _CARRIER_STATUS_KEYWORDS:
            if kw in text_lower:
                return "23andme_carrier_status"

        for kw in _WELLNESS_KEYWORDS:
            if kw in text_lower:
                return "23andme_wellness"

        if "23andme" in text_lower:
            return "23andme_health_risk"

        return "unknown"

    def parse_pdf(self, path: Path) -> list[dict]:
        """Parse a 23andMe PDF and return structured findings.

        Returns a list of dicts, each with:
            report_name, category, condition, gene, snp_id, genotype,
            risk_level, result_summary, variant_detected
        """
        try:
            with pdfplumber.open(path) as pdf:
                pages_text = []
                for page in pdf.pages:
                    text = page.extract_text()
                    if text:
                        pages_text.append(text)
                full_text = "\n".join(pages_text)
        except Exception as exc:
            logger.error("Failed to open PDF %s: %s", path.name, exc)
            return []

        if not full_text.strip():
            logger.warning("Empty PDF: %s", path)
            return []

        fmt = self.detect_format(full_text)
        self.last_detected_format = fmt

        if fmt == "skip":
            logger.info("Skipping non-health PDF: %s", path.name)
            return []

        if fmt == "unknown":
            logger.warning("Unknown PDF format: %s", path.name)
            return []

        if fmt == "23andme_haplogroup":
            return self._parse_haplogroup(full_text, path)

        return self._parse_health_report(full_text, path, fmt)

    def _parse_health_report(self, text: str, path: Path, fmt: str) -> list[dict]:
        """Parse a health risk, carrier status, or wellness report."""
        report_name = self._extract_report_name(path)
        category = self._format_to_category(fmt)
        condition = self._extract_condition(text, report_name)
        result_summary = self._extract_result_summary(text)
        variant_detected = self._detect_variant(text)

        # Extract marker data from Scientific Details section
        gene = self._extract_gene(text)
        snp_id = self._extract_snp_id(text)
        genotype = self._extract_genotype(text)
        risk_level = self._determine_risk_level(variant_detected, fmt)

        finding = {
            "report_name": report_name,
            "category": category,
            "condition": condition,
            "gene": gene,
            "snp_id": snp_id,
            "genotype": genotype,
            "risk_level": risk_level,
            "result_summary": result_summary,
            "variant_detected": variant_detected,
        }

        return [finding]

    def _parse_haplogroup(self, text: str, path: Path) -> list[dict]:
        """Parse a maternal or paternal haplogroup report."""
        is_maternal = "maternal" in path.name.lower()
        report_name = "Maternal Haplogroup" if is_maternal else "Paternal Haplogroup"

        haplogroup = self._extract_haplogroup(text)
        result_summary = (
            f"{'Maternal' if is_maternal else 'Paternal'} haplogroup: {haplogroup}"
            if haplogroup
            else "Haplogroup not determined"
        )

        return [
            {
                "report_name": report_name,
                "category": "haplogroup",
                "condition": report_name,
                "gene": "mtDNA" if is_maternal else "Y-DNA",
                "snp_id": None,
                "genotype": haplogroup,
                "risk_level": "informational",
                "result_summary": result_summary,
                "variant_detected": False,
            }
        ]

    # ------------------------------------------------------------------
    # Extraction helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _extract_report_name(path: Path) -> str:
        """Extract clean report name from filename."""
        name = path.stem
        # Remove " - 23andMe" suffix and "Report" suffix first
        name = re.sub(r"\s*-?\s*23andMe$", "", name)
        name = re.sub(r"\s+Report$", "", name)
        # Remove owner name prefix: everything before the last
        # occurrence of a known report keyword pattern.
        # 23andMe filenames follow: "<Owner Name> <Report Name> Report - 23andMe"
        # Strategy: strip leading words that look like person names
        # (capitalized words not part of medical/genetic terms)
        match = re.match(
            r"^((?:[A-Z][a-z\u00C0-\u017E]+\s+){2,3})" r"([A-Z].*)",
            name,
        )
        if match:
            name = match.group(2)
        return name.strip()

    @staticmethod
    def _extract_condition(text: str, report_name: str) -> str:
        """Extract the condition name from the report text."""
        # Try to find the main heading after the owner name line
        match = re.search(
            r"(?:[A-Z][a-z]+\s[A-Z].*?\n)(.+?)(?:\n[A-Z]|\nis characterized|\nis a)",
            text,
        )
        if match:
            condition = match.group(1).strip()
            if len(condition) < 100:
                return condition
        return report_name

    @staticmethod
    def _extract_result_summary(text: str) -> str:
        """Extract the main result statement."""
        # Pattern: "<Name>, you/your ..." — capture from "you" onward
        match = re.search(
            r"[A-Z][a-z]+,?\s+(you(?:r)?\s+.+?)(?:\.|Your risk|Studies|$)",
            text,
            re.IGNORECASE,
        )
        if match:
            summary = match.group(1).strip().rstrip(".")
            # Clean up whitespace
            summary = re.sub(r"\s+", " ", summary)
            if len(summary) < 300:
                return summary

        # Fallback: look for "X variants detected"
        match = re.search(r"(\d+)\s+variants?\s+detected", text, re.IGNORECASE)
        if match:
            count = int(match.group(1))
            return f"{count} variant(s) detected"

        return "Result not parsed"

    @staticmethod
    def _detect_variant(text: str) -> bool:
        """Determine if a variant was detected."""
        text_lower = text.lower()
        # Explicit "0 variants detected"
        if re.search(r"0\s+variants?\s+detected", text_lower):
            return False
        # "you do not have"
        if "you do not have" in text_lower:
            return False
        # "not detected"
        if "not detected" in text_lower and "variant" in text_lower:
            return False
        # Positive detection
        if re.search(r"[1-9]\d*\s+variants?\s+detected", text_lower):
            return True
        if "you have" in text_lower and "variant" in text_lower:
            # Check it's not "you have ... not"
            match = re.search(r"you have.*?(variant|copy)", text_lower)
            if (
                match
                and "not" not in text_lower[max(0, match.start() - 20) : match.start()]
            ):
                return True
        return False

    @staticmethod
    def _extract_gene(text: str) -> Optional[str]:
        """Extract gene name from marker table or text."""
        # Pattern: "in the GENE gene" or "Gene GENE"
        match = re.search(
            r"(?:in the|Gene)\s+([A-Z][A-Z0-9]{1,10})\s+gene",
            text,
        )
        if match:
            return match.group(1)

        # From marker table: look for gene column
        match = re.search(
            r"(?:Gene|gene)\s+(?:Marker ID|Marker)\s.*?\n\s*\S+\s+([A-Z][A-Z0-9]{1,10})\s+rs\d+",
            text,
        )
        if match:
            return match.group(1)

        return None

    @staticmethod
    def _extract_snp_id(text: str) -> Optional[str]:
        """Extract rsID (SNP identifier) from text."""
        match = re.search(r"(rs\d{3,})", text)
        return match.group(1) if match else None

    @staticmethod
    def _extract_genotype(text: str) -> Optional[str]:
        """Extract the user's genotype from the marker table."""
        # "Your Genotype" followed by letters
        match = re.search(
            r"Your Genotype\*?\s+.*?\n.*?([ACGT]{1,2})\s+(?:[ACGT])",
            text,
            re.DOTALL,
        )
        if match:
            return match.group(1)

        # Alternative: "You have one copy of the X variant and one copy of the Y variant"
        match = re.search(
            r"You have one copy of the ([A-Z]) variant and one copy of the ([A-Z]) variant",
            text,
        )
        if match:
            return match.group(1) + match.group(2)

        # Fallback: genotype near rsID
        match = re.search(r"rs\d+\s+([ACGT])\s+([ACGT])", text)
        if match:
            return match.group(1) + match.group(2)

        return None

    @staticmethod
    def _extract_haplogroup(text: str) -> Optional[str]:
        """Extract haplogroup designation from text."""
        match = re.search(
            r"your (?:maternal|paternal) haplogroup is\s+([A-Z][A-Za-z0-9]*)",
            text,
            re.IGNORECASE,
        )
        return match.group(1) if match else None

    @staticmethod
    def _format_to_category(fmt: str) -> str:
        """Map format string to category."""
        mapping = {
            "23andme_health_risk": "health_risk",
            "23andme_carrier_status": "carrier_status",
            "23andme_wellness": "wellness",
            "23andme_haplogroup": "haplogroup",
        }
        return mapping.get(fmt, "other")

    @staticmethod
    def _determine_risk_level(variant_detected: bool, fmt: str) -> str:
        """Determine risk level based on variant detection and report type."""
        if fmt == "23andme_wellness":
            return "informational"
        if variant_detected:
            if fmt == "23andme_carrier_status":
                return "medium"
            return "high"
        return "low"
