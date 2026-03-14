"""Parse 23andMe health report PDFs into structured findings.

Handles PDF report types:
- Health risk reports (Alzheimer's, Parkinson's, T2D, etc.)
- Carrier status reports (Hemochromatosis, Thrombophilia, etc.)
- Wellness/trait reports (Muscle Composition, Genetic Weight)
- Haplogroup reports (Maternal, Paternal)
- Ancestry composition reports (population percentages)
- Neanderthal ancestry reports (variant count, percentile)

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
_ANCESTRY_KEYWORDS = [
    "ancestry composition",
    "ancestry report",
]
_NEANDERTHAL_KEYWORDS = [
    "neanderthal",
]
_SKIP_KEYWORDS = [
    "reports summary",
]


class GeneticsPdfParser:
    """Parse 23andMe PDF reports into structured health findings."""

    def __init__(self) -> None:
        self.last_detected_format: str = "unknown"

    def detect_format(self, text: str) -> str:
        """Detect the 23andMe report format from extracted text.

        Returns one of:
            '23andme_health_risk', '23andme_carrier_status',
            '23andme_wellness', '23andme_haplogroup',
            '23andme_ancestry', '23andme_neanderthal',
            'skip', 'unknown'
        """
        text_lower = text.lower()

        # Check ancestry and neanderthal BEFORE skip keywords
        for kw in _ANCESTRY_KEYWORDS:
            if kw in text_lower:
                return "23andme_ancestry"

        for kw in _NEANDERTHAL_KEYWORDS:
            if kw in text_lower:
                return "23andme_neanderthal"

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

        if fmt == "23andme_ancestry":
            return self._parse_ancestry(full_text, path)

        if fmt == "23andme_neanderthal":
            return self._parse_neanderthal(full_text, path)

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

    def _parse_ancestry(self, text: str, path: Path) -> list[dict]:
        """Parse an ancestry composition report.

        Extracts population-level ancestry percentages from lines like
        "Scandinavian 84.4%" or tabular percentage layouts.

        Returns list of dicts with: population, region, percentage,
        confidence_level, report_name, category.
        """
        report_name = self._extract_report_name(path)
        rows: list[dict] = []

        # Region hierarchy mapping — known 23andMe regions
        _region_map: dict[str, str] = {
            "scandinavian": "Northwestern European",
            "finnish": "Northwestern European",
            "british & irish": "Northwestern European",
            "french & german": "Northwestern European",
            "northwestern european": "European",
            "southern european": "European",
            "italian": "Southern European",
            "spanish & portuguese": "Southern European",
            "greek & balkan": "Southern European",
            "sardinian": "Southern European",
            "eastern european": "European",
            "ashkenazi jewish": "European",
            "european": "Broadly",
            "sub-saharan african": "African",
            "west african": "African",
            "east african": "African",
            "north african & arabian": "Western Asian & North African",
            "western asian & north african": "Broadly",
            "east asian & native american": "Broadly",
            "east asian": "East Asian & Native American",
            "native american": "East Asian & Native American",
            "south asian": "Broadly",
            "melanesian": "Broadly",
            "unassigned": "Unassigned",
        }

        # Pattern: "Population Name  XX.X%" — flexible whitespace and optional %
        pattern = re.compile(
            r"^(.+?)\s{2,}(\d{1,3}(?:\.\d{1,2})?)\s*%",
            re.MULTILINE,
        )

        for match in pattern.finditer(text):
            population = match.group(1).strip()
            percentage = float(match.group(2))

            # Skip noise lines (headers, footers, page artifacts)
            if len(population) > 60 or percentage > 100:
                continue
            # Skip lines that are clearly not population names
            if any(
                skip in population.lower()
                for skip in ["page", "23andme", "learn more", "confidence"]
            ):
                continue

            pop_lower = population.lower()
            region = _region_map.get(pop_lower, "Other")

            rows.append(
                {
                    "report_name": report_name,
                    "category": "ancestry",
                    "population": population,
                    "region": region,
                    "percentage": percentage,
                    "confidence_level": self._extract_confidence_level(text),
                }
            )

        if not rows:
            logger.warning("No ancestry populations parsed from %s", path.name)

        return rows

    def _parse_neanderthal(self, text: str, path: Path) -> list[dict]:
        """Parse a neanderthal ancestry report.

        Extracts variant count and percentile ranking.

        Returns a single-item list with: category, trait_name,
        result_value, result_numeric, gene, snp_id, genotype,
        report_name.
        """
        report_name = self._extract_report_name(path)

        # Extract variant count: "You have X Neanderthal variants"
        variant_count: Optional[float] = None
        match = re.search(
            r"(\d+)\s+(?:neanderthal\s+)?variants?",
            text,
            re.IGNORECASE,
        )
        if match:
            variant_count = float(match.group(1))

        # Extract percentile: "more variants than X% of customers"
        # or "Xth percentile"
        percentile: Optional[float] = None
        match = re.search(
            r"(?:more\s+.*?than\s+|top\s+)(\d{1,3})(?:\.\d+)?\s*%",
            text,
            re.IGNORECASE,
        )
        if match:
            percentile = float(match.group(1))

        if percentile is None:
            match = re.search(
                r"(\d{1,3})(?:st|nd|rd|th)\s+percentile", text, re.IGNORECASE
            )
            if match:
                percentile = float(match.group(1))

        # Build description
        parts = []
        if variant_count is not None:
            parts.append(f"{int(variant_count)} Neanderthal variants")
        if percentile is not None:
            parts.append(f"more than {percentile}% of customers")
        result_value = "; ".join(parts) if parts else "Neanderthal ancestry data found"

        # Use variant count as primary numeric, percentile as fallback
        result_numeric = variant_count if variant_count is not None else percentile

        return [
            {
                "report_name": report_name,
                "category": "neanderthal",
                "trait_name": "neanderthal_ancestry",
                "result_value": result_value,
                "result_numeric": result_numeric,
                "gene": None,
                "snp_id": None,
                "genotype": None,
            }
        ]

    def _parse_wellness_traits(self, text: str, path: Path) -> list[dict]:
        """Parse detailed wellness/trait info from a report.

        Extracts structured trait data including gene, SNP, and genotype
        for traits like muscle composition, genetic weight, saturated fat,
        lactose intolerance, etc.

        Returns list of dicts with: category, trait_name, result_value,
        result_numeric, gene, snp_id, genotype, report_name.
        """
        report_name = self._extract_report_name(path)
        condition = self._extract_condition(text, report_name)
        result_summary = self._extract_result_summary(text)
        gene = self._extract_gene(text)
        snp_id = self._extract_snp_id(text)
        genotype = self._extract_genotype(text)

        # Try to extract a numeric value from the result summary
        result_numeric: Optional[float] = None
        match = re.search(r"(\d+(?:\.\d+)?)\s*%", result_summary)
        if match:
            result_numeric = float(match.group(1))

        return [
            {
                "report_name": report_name,
                "category": "wellness",
                "trait_name": condition,
                "result_value": result_summary,
                "result_numeric": result_numeric,
                "gene": gene,
                "snp_id": snp_id,
                "genotype": genotype,
            }
        ]

    @staticmethod
    def _extract_confidence_level(text: str) -> str:
        """Extract the confidence level from ancestry report text.

        23andMe uses 50%, 70%, 80%, 90% confidence thresholds.
        """
        # Look for "at X% confidence" or "confidence level: X%"
        match = re.search(
            r"(?:at\s+|confidence\s*(?:level)?[:\s]+)(\d{2,3})\s*%",
            text,
            re.IGNORECASE,
        )
        if match:
            return f"{match.group(1)}%"
        return "standard"

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
            "23andme_ancestry": "ancestry",
            "23andme_neanderthal": "neanderthal",
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
