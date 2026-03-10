"""PubMed E-utilities API client.

Searches and fetches article metadata from NCBI PubMed.
Uses esearch + efetch endpoints with XML parsing.
No new dependencies — uses requests (already in deps) + xml.etree (stdlib).

Rate limit: 3 requests/second without API key, 10/second with key.
"""

from __future__ import annotations

import time
import xml.etree.ElementTree as ET

import requests
from health_platform.utils.keychain import get_secret
from health_platform.utils.logging_config import get_logger

logger = get_logger("knowledge.pubmed")

ESEARCH_URL = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esearch.fcgi"
EFETCH_URL = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/efetch.fcgi"

# GRADE-inspired evidence hierarchy: publication_type -> (level, score)
EVIDENCE_HIERARCHY: dict[str, tuple[str, float]] = {
    "Meta-Analysis": ("meta_analysis", 1.0),
    "Systematic Review": ("systematic_review", 0.95),
    "Practice Guideline": ("guideline", 0.90),
    "Guideline": ("guideline", 0.90),
    "Randomized Controlled Trial": ("rct", 0.85),
    "Clinical Trial": ("clinical_trial", 0.75),
    "Clinical Trial, Phase IV": ("clinical_trial", 0.75),
    "Clinical Trial, Phase III": ("clinical_trial", 0.75),
    "Observational Study": ("observational", 0.65),
    "Cohort Study": ("cohort", 0.65),
    "Case-Control Study": ("case_control", 0.55),
    "Review": ("review", 0.50),
    "Case Reports": ("case_report", 0.40),
}

DEFAULT_EVIDENCE = ("other", 0.30)


class PubMedClient:
    """Fetches article metadata from the PubMed E-utilities API."""

    def __init__(self, api_key: str | None = None) -> None:
        self.api_key = api_key or get_secret("NCBI_API_KEY")
        self.session = requests.Session()
        self._last_request_time: float = 0.0
        self._min_interval = 0.10 if self.api_key else 0.34

    def search(
        self, query: str, max_results: int = 10, min_year: str = ""
    ) -> list[str]:
        """Search PubMed and return a list of PMIDs."""
        self._rate_limit()

        params: dict[str, str | int] = {
            "db": "pubmed",
            "term": query,
            "retmax": max_results,
            "retmode": "json",
            "sort": "relevance",
        }
        if min_year:
            params["mindate"] = f"{min_year}/01/01"
            params["maxdate"] = "3000/12/31"
            params["datetype"] = "pdat"
        if self.api_key:
            params["api_key"] = self.api_key

        response = self.session.get(ESEARCH_URL, params=params, timeout=15)
        response.raise_for_status()
        data = response.json()

        return data.get("esearchresult", {}).get("idlist", [])

    def fetch_articles(self, pmids: list[str]) -> list[dict]:
        """Fetch article metadata for a list of PMIDs."""
        if not pmids:
            return []

        self._rate_limit()

        params: dict[str, str] = {
            "db": "pubmed",
            "id": ",".join(pmids),
            "retmode": "xml",
            "rettype": "abstract",
        }
        if self.api_key:
            params["api_key"] = self.api_key

        response = self.session.get(EFETCH_URL, params=params, timeout=30)
        response.raise_for_status()

        root = ET.fromstring(response.text)
        articles = []
        for article_elem in root.findall(".//PubmedArticle"):
            parsed = self._parse_article_xml(article_elem)
            if parsed:
                articles.append(parsed)

        return articles

    @staticmethod
    def _parse_article_xml(element: ET.Element) -> dict | None:
        """Parse a single PubmedArticle XML element into a dict."""
        medline = element.find("MedlineCitation")
        if medline is None:
            return None

        pmid_elem = medline.find("PMID")
        pmid = pmid_elem.text if pmid_elem is not None else ""
        if not pmid:
            return None

        article = medline.find("Article")
        if article is None:
            return None

        title_elem = article.find("ArticleTitle")
        title = title_elem.text or "" if title_elem is not None else ""

        abstract_parts = []
        abstract_elem = article.find("Abstract")
        if abstract_elem is not None:
            for text_elem in abstract_elem.findall("AbstractText"):
                label = text_elem.get("Label", "")
                text = "".join(text_elem.itertext()).strip()
                if label:
                    abstract_parts.append(f"{label}: {text}")
                else:
                    abstract_parts.append(text)
        abstract = " ".join(abstract_parts)

        author_list = article.find("AuthorList")
        authors = []
        if author_list is not None:
            for author in author_list.findall("Author"):
                last = author.find("LastName")
                initials = author.find("Initials")
                if last is not None and initials is not None:
                    authors.append(f"{last.text} {initials.text}")
                elif last is not None:
                    authors.append(last.text)
        if len(authors) > 3:
            author_str = f"{authors[0]}, {authors[1]}, et al."
        else:
            author_str = ", ".join(authors)

        journal_elem = article.find("Journal/Title")
        journal = journal_elem.text if journal_elem is not None else ""

        pub_date_elem = article.find("Journal/JournalIssue/PubDate")
        pub_year = None
        pub_date = ""
        if pub_date_elem is not None:
            year_elem = pub_date_elem.find("Year")
            month_elem = pub_date_elem.find("Month")
            if year_elem is not None:
                pub_year = int(year_elem.text)
                pub_date = year_elem.text
                if month_elem is not None:
                    pub_date = f"{year_elem.text} {month_elem.text}"
            else:
                medline_date = pub_date_elem.find("MedlineDate")
                if medline_date is not None and medline_date.text:
                    pub_date = medline_date.text
                    parts = medline_date.text.split()
                    if parts and parts[0].isdigit():
                        pub_year = int(parts[0])

        doi = ""
        article_id_list = element.find("PubmedData/ArticleIdList")
        if article_id_list is not None:
            for aid in article_id_list.findall("ArticleId"):
                if aid.get("IdType") == "doi":
                    doi = aid.text or ""
                    break

        pub_type_list = article.find("PublicationTypeList")
        publication_types = []
        if pub_type_list is not None:
            for pt in pub_type_list.findall("PublicationType"):
                if pt.text:
                    publication_types.append(pt.text)

        mesh_list = medline.find("MeshHeadingList")
        mesh_terms = []
        if mesh_list is not None:
            for heading in mesh_list.findall("MeshHeading/DescriptorName"):
                if heading.text:
                    mesh_terms.append(heading.text)

        evidence_level, evidence_score = PubMedClient._classify_evidence(
            publication_types
        )

        return {
            "pmid": pmid,
            "title": title,
            "abstract": abstract,
            "authors": author_str,
            "journal": journal,
            "pub_date": pub_date,
            "pub_year": pub_year,
            "doi": doi,
            "publication_types": publication_types,
            "mesh_terms": mesh_terms,
            "evidence_level": evidence_level,
            "evidence_score": evidence_score,
        }

    @staticmethod
    def _classify_evidence(publication_types: list[str]) -> tuple[str, float]:
        """Classify evidence level using GRADE-inspired hierarchy."""
        best_level, best_score = DEFAULT_EVIDENCE
        for pt in publication_types:
            if pt in EVIDENCE_HIERARCHY:
                level, score = EVIDENCE_HIERARCHY[pt]
                if score > best_score:
                    best_level, best_score = level, score
        return best_level, best_score

    def _rate_limit(self) -> None:
        """Enforce minimum interval between API requests."""
        now = time.monotonic()
        elapsed = now - self._last_request_time
        if elapsed < self._min_interval:
            time.sleep(self._min_interval - elapsed)
        self._last_request_time = time.monotonic()
