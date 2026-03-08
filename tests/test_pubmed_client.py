"""Tests for PubMed E-utilities client.

All API calls mocked — no real network requests.
"""

from __future__ import annotations

import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "health_unified_platform"))

from health_platform.knowledge.pubmed_client import (
    DEFAULT_EVIDENCE,
    PubMedClient,
)

MOCK_ESEARCH_RESPONSE = {
    "esearchresult": {
        "count": "3",
        "retmax": "3",
        "idlist": ["39000001", "39000002", "39000003"],
    }
}

MOCK_EFETCH_XML = """<?xml version="1.0" encoding="UTF-8"?>
<PubmedArticleSet>
  <PubmedArticle>
    <MedlineCitation>
      <PMID>39000001</PMID>
      <Article>
        <ArticleTitle>Magnesium supplementation and sleep quality: a meta-analysis</ArticleTitle>
        <Abstract>
          <AbstractText Label="BACKGROUND">Sleep disorders affect millions worldwide.</AbstractText>
          <AbstractText Label="RESULTS">Magnesium improved sleep quality scores significantly.</AbstractText>
        </Abstract>
        <AuthorList>
          <Author><LastName>Smith</LastName><Initials>J</Initials></Author>
          <Author><LastName>Jones</LastName><Initials>A</Initials></Author>
          <Author><LastName>Wang</LastName><Initials>L</Initials></Author>
          <Author><LastName>Kim</LastName><Initials>S</Initials></Author>
        </AuthorList>
        <Journal>
          <Title>Sleep Medicine Reviews</Title>
          <JournalIssue><PubDate><Year>2024</Year><Month>Jan</Month></PubDate></JournalIssue>
        </Journal>
        <PublicationTypeList>
          <PublicationType>Meta-Analysis</PublicationType>
          <PublicationType>Journal Article</PublicationType>
        </PublicationTypeList>
      </Article>
      <MeshHeadingList>
        <MeshHeading><DescriptorName>Magnesium</DescriptorName></MeshHeading>
        <MeshHeading><DescriptorName>Sleep Quality</DescriptorName></MeshHeading>
      </MeshHeadingList>
    </MedlineCitation>
    <PubmedData>
      <ArticleIdList>
        <ArticleId IdType="pubmed">39000001</ArticleId>
        <ArticleId IdType="doi">10.1016/j.smrv.2024.001</ArticleId>
      </ArticleIdList>
    </PubmedData>
  </PubmedArticle>
  <PubmedArticle>
    <MedlineCitation>
      <PMID>39000002</PMID>
      <Article>
        <ArticleTitle>HRV and sleep: a randomized controlled trial</ArticleTitle>
        <Abstract>
          <AbstractText>Heart rate variability predicts sleep quality.</AbstractText>
        </Abstract>
        <AuthorList>
          <Author><LastName>Garcia</LastName><Initials>M</Initials></Author>
          <Author><LastName>Lee</LastName><Initials>K</Initials></Author>
        </AuthorList>
        <Journal>
          <Title>Journal of Sleep Research</Title>
          <JournalIssue><PubDate><Year>2023</Year></PubDate></JournalIssue>
        </Journal>
        <PublicationTypeList>
          <PublicationType>Randomized Controlled Trial</PublicationType>
        </PublicationTypeList>
      </Article>
    </MedlineCitation>
    <PubmedData>
      <ArticleIdList>
        <ArticleId IdType="pubmed">39000002</ArticleId>
      </ArticleIdList>
    </PubmedData>
  </PubmedArticle>
</PubmedArticleSet>
"""


class TestPubMedClientParsing:
    def test_parse_article_extracts_all_fields(self) -> None:
        import xml.etree.ElementTree as ET

        root = ET.fromstring(MOCK_EFETCH_XML)
        elem = root.findall(".//PubmedArticle")[0]
        article = PubMedClient._parse_article_xml(elem)
        assert article is not None
        assert article["pmid"] == "39000001"
        assert "Magnesium" in article["title"]
        assert article["pub_year"] == 2024
        assert article["doi"] == "10.1016/j.smrv.2024.001"
        assert "Meta-Analysis" in article["publication_types"]

    def test_parse_article_truncates_long_author_list(self) -> None:
        import xml.etree.ElementTree as ET

        root = ET.fromstring(MOCK_EFETCH_XML)
        article = PubMedClient._parse_article_xml(root.findall(".//PubmedArticle")[0])
        assert article is not None
        assert "et al." in article["authors"]

    def test_parse_article_structured_abstract(self) -> None:
        import xml.etree.ElementTree as ET

        root = ET.fromstring(MOCK_EFETCH_XML)
        article = PubMedClient._parse_article_xml(root.findall(".//PubmedArticle")[0])
        assert article is not None
        assert "BACKGROUND:" in article["abstract"]

    def test_parse_article_mesh_terms(self) -> None:
        import xml.etree.ElementTree as ET

        root = ET.fromstring(MOCK_EFETCH_XML)
        article = PubMedClient._parse_article_xml(root.findall(".//PubmedArticle")[0])
        assert article is not None
        assert "Magnesium" in article["mesh_terms"]

    def test_classify_evidence_meta_analysis(self) -> None:
        level, score = PubMedClient._classify_evidence(
            ["Meta-Analysis", "Journal Article"]
        )
        assert level == "meta_analysis"
        assert score == 1.0

    def test_classify_evidence_rct(self) -> None:
        level, score = PubMedClient._classify_evidence(["Randomized Controlled Trial"])
        assert level == "rct"
        assert score == 0.85

    def test_classify_evidence_unknown_type(self) -> None:
        level, score = PubMedClient._classify_evidence(["Journal Article"])
        assert level == DEFAULT_EVIDENCE[0]

    def test_classify_evidence_picks_highest(self) -> None:
        level, score = PubMedClient._classify_evidence(["Review", "Meta-Analysis"])
        assert level == "meta_analysis"
        assert score == 1.0


class TestPubMedClientFetch:
    @patch("health_platform.knowledge.pubmed_client.get_secret", return_value=None)
    @patch("health_platform.knowledge.pubmed_client.requests.Session")
    def test_search_returns_pmids(self, mock_session_cls, _mock_secret) -> None:
        mock_session = MagicMock()
        mock_resp = MagicMock()
        mock_resp.json.return_value = MOCK_ESEARCH_RESPONSE
        mock_resp.raise_for_status.return_value = None
        mock_session.get.return_value = mock_resp
        mock_session_cls.return_value = mock_session
        client = PubMedClient(api_key=None)
        client.session = mock_session
        client._last_request_time = 0.0
        pmids = client.search("magnesium sleep", max_results=3)
        assert pmids == ["39000001", "39000002", "39000003"]

    @patch("health_platform.knowledge.pubmed_client.get_secret", return_value=None)
    @patch("health_platform.knowledge.pubmed_client.requests.Session")
    def test_fetch_articles_parses_xml(self, mock_session_cls, _mock_secret) -> None:
        mock_session = MagicMock()
        mock_resp = MagicMock()
        mock_resp.text = MOCK_EFETCH_XML
        mock_resp.raise_for_status.return_value = None
        mock_session.get.return_value = mock_resp
        mock_session_cls.return_value = mock_session
        client = PubMedClient(api_key=None)
        client.session = mock_session
        client._last_request_time = 0.0
        articles = client.fetch_articles(["39000001", "39000002"])
        assert len(articles) == 2
        assert articles[0]["evidence_level"] == "meta_analysis"
        assert articles[1]["evidence_level"] == "rct"

    @patch("health_platform.knowledge.pubmed_client.get_secret", return_value=None)
    def test_rate_limiting(self, _mock_secret) -> None:
        client = PubMedClient(api_key=None)
        assert client._min_interval == 0.34

    @patch("health_platform.knowledge.pubmed_client.get_secret", return_value=None)
    def test_fetch_empty_pmids_returns_empty(self, _mock_secret) -> None:
        client = PubMedClient(api_key=None)
        assert client.fetch_articles([]) == []
