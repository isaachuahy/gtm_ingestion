from __future__ import annotations

import copy
from pathlib import Path

import pandas as pd

from pipeline import load_scoring_rules, score_leads


def load_test_scoring_rules() -> dict:
    rules_path = Path(__file__).resolve().parents[1] / "scoring_rules.json"
    return load_scoring_rules(rules_path)


def test_score_leads_adds_title_seniority_industry_and_company_size_points() -> None:
    leads = pd.DataFrame(
        [
            {
                "title": "Creative Operations Manager",
                "company": "Acme Studio",
                "email": "jane@acmestudio.com",
                "industry": "agency",
                "company_size": "mid_market",
                "enrichment_status": "enriched",
            }
        ]
    )

    scored = score_leads(leads, load_test_scoring_rules())

    assert scored.loc[0, "lead_score"] == 46
    assert scored.loc[0, "score_reasons"] == [
        "creative_operations",
        "manager_lead",
        "industry_agency",
        "company_size_mid_market",
    ]


def test_score_leads_applies_penalties_and_clamps_to_min_score() -> None:
    leads = pd.DataFrame(
        [
            {
                "title": "Software Engineer",
                "company": None,
                "email": "alex@gmail.com",
                "industry": "unknown",
                "company_size": "unknown",
                "enrichment_status": "failed",
            }
        ]
    )

    scored = score_leads(leads, load_test_scoring_rules())

    assert scored.loc[0, "lead_score"] == 0
    assert scored.loc[0, "score_reasons"] == [
        "engineering",
        "penalty_free_email_domain",
        "penalty_missing_company",
        "penalty_failed_enrichment",
    ]


def test_score_leads_clamps_to_max_score_when_total_exceeds_bound() -> None:
    scoring_rules = copy.deepcopy(load_test_scoring_rules())
    scoring_rules["base_score"] = 95

    leads = pd.DataFrame(
        [
            {
                "title": "Creative Operations Chief",
                "company": "Global Systems",
                "email": "jane@globalsystems.com",
                "industry": "media",
                "company_size": "enterprise",
                "enrichment_status": "enriched",
            }
        ]
    )

    scored = score_leads(leads, scoring_rules)

    assert scored.loc[0, "lead_score"] == 100
    assert scored.loc[0, "score_reasons"] == [
        "creative_operations",
        "executive",
        "industry_media",
        "company_size_enterprise",
    ]


def test_score_leads_returns_empty_scored_dataframe_for_empty_input() -> None:
    leads = pd.DataFrame(
        columns=[
            "title",
            "company",
            "email",
            "industry",
            "company_size",
            "enrichment_status",
        ]
    )

    scored = score_leads(leads, load_test_scoring_rules())

    assert scored.empty
    assert "lead_score" in scored.columns
    assert "score_reasons" in scored.columns


def test_score_leads_uses_first_matching_rule_per_category() -> None:
    leads = pd.DataFrame(
        [
            {
                "title": "Brand Director",
                "company": "Northwind Media",
                "email": "jane@northwindmedia.com",
                "industry": "media",
                "company_size": "mid_market",
                "enrichment_status": "enriched",
            }
        ]
    )

    scored = score_leads(leads, load_test_scoring_rules())

    assert scored.loc[0, "lead_score"] == 51
    assert scored.loc[0, "score_reasons"] == [
        "brand_and_creative_leadership",
        "vp_head_director",
        "industry_media",
        "company_size_mid_market",
    ]
