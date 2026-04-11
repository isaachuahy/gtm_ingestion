import pandas as pd

from salesforce import build_salesforce_export, serialize_score_reasons


def make_clean_leads_dataframe() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "source_row_number": 1,
                "first_name": "Jane",
                "last_name": "Doe",
                "email": "jane@example.com",
                "company": "Acme Studio",
                "title": "Creative Operations Manager",
                "phone": "5551234567",
                "source": "Inbound Demo",
                "country": "United States",
                "industry": "agency",
                "company_size": "mid_market",
                "company_domain": "acmestudio.com",
                "lead_score": 42,
                "score_reasons": ["title_match", "industry_match"],
                "lead_date": "2026-04-01",
                "salesforce_ready": True,
            },
            {
                "source_row_number": 2,
                "first_name": "Prince",
                "last_name": None,
                "email": "prince@example.com",
                "company": "Northwind",
                "title": "Artist",
                "phone": "5551230000",
                "source": "Referral",
                "country": "Canada",
                "industry": "media",
                "company_size": "small_business",
                "company_domain": "northwind.com",
                "lead_score": 18,
                "score_reasons": ["industry_match"],
                "lead_date": "2026-04-02",
                "salesforce_ready": False,
            },
        ]
    )


def test_build_salesforce_export_renames_columns_and_preserves_order() -> None:
    export_df = build_salesforce_export(make_clean_leads_dataframe(), salesforce_ready_only=False)

    assert list(export_df.columns) == [
        "FirstName",
        "LastName",
        "Email",
        "Company",
        "Title",
        "Phone",
        "LeadSource",
        "Country",
        "Industry__c",
        "Company_Size__c",
        "Company_Domain__c",
        "Lead_Score__c",
        "Score_Reasons__c",
        "Lead_Date__c",
        "Source_Row_Number__c",
    ]

    assert export_df.loc[0, "FirstName"] == "Jane"
    assert export_df.loc[0, "Email"] == "jane@example.com"
    assert export_df.loc[0, "LeadSource"] == "Inbound Demo"
    assert export_df.loc[0, "Industry__c"] == "agency"
    assert export_df.loc[0, "Lead_Score__c"] == 42


def test_build_salesforce_export_filters_to_salesforce_ready_rows_by_default() -> None:
    export_df = build_salesforce_export(make_clean_leads_dataframe())

    assert len(export_df) == 1
    assert export_df.loc[export_df.index[0], "Email"] == "jane@example.com"


def test_build_salesforce_export_can_include_non_ready_rows() -> None:
    export_df = build_salesforce_export(make_clean_leads_dataframe(), salesforce_ready_only=False)

    assert len(export_df) == 2
    assert export_df["Email"].tolist() == ["jane@example.com", "prince@example.com"]


def test_build_salesforce_export_adds_missing_expected_columns_as_null() -> None:
    clean_leads = pd.DataFrame(
        [
            {
                "first_name": "Jane",
                "last_name": "Doe",
                "email": "jane@example.com",
                "company": "Acme Studio",
                "salesforce_ready": True,
            }
        ]
    )

    export_df = build_salesforce_export(clean_leads)

    assert export_df.loc[0, "Title"] is None
    assert export_df.loc[0, "Phone"] is None
    assert export_df.loc[0, "Industry__c"] is None
    assert export_df.loc[0, "Score_Reasons__c"] is None
    assert export_df.loc[0, "Source_Row_Number__c"] is None


def test_build_salesforce_export_serializes_score_reasons_list() -> None:
    export_df = build_salesforce_export(make_clean_leads_dataframe(), salesforce_ready_only=False)

    assert export_df.loc[0, "Score_Reasons__c"] == "title_match; industry_match"
    assert export_df.loc[1, "Score_Reasons__c"] == "industry_match"


def test_serialize_score_reasons_handles_lists_strings_and_empty_values() -> None:
    assert serialize_score_reasons(["title_match", "industry_match"]) == "title_match; industry_match"
    assert serialize_score_reasons(["title_match", " ", "industry_match"]) == "title_match; industry_match"
    assert serialize_score_reasons([]) is None
    assert serialize_score_reasons(None) is None
    assert serialize_score_reasons("title_match") == "title_match"
