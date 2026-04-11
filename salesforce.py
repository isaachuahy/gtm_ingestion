from __future__ import annotations

"""Salesforce export mapping for clean lead data."""

from typing import Any

import pandas as pd


SALESFORCE_COLUMN_MAP = {
    "first_name": "FirstName",
    "last_name": "LastName",
    "email": "Email",
    "company": "Company",
    "title": "Title",
    "phone": "Phone",
    "source": "LeadSource",
    "country": "Country",
    "industry": "Industry__c",
    "company_size": "Company_Size__c",
    "company_domain": "Company_Domain__c",
    "lead_score": "Lead_Score__c",
    "score_reasons": "Score_Reasons__c",
    "lead_date": "Lead_Date__c",
    "source_row_number": "Source_Row_Number__c",
}


SALESFORCE_EXPORT_COLUMNS = (
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
)


def build_salesforce_export(
    clean_leads: pd.DataFrame,
    salesforce_ready_only: bool = True,
) -> pd.DataFrame:
    """Build a Salesforce-aligned export dataframe from clean leads."""

    export_df = clean_leads.copy()

    if salesforce_ready_only and "salesforce_ready" in export_df.columns:
        export_df = export_df.loc[export_df["salesforce_ready"] == True].copy()

    for source_column in SALESFORCE_COLUMN_MAP:
        if source_column not in export_df.columns:
            export_df[source_column] = None

    export_df["score_reasons"] = export_df["score_reasons"].apply(serialize_score_reasons)
    export_df = export_df.rename(columns=SALESFORCE_COLUMN_MAP)

    for export_column in SALESFORCE_EXPORT_COLUMNS:
        if export_column not in export_df.columns:
            export_df[export_column] = None

    return export_df[list(SALESFORCE_EXPORT_COLUMNS)].copy()


def serialize_score_reasons(value: Any) -> str | None:
    if value is None:
        return None

    if isinstance(value, list):
        filtered_values = [str(item).strip() for item in value if str(item).strip()]
        if not filtered_values:
            return None
        return "; ".join(filtered_values)

    text = str(value).strip()
    return text or None
