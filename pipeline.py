from __future__ import annotations

"""Air GTM ingestion pipeline.

Step 1 of the rebuild defines the data contract and stage boundaries.
Implementation details for normalization, validation, deduplication,
enrichment, scoring, and reporting will be added iteratively.
"""

import json
import re
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, cast

import pandas as pd


EXPECTED_INPUT_COLUMNS = (
    "Name",
    "Email",
    "Title",
    "Company",
    "Phone",
    "Source",
    "Country",
    "Created At",
)


INPUT_TO_CANONICAL_COLUMN_MAP = {
    "Name": "full_name",
    "Email": "email",
    "Title": "title",
    "Company": "company",
    "Phone": "phone",
    "Source": "source",
    "Country": "country",
    "Created At": "lead_date",
}


NORMALIZED_LEAD_COLUMNS = (
    "full_name",
    "first_name",
    "last_name",
    "email",
    "title",
    "company",
    "phone",
    "source",
    "country",
    "lead_date",
)


ENRICHMENT_COLUMNS = (
    "industry",
    "company_size",
    "company_domain",
    "enrichment_status",
)


SCORING_COLUMNS = (
    "lead_score",
    "score_reasons",
)


PIPELINE_METADATA_COLUMNS = (
    "source_row_number",
    "salesforce_ready",
)


REJECTION_METADATA_COLUMNS = ("drop_reason",)


CLEAN_LEAD_COLUMNS = (
    "source_row_number",
    "first_name",
    "last_name",
    "full_name",
    "email",
    "title",
    "company",
    "phone",
    "source",
    "country",
    "lead_date",
    "industry",
    "company_size",
    "company_domain",
    "enrichment_status",
    "lead_score",
    "score_reasons",
    "salesforce_ready",
)


ACCEPTED_REQUIRED_FIELDS = ("email",)


SALESFORCE_READY_REQUIRED_FIELDS = (
    "email",
    "last_name",
    "company",
)


FREE_EMAIL_DOMAINS = {
    "gmail.com",
    "hotmail.com",
    "icloud.com",
    "outlook.com",
    "yahoo.com",
}


@dataclass(slots=True)
class PipelineResult:
    clean_leads: pd.DataFrame
    rejected_leads: pd.DataFrame
    summary_report: dict[str, Any]


def load_scoring_rules(path: str | Path) -> dict[str, Any]:
    with Path(path).open("r", encoding="utf-8") as handle:
        return json.load(handle)


def run_pipeline(
    raw_df: pd.DataFrame,
    scoring_rules: dict[str, Any],
    enrichment_client: Any | None = None,
) -> PipelineResult:
    raise NotImplementedError("Step 1 scaffold only: pipeline stages will be added iteratively.")


def prepare_input_dataframe(raw_df: pd.DataFrame) -> pd.DataFrame:
    """
    Validates that the raw input dataframe contains the expected columns, selects and renames those columns,
    and adds any missing normalized lead columns with null values.
    """

    # Validate presence of expected input columns
    missing_columns = [column for column in EXPECTED_INPUT_COLUMNS if column not in raw_df.columns]
    if missing_columns:
        # Display missing columns in a user-friendly format
        missing_display = ", ".join(missing_columns)
        raise ValueError(f"Missing expected input columns: {missing_display}")

    # Prepare the dataframe by selecting and renaming columns, and adding missing normalized lead columns
    prepared = cast(pd.DataFrame, raw_df[list(EXPECTED_INPUT_COLUMNS)]).copy()
    prepared = prepared.rename(columns=INPUT_TO_CANONICAL_COLUMN_MAP)
    # Add source_row_number as a unique identifier for each row in the original input dataframe
    prepared.insert(0, "source_row_number", range(1, len(prepared) + 1))

    # Add missing normalized lead columns with null values
    for column in NORMALIZED_LEAD_COLUMNS:
        # This ensures that all expected columns are present in the prepared dataframe, even if they are not in the raw input.
        if column not in prepared.columns:
            prepared[column] = None

    # Reorder columns to have source_row_number first, followed by normalized lead columns, for consistency in downstream processing.
    ordered_columns = ("source_row_number", *NORMALIZED_LEAD_COLUMNS)

    # Return the prepared dataframe with the expected columns in a consistent order for downstream processing.
    return cast(pd.DataFrame, prepared[list(ordered_columns)])


def normalize_leads(df: pd.DataFrame) -> pd.DataFrame:
    normalized = cast(pd.DataFrame, df.copy())

    normalized["full_name"] = normalized["full_name"].apply(normalize_text)
    normalized["email"] = normalized["email"].apply(normalize_email)
    normalized["title"] = normalized["title"].apply(normalize_text)
    normalized["company"] = normalized["company"].apply(normalize_text)
    normalized["phone"] = normalized["phone"].apply(normalize_phone)
    normalized["source"] = normalized["source"].apply(normalize_text)
    normalized["country"] = normalized["country"].apply(normalize_text)
    normalized["lead_date"] = normalized["lead_date"].apply(normalize_date)

    split_names = normalized["full_name"].apply(split_full_name)
    normalized["first_name"] = split_names.apply(lambda parts: parts[0])
    normalized["last_name"] = split_names.apply(lambda parts: parts[1])

    ordered_columns = ("source_row_number", *NORMALIZED_LEAD_COLUMNS)
    return cast(pd.DataFrame, normalized[list(ordered_columns)])


def split_accepted_and_rejected(df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    raise NotImplementedError("Step 1 scaffold only.")


def deduplicate_leads(df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    raise NotImplementedError("Step 1 scaffold only.")


def enrich_leads(df: pd.DataFrame, enrichment_client: Any | None = None) -> pd.DataFrame:
    raise NotImplementedError("Step 1 scaffold only.")


def score_leads(df: pd.DataFrame, scoring_rules: dict[str, Any]) -> pd.DataFrame:
    raise NotImplementedError("Step 1 scaffold only.")


def finalize_clean_leads(df: pd.DataFrame) -> pd.DataFrame:
    raise NotImplementedError("Step 1 scaffold only.")


def build_summary_report(
    raw_df: pd.DataFrame,
    clean_leads: pd.DataFrame,
    rejected_leads: pd.DataFrame,
) -> dict[str, Any]:
    raise NotImplementedError("Step 1 scaffold only.")


def normalize_text(value: Any) -> str | None:
    if pd.isna(value):
        return None

    text = str(value).strip()
    if not text:
        return None

    compact_text = re.sub(r"\s+", " ", text)
    return compact_text or None


def normalize_email(value: Any) -> str | None:
    text = normalize_text(value)
    if not text:
        return None

    candidate = text.lower()
    if re.fullmatch(r"[^@\s]+@[^@\s]+\.[^@\s]+", candidate):
        return candidate

    return None


def split_full_name(value: Any) -> tuple[str | None, str | None]:
    full_name = normalize_text(value)
    if not full_name:
        return None, None

    parts = full_name.split(" ")
    if len(parts) == 1:
        return title_case_name_part(parts[0]), None

    return title_case_name_part(parts[0]), title_case_name_part(parts[-1])


def title_case_name_part(value: str | None) -> str | None:
    if not value:
        return None
    return value.title()


def normalize_date(value: Any) -> str | None:
    text = normalize_text(value)
    if not text:
        return None

    slash_date = parse_slash_date(text)
    if slash_date:
        return slash_date

    known_formats = (
        "%Y-%m-%d",
        "%Y/%m/%d",
        "%d-%b-%Y",
        "%d-%B-%Y",
        "%m/%d/%Y",
        "%d/%m/%Y",
        "%d-%m-%Y",
        "%m-%d-%Y",
        "%Y-%m-%d %H:%M:%S",
    )

    for date_format in known_formats:
        try:
            return datetime.strptime(text, date_format).date().isoformat()
        except ValueError:
            continue

    try:
        parsed = pd.to_datetime(text, errors="raise")
    except (TypeError, ValueError):
        return None

    return parsed.date().isoformat()


def normalize_phone(value: Any) -> str | None:
    text = normalize_text(value)
    if not text:
        return None

    without_extension = re.split(r"(?i)\b(?:ext|ext\.|x)\s*\d+\b", text, maxsplit=1)[0].strip()
    if not without_extension:
        return None

    has_leading_plus = without_extension.startswith("+")
    digits_only = re.sub(r"\D", "", without_extension)
    if not digits_only:
        return None

    normalized = f"+{digits_only}" if has_leading_plus else digits_only
    return normalized


def parse_slash_date(value: str) -> str | None:
    match = re.fullmatch(r"(\d{1,2})/(\d{1,2})/(\d{4})", value)
    if not match:
        return None

    first = int(match.group(1))
    second = int(match.group(2))
    year = int(match.group(3))

    if first > 12:
        day = first
        month = second
    elif second > 12:
        month = first
        day = second
    else:
        month = first
        day = second

    try:
        return datetime(year, month, day).date().isoformat()
    except ValueError:
        return None
