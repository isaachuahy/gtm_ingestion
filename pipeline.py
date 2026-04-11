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
from functools import lru_cache
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
    "raw_email",
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


COUNTRY_MAPPINGS_PATH = Path(__file__).resolve().with_name("country_mappings.json")


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
    prepared["raw_email"] = prepared["Email"]
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
    normalized["raw_email"] = normalized["raw_email"].apply(normalize_text)
    normalized["email"] = normalized["email"].apply(normalize_email)
    normalized["title"] = normalized["title"].apply(normalize_text)
    normalized["company"] = normalized["company"].apply(normalize_text)
    normalized["phone"] = normalized["phone"].apply(normalize_phone)
    normalized["source"] = normalized["source"].apply(normalize_text)
    normalized["country"] = normalized["country"].apply(normalize_country)
    normalized["lead_date"] = normalized["lead_date"].apply(normalize_date)

    split_names = normalized["full_name"].apply(split_full_name)
    normalized["first_name"] = split_names.apply(lambda parts: parts[0])
    normalized["last_name"] = split_names.apply(lambda parts: parts[1])

    ordered_columns = ("source_row_number", *NORMALIZED_LEAD_COLUMNS)
    return cast(pd.DataFrame, normalized[list(ordered_columns)])


def split_accepted_and_rejected(df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Splits the input dataframe into accepted and rejected leads based on validation rules.
    For this step, the only validation rule is that the email must be present and valid.
    """
    with_status = cast(pd.DataFrame, df.copy())
    with_status["drop_reason"] = with_status["email"].apply(
        lambda email: None if has_non_empty_value(email) else "missing_or_invalid_email"
    )

    accepted_mask = with_status["drop_reason"].isna()
    accepted = cast(pd.DataFrame, with_status.loc[accepted_mask, list(df.columns)].copy())
    rejected_columns = [*df.columns, *REJECTION_METADATA_COLUMNS]
    rejected = cast(
        pd.DataFrame,
        with_status.loc[~accepted_mask, rejected_columns].copy(),
    )

    return accepted, rejected


def deduplicate_leads(df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Deduplicates leads based on email address. 
    
    For each set of duplicates, the most complete lead is retained
    and the others are rejected with a drop reason indicating they were duplicates.
    """
    if df.empty:
        empty_rejected = cast(pd.DataFrame, pd.DataFrame(columns=[*df.columns, *REJECTION_METADATA_COLUMNS]))
        return cast(pd.DataFrame, df.copy()), empty_rejected

    # Process duplicates in a deterministic order by grouping by email and sorting by source_row_number.
    deduplicated_rows: list[dict[str, Any]] = []
    duplicate_rows: list[dict[str, Any]] = []

    # Group by email and sort by source_row_number to ensure deterministic processing order for duplicates
    grouped = df.groupby("email", sort=False, dropna=False)
    # For each group of duplicates, merge them into a single lead and collect the rejected duplicates with appropriate drop reasons.
    for _, group in grouped:
        # Sort the group by source_row_number to ensure deterministic processing order for duplicates
        group_df = cast(pd.DataFrame, group.sort_values("source_row_number"))
        # Merge the duplicate group into a single lead using the defined merging logic, and add it to the deduplicated results.
        merged_row = merge_duplicate_group(group_df)
        deduplicated_rows.append(merged_row)

        if len(group_df) > 1:
            for _, duplicate_row in group_df.iloc[1:].iterrows():
                rejected_row = duplicate_row.to_dict()
                rejected_row["drop_reason"] = "duplicate_email_merged"
                duplicate_rows.append(rejected_row)

    deduplicated = cast(pd.DataFrame, pd.DataFrame(deduplicated_rows))
    duplicate_rejected = cast(
        pd.DataFrame,
        pd.DataFrame(duplicate_rows, columns=[*df.columns, *REJECTION_METADATA_COLUMNS]),
    )

    return deduplicated, duplicate_rejected

def score_leads(df: pd.DataFrame, scoring_rules: dict[str, Any]) -> pd.DataFrame:
    scored = cast(pd.DataFrame, df.copy())

    if scored.empty:
        scored["lead_score"] = pd.Series(dtype="int64")
        scored["score_reasons"] = pd.Series(dtype=object)
        return scored

    base_score = int(scoring_rules.get("base_score", 0))
    score_bounds = scoring_rules.get("score_bounds", {})
    min_score = int(score_bounds.get("min", 0))
    max_score = int(score_bounds.get("max", 100))

    title_function_rules = scoring_rules.get("title_function_scores", [])
    seniority_rules = scoring_rules.get("seniority_scores", [])
    company_size_scores = scoring_rules.get("company_size_scores", {})
    industry_scores = scoring_rules.get("industry_scores", {})
    penalties = scoring_rules.get("penalties", {})

    lead_scores: list[int] = []
    score_reasons: list[list[str]] = []

    for _, row in scored.iterrows():
        title_text = (normalize_text(row.get("title")) or "").casefold()
        company = normalize_text(row.get("company"))
        industry = (normalize_text(row.get("industry")) or "").casefold()
        company_size = (normalize_text(row.get("company_size")) or "").casefold()
        email = normalize_text(row.get("email"))
        enrichment_status = (normalize_text(row.get("enrichment_status")) or "").casefold()

        score = base_score
        reasons: list[str] = []

        # Use the first matching rule in each category to avoid double-counting overlapping keywords.
        title_function_match = find_first_matching_score_rule(title_text, title_function_rules)
        if title_function_match:
            score += int(title_function_match["score"])
            reasons.append(str(title_function_match["name"]))

        seniority_match = find_first_matching_score_rule(title_text, seniority_rules)
        if seniority_match:
            score += int(seniority_match["score"])
            reasons.append(str(seniority_match["name"]))

        if industry in industry_scores:
            industry_score = int(industry_scores[industry])
            score += industry_score
            if industry_score:
                reasons.append(f"industry_{industry}")

        if company_size in company_size_scores:
            company_size_score = int(company_size_scores[company_size])
            score += company_size_score
            if company_size_score:
                reasons.append(f"company_size_{company_size}")

        email_domain = extract_email_domain(email)
        if email_domain in FREE_EMAIL_DOMAINS:
            free_email_penalty = int(penalties.get("free_email_domain", 0))
            score -= free_email_penalty
            if free_email_penalty:
                reasons.append("penalty_free_email_domain")

        if not has_non_empty_value(company):
            missing_company_penalty = int(penalties.get("missing_company", 0))
            score -= missing_company_penalty
            if missing_company_penalty:
                reasons.append("penalty_missing_company")

        if enrichment_status == "failed":
            failed_enrichment_penalty = int(penalties.get("failed_enrichment", 0))
            score -= failed_enrichment_penalty
            if failed_enrichment_penalty:
                reasons.append("penalty_failed_enrichment")

        bounded_score = max(min_score, min(score, max_score))
        lead_scores.append(int(bounded_score))
        score_reasons.append(reasons)

    scored["lead_score"] = lead_scores
    scored["score_reasons"] = score_reasons
    return scored

def finalize_clean_leads(df: pd.DataFrame) -> pd.DataFrame:
    """
    Finalize clean leads by filling expected output fields, computing
    Salesforce readiness, and returning a stable output schema.
    """
    finalized = cast(pd.DataFrame, df.copy())

    default_values: dict[str, Any] = {
        "industry": None,
        "company_size": None,
        "company_domain": None,
        "enrichment_status": "not_enriched",
        "lead_score": 0,
        "score_reasons": [],
    }

    for column, default_value in default_values.items():
        if column not in finalized.columns:
            finalized[column] = [clone_default_value(default_value) for _ in range(len(finalized))]
            continue

        finalized[column] = finalized[column].apply(
            lambda value, fallback=default_value: (
                clone_default_value(fallback) if not has_non_empty_value(value) else value
            )
        )

    finalized["salesforce_ready"] = finalized.apply(
        lambda row: bool(is_salesforce_ready_row(row)),
        axis=1,
    )

    for column in CLEAN_LEAD_COLUMNS:
        if column not in finalized.columns:
            finalized[column] = None

    return cast(pd.DataFrame, finalized[list(CLEAN_LEAD_COLUMNS)].copy())


def build_summary_report(
    raw_df: pd.DataFrame,
    clean_leads: pd.DataFrame,
    rejected_leads: pd.DataFrame,
) -> dict[str, Any]:
    """
    Builds a summary report containing counts of raw, clean, rejected, and Salesforce-ready leads, 
    as well as breakdowns of enrichment statuses and rejection reasons.
    """
    raw_rows = int(len(raw_df))
    salesforce_ready_rows = 0
    clean_but_not_salesforce_ready_rows = len(clean_leads)
    average_lead_score = 0.0

    if "salesforce_ready" in clean_leads.columns:
        salesforce_ready_mask = clean_leads["salesforce_ready"] == True
        salesforce_ready_rows = int(salesforce_ready_mask.sum())
        clean_but_not_salesforce_ready_rows = int((~salesforce_ready_mask).sum())

    if "lead_score" in clean_leads.columns and not clean_leads.empty:
        average_lead_score = float(clean_leads["lead_score"].fillna(0).mean())

    enrichment_status_counts: dict[str, int] = {}
    enriched_rows = 0
    if "enrichment_status" in clean_leads.columns:
        enrichment_status_counts = {
            str(key): int(value)
            for key, value in clean_leads["enrichment_status"].value_counts(dropna=False).items()
        }
        enriched_rows = int((clean_leads["enrichment_status"] == "enriched").sum())

    percent_enriched_from_raw = 0.0
    if raw_rows > 0:
        percent_enriched_from_raw = (enriched_rows / raw_rows) * 100

    rejection_reason_counts: dict[str, int] = {}
    if "drop_reason" in rejected_leads.columns:
        rejection_reason_counts = {
            str(key): int(value)
            for key, value in rejected_leads["drop_reason"].value_counts(dropna=False).items()
        }

    return {
        "raw_rows": raw_rows,
        "clean_rows": int(len(clean_leads)),
        "rejected_rows": int(len(rejected_leads)),
        "salesforce_ready_rows": salesforce_ready_rows,
        "clean_but_not_salesforce_ready_rows": clean_but_not_salesforce_ready_rows,
        "average_lead_score": average_lead_score,
        "percent_enriched_from_raw": percent_enriched_from_raw,
        "enrichment_status_counts": enrichment_status_counts,
        "rejection_reason_counts": rejection_reason_counts,
    }


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
    # Basic email format validation using regex. 
    # This is a simple check and can be enhanced with more sophisticated validation if needed.
    if re.fullmatch(r"[^@\s]+@[^@\s]+\.[^@\s]+", candidate):
        return candidate

    return None


def normalize_country(value: Any) -> str | None:
    text = normalize_text(value)
    if not text:
        return None

    alias_map = load_country_alias_map()
    lookup_key = normalize_country_lookup_key(text)
    return alias_map.get(lookup_key, text)


def split_full_name(value: Any) -> tuple[str | None, str | None]:
    full_name = normalize_text(value)
    if not full_name:
        return None, None

    parts = full_name.split(" ")
    if len(parts) == 1:
        return title_case_name_part(parts[0]), None

    return title_case_name_part(parts[0]), title_case_name_part(parts[-1])


def title_case_name_part(value: str | None) -> str | None:
    """
    Applies title casing to a name part while preserving common name prefixes and particles in lowercase.
    """
    if not value:
        return None
    return value.title()


def normalize_date(value: Any) -> str | None:
    """
    Normalizes date values by attempting to parse them using a variety of common date formats, including handling ambiguous slash-separated formats.
    """
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


@lru_cache(maxsize=1)
def load_country_alias_map() -> dict[str, str]:
    """
    Loads country mappings from the country_mappings.json file and constructs a lookup map that normalizes various country name formats to their canonical names.
    """
    with COUNTRY_MAPPINGS_PATH.open("r", encoding="utf-8") as handle:
        country_entries = json.load(handle)

    alias_map: dict[str, str] = {}
    for entry in country_entries:
        canonical_name = entry["canonical_name"]
        alias_candidates = [
            canonical_name,
            entry.get("alpha2"),
            entry.get("alpha3"),
            *entry.get("aliases", []),
        ]

        for alias in alias_candidates:
            if not alias:
                continue
            alias_map[normalize_country_lookup_key(alias)] = canonical_name

    return alias_map


def normalize_country_lookup_key(value: str) -> str:
    """
    Normalizes country names for lookup by applying case folding, trimming whitespace, and removing common punctuation.
    """
    normalized = value.casefold().strip()
    normalized = normalized.replace("&", " and ")
    normalized = re.sub(r"[.'’]", "", normalized)
    normalized = normalized.replace("_", " ")
    normalized = re.sub(r"[^\w\s]+", " ", normalized)
    normalized = re.sub(r"\s+", " ", normalized)
    return normalized.strip()


def normalize_phone(value: Any) -> str | None:
    """
    Normalizes phone numbers by removing non-digit characters, while preserving a leading "+" if present.
    """

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
    """
    Helper function to parse dates in formats that use slashes as separators, such as "MM/DD/YYYY" or "DD/MM/YYYY".
    """
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


def has_non_empty_value(value: Any) -> bool:
    """
    Helper function to determine if a value is considered non-empty for the purposes of merging duplicate leads.
    This function checks for None, NaN, empty strings, and strings that are only whitespace
    """
    if value is None:
        return False
    if isinstance(value, (list, tuple, set, dict)):
        return len(value) > 0
    try:
        if pd.isna(value):
            return False
    except (TypeError, ValueError):
        pass
    if isinstance(value, str):
        return value.strip() != ""
    return True


def find_first_matching_score_rule(
    text: str,
    rules: list[dict[str, Any]],
) -> dict[str, Any] | None:
    # Normalizes the input text and keywords for case-insensitive matching, and searches for the first rule 
    # where any of its keywords match the text as a whole word.
    normalized_text = re.sub(r"\s+", " ", text.casefold()).strip()

    # Iterate through the provided scoring rules and their associated keywords to find the first rule that matches the input text.
    for rule in rules:
        for keyword in rule.get("keywords", []):
            normalized_keyword = re.sub(r"\s+", " ", str(keyword).casefold()).strip()
            if not normalized_keyword:
                continue

            pattern = rf"\b{re.escape(normalized_keyword)}\b"
            if re.search(pattern, normalized_text):
                return rule

    return None


def extract_email_domain(email: str | None) -> str | None:
    if not email or "@" not in email:
        return None
    return email.rsplit("@", 1)[1].casefold()


def merge_duplicate_group(group_df: pd.DataFrame) -> dict[str, Any]:
    """
    Merges a group of duplicate leads (with the same email) into a single lead by applying the following logic:
    - For each column, if there are multiple non-empty values across the duplicates, apply column-specific rules to choose the most appropriate value.
    - For "source_row_number", choose the minimum value to retain the earliest source row number among the duplicates.
    - For "lead_date", choose the minimum (oldest) date to retain the earliest lead date among the duplicates.
    - For "phone", prefer values with a leading "+" and choose the longest valid phone number to retain the most complete phone information.
    """
    # Convert the group dataframe to a list of dictionaries for easier processing in the merging logic.
    # Each dictionary represents a lead, and the list contains all duplicates for the same email.
    # itearrows() is used to iterate over the rows of the group dataframe, and to_dict() converts each row 
    # to a dictionary format for easier access to column values during merging.
    rows = [row.to_dict() for _, row in group_df.iterrows()]
    # Use the lead completeness score to determine the primary row among the duplicates, 
    # which serves as the base for merging.
    primary_row = max(rows, key=lead_completeness_score)
    merged = primary_row.copy()

    # For each column in the group, apply the merging logic to choose the appropriate value among the duplicates,
    # and update the merged result accordingly.
    for column in group_df.columns:
        merged[column] = choose_merged_value(column, rows, primary_row.get(column))

    return merged


def choose_merged_value(column: str, rows: list[dict[str, Any]], fallback: Any) -> Any:
    """
    Helper function that chooses the merged value for a specific column among a group of duplicate rows 
    based on column-specific rules:
    - For "source_row_number", choose the minimum value.
    - For "lead_date", choose the minimum (oldest) date.
    - For "phone", prefer values with a leading "+" and choose the longest valid phone number.
    - For "full_name", "title", and "company", choose the longest non-empty
    - For "source", "country", "raw_email", "email", "first_name", and "last_name", choose the first non-empty value.
    """
    values = [row.get(column) for row in rows if has_non_empty_value(row.get(column))]
    if not values:
        return fallback

    if column == "source_row_number":
        return min(int(value) for value in values)

    if column == "lead_date":
        return min(values)

    if column == "phone":
        plus_prefixed = [value for value in values if isinstance(value, str) and value.startswith("+")]
        candidate_pool = plus_prefixed or values
        return max(candidate_pool, key=lambda value: len(str(value)))

    if column in {"full_name", "title", "company"}:
        return max(values, key=lambda value: len(str(value)))

    if column in {"source", "country", "raw_email", "email", "first_name", "last_name"}:
        return values[0]

    return values[0]


def lead_completeness_score(row: dict[str, Any]) -> int:
    """
    Helper function to calculate a completeness score for a lead based on the presence of non-empty values in key fields.
    This score is used to determine which lead to retain when merging duplicates, with a higher score indicating a more complete lead.
    """
    scored_fields = (
        "full_name",
        "first_name",
        "last_name",
        "raw_email",
        "email",
        "title",
        "company",
        "phone",
        "source",
        "country",
        "lead_date",
    )
    return sum(1 for field in scored_fields if has_non_empty_value(row.get(field)))


def is_salesforce_ready_row(row: pd.Series) -> bool:
    return all(has_non_empty_value(row.get(field)) for field in SALESFORCE_READY_REQUIRED_FIELDS)


def clone_default_value(value: Any) -> Any:
    """
    Helper function to clone default values for enrichment and scoring columns to avoid shared mutable defaults across rows.
    This function creates a new instance of the default value for each row, which is important for mutable types like lists and dictionaries 
    to prevent unintended side effects from shared references.
    """
    if isinstance(value, list):
        return list(value)
    if isinstance(value, dict):
        return dict(value)
    if isinstance(value, set):
        return set(value)
    if isinstance(value, tuple):
        return tuple(value)
    return value
