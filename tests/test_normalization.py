import pandas as pd
import pytest

from pipeline import (
    deduplicate_leads,
    normalize_leads,
    prepare_input_dataframe,
    split_accepted_and_rejected,
)


def make_raw_dataframe() -> pd.DataFrame:
    """Helper function to create a sample raw dataframe of 2 leads for testing."""
    return pd.DataFrame(
        [
            {
                "Name": "Jane Doe",
                "Email": "jane@example.com",
                "Title": "Creative Operations Manager",
                "Company": "Acme Studio",
                "Phone": "(555) 123-4567",
                "Source": "Inbound Demo",
                "Country": "USA",
                "Created At": "2026-04-01",
            },
            {
                "Name": "John Smith",
                "Email": "john@example.com",
                "Title": "Brand Director",
                "Company": "Northwind",
                "Phone": "+1 416 555 0101",
                "Source": "Webinar",
                "Country": "Canada",
                "Created At": "2026-04-02",
            },
        ]
    )


def test_prepare_input_dataframe_renames_and_orders_columns() -> None:
    prepared = prepare_input_dataframe(make_raw_dataframe())

    assert list(prepared.columns) == [
        "source_row_number",
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
    ]

    assert prepared.loc[0, "full_name"] == "Jane Doe"
    assert prepared.loc[0, "raw_email"] == "jane@example.com"
    assert prepared.loc[0, "email"] == "jane@example.com"
    assert prepared.loc[0, "source"] == "Inbound Demo"
    assert prepared.loc[0, "lead_date"] == "2026-04-01"


def test_prepare_input_dataframe_adds_source_row_number() -> None:
    prepared = prepare_input_dataframe(make_raw_dataframe())

    assert prepared["source_row_number"].tolist() == [1, 2]


def test_prepare_input_dataframe_initializes_derived_name_columns() -> None:
    prepared = prepare_input_dataframe(make_raw_dataframe())

    assert prepared["first_name"].isna().all()
    assert prepared["last_name"].isna().all()


def test_prepare_input_dataframe_requires_expected_columns() -> None:
    raw_df = make_raw_dataframe().drop(columns=["Source"])

    with pytest.raises(ValueError, match="Missing expected input columns: Source"):
        prepare_input_dataframe(raw_df)


def test_normalize_leads_normalizes_email_and_text_fields() -> None:
    raw_df = pd.DataFrame(
        [
            {
                "Name": "  jane   doe  ",
                "Email": "  JANE@EXAMPLE.COM  ",
                "Title": "  Creative   Operations Manager ",
                "Company": "  Acme   Studio ",
                "Phone": "(555) 123-4567",
                "Source": "  Inbound   Demo ",
                "Country": "  USA ",
                "Created At": "2026-04-01",
            }
        ]
    )

    normalized = normalize_leads(prepare_input_dataframe(raw_df))

    assert normalized.loc[0, "full_name"] == "jane doe"
    assert normalized.loc[0, "raw_email"] == "JANE@EXAMPLE.COM"
    assert normalized.loc[0, "email"] == "jane@example.com"
    assert normalized.loc[0, "title"] == "Creative Operations Manager"
    assert normalized.loc[0, "company"] == "Acme Studio"
    assert normalized.loc[0, "source"] == "Inbound Demo"
    assert normalized.loc[0, "country"] == "United States"


def test_normalize_leads_splits_full_name_on_best_effort_basis() -> None:
    raw_df = pd.DataFrame(
        [
            {
                "Name": "jane mary doe",
                "Email": "jane@example.com",
                "Title": "Creative Operations Manager",
                "Company": "Acme Studio",
                "Phone": "(555) 123-4567",
                "Source": "Inbound Demo",
                "Country": "USA",
                "Created At": "2026-04-01",
            },
            {
                "Name": "Prince",
                "Email": "prince@example.com",
                "Title": "Artist",
                "Company": "Northwind",
                "Phone": "5551234567",
                "Source": "Referral",
                "Country": "Canada",
                "Created At": "2026-04-02",
            },
        ]
    )

    normalized = normalize_leads(prepare_input_dataframe(raw_df))

    assert normalized.loc[0, "first_name"] == "Jane"
    assert normalized.loc[0, "last_name"] == "Doe"
    assert normalized.loc[1, "first_name"] == "Prince"
    assert pd.isna(normalized.loc[1, "last_name"])


def test_normalize_leads_normalizes_phone_numbers_conservatively() -> None:
    raw_df = pd.DataFrame(
        [
            {
                "Name": "Jane Doe",
                "Email": "jane@example.com",
                "Title": "Creative Operations Manager",
                "Company": "Acme Studio",
                "Phone": "(555) 123-4567",
                "Source": "Inbound Demo",
                "Country": "USA",
                "Created At": "2026-04-01",
            },
            {
                "Name": "John Smith",
                "Email": "john@example.com",
                "Title": "Brand Director",
                "Company": "Northwind",
                "Phone": "+1 (416) 555-0101 ext 9",
                "Source": "Webinar",
                "Country": "Canada",
                "Created At": "2026-04-02",
            },
        ]
    )

    normalized = normalize_leads(prepare_input_dataframe(raw_df))

    assert normalized.loc[0, "phone"] == "5551234567"
    assert normalized.loc[1, "phone"] == "+14165550101"


@pytest.mark.parametrize(
    ("raw_date", "expected_date"),
    [
        ("15-Oct-2024", "2024-10-15"),
        ("2025-06-20", "2025-06-20"),
        ("10/01/2024", "2024-10-01"),
        ("10/18/2024", "2024-10-18"),
        ("15/10/2024", "2024-10-15"),
        ("2024/10/15", "2024-10-15"),
    ],
)
def test_normalize_leads_normalizes_multiple_date_formats(
    raw_date: str,
    expected_date: str,
) -> None:
    raw_df = pd.DataFrame(
        [
            {
                "Name": "Jane Doe",
                "Email": "jane@example.com",
                "Title": "Creative Operations Manager",
                "Company": "Acme Studio",
                "Phone": "(555) 123-4567",
                "Source": "Inbound Demo",
                "Country": "USA",
                "Created At": raw_date,
            }
        ]
    )

    normalized = normalize_leads(prepare_input_dataframe(raw_df))

    assert normalized.loc[0, "lead_date"] == expected_date


def test_normalize_leads_invalid_email_becomes_null_but_preserves_raw_email() -> None:
    raw_df = pd.DataFrame(
        [
            {
                "Name": "Jane Doe",
                "Email": "  not-an-email  ",
                "Title": "Creative Operations Manager",
                "Company": "Acme Studio",
                "Phone": "(555) 123-4567",
                "Source": "Inbound Demo",
                "Country": "USA",
                "Created At": "2026-04-01",
            }
        ]
    )

    normalized = normalize_leads(prepare_input_dataframe(raw_df))

    assert normalized.loc[0, "raw_email"] == "not-an-email"
    assert pd.isna(normalized.loc[0, "email"])


@pytest.mark.parametrize(
    ("raw_country", "expected_country"),
    [
        ("USA", "United States"),
        ("u.s.", "United States"),
        ("United States of America", "United States"),
        ("GB", "United Kingdom"),
        ("Great Britain", "United Kingdom"),
        ("Canada", "Canada"),
    ],
)
def test_normalize_leads_maps_country_aliases_to_canonical_names(
    raw_country: str,
    expected_country: str,
) -> None:
    raw_df = pd.DataFrame(
        [
            {
                "Name": "Jane Doe",
                "Email": "jane@example.com",
                "Title": "Creative Operations Manager",
                "Company": "Acme Studio",
                "Phone": "(555) 123-4567",
                "Source": "Inbound Demo",
                "Country": raw_country,
                "Created At": "2026-04-01",
            }
        ]
    )

    normalized = normalize_leads(prepare_input_dataframe(raw_df))

    assert normalized.loc[0, "country"] == expected_country


def test_normalize_leads_keeps_cleaned_unknown_country_values() -> None:
    raw_df = pd.DataFrame(
        [
            {
                "Name": "Jane Doe",
                "Email": "jane@example.com",
                "Title": "Creative Operations Manager",
                "Company": "Acme Studio",
                "Phone": "(555) 123-4567",
                "Source": "Inbound Demo",
                "Country": "  Wakanda   Republic  ",
                "Created At": "2026-04-01",
            }
        ]
    )

    normalized = normalize_leads(prepare_input_dataframe(raw_df))

    assert normalized.loc[0, "country"] == "Wakanda Republic"


def test_split_accepted_and_rejected_keeps_valid_normalized_emails_in_accepted() -> None:
    normalized = normalize_leads(prepare_input_dataframe(make_raw_dataframe()))

    accepted, rejected = split_accepted_and_rejected(normalized)

    assert len(accepted) == 2
    assert len(rejected) == 0
    assert accepted["email"].tolist() == ["jane@example.com", "john@example.com"]


def test_split_accepted_and_rejected_rejects_invalid_or_missing_emails() -> None:
    raw_df = pd.DataFrame(
        [
            {
                "Name": "Jane Doe",
                "Email": "not-an-email",
                "Title": "Creative Operations Manager",
                "Company": "Acme Studio",
                "Phone": "(555) 123-4567",
                "Source": "Inbound Demo",
                "Country": "USA",
                "Created At": "2026-04-01",
            },
            {
                "Name": "John Smith",
                "Email": "   ",
                "Title": "Brand Director",
                "Company": "Northwind",
                "Phone": "+1 416 555 0101",
                "Source": "Webinar",
                "Country": "Canada",
                "Created At": "2026-04-02",
            },
            {
                "Name": "Alice Jones",
                "Email": "alice@example.com",
                "Title": "Studio Manager",
                "Company": "Creative House",
                "Phone": "+44 20 7946 0018",
                "Source": "Referral",
                "Country": "GB",
                "Created At": "2026-04-03",
            },
        ]
    )

    normalized = normalize_leads(prepare_input_dataframe(raw_df))
    accepted, rejected = split_accepted_and_rejected(normalized)

    assert len(accepted) == 1
    assert accepted.loc[accepted.index[0], "email"] == "alice@example.com"
    assert len(rejected) == 2
    assert rejected["drop_reason"].tolist() == [
        "missing_or_invalid_email",
        "missing_or_invalid_email",
    ]
    assert rejected.loc[rejected.index[0], "raw_email"] == "not-an-email"
    assert pd.isna(rejected.loc[rejected.index[1], "raw_email"])


def test_split_accepted_and_rejected_treats_trimmed_valid_email_as_accepted() -> None:
    raw_df = pd.DataFrame(
        [
            {
                "Name": "Jane Doe",
                "Email": "  JANE@EXAMPLE.COM ",
                "Title": "Creative Operations Manager",
                "Company": "Acme Studio",
                "Phone": "(555) 123-4567",
                "Source": "Inbound Demo",
                "Country": "USA",
                "Created At": "2026-04-01",
            }
        ]
    )

    normalized = normalize_leads(prepare_input_dataframe(raw_df))
    accepted, rejected = split_accepted_and_rejected(normalized)

    assert len(accepted) == 1
    assert len(rejected) == 0
    assert accepted.loc[accepted.index[0], "email"] == "jane@example.com"
    assert accepted.loc[accepted.index[0], "raw_email"] == "JANE@EXAMPLE.COM"


def test_split_accepted_and_rejected_handles_single_at_symbol_edge_case() -> None:
    raw_df = pd.DataFrame(
        [
            {
                "Name": "Jane Doe",
                "Email": "@",
                "Title": "Creative Operations Manager",
                "Company": "Acme Studio",
                "Phone": "(555) 123-4567",
                "Source": "Inbound Demo",
                "Country": "USA",
                "Created At": "2026-04-01",
            }
        ]
    )

    normalized = normalize_leads(prepare_input_dataframe(raw_df))
    accepted, rejected = split_accepted_and_rejected(normalized)

    assert len(accepted) == 0
    assert len(rejected) == 1
    assert rejected.loc[rejected.index[0], "drop_reason"] == "missing_or_invalid_email"
    assert rejected.loc[rejected.index[0], "raw_email"] == "@"


def test_deduplicate_leads_merges_complementary_duplicate_rows() -> None:
    raw_df = pd.DataFrame(
        [
            {
                "Name": "Jane Doe",
                "Email": "jane@example.com",
                "Title": "",
                "Company": "Acme",
                "Phone": "555-123-4567",
                "Source": "Inbound Demo",
                "Country": "USA",
                "Created At": "2026-04-03",
            },
            {
                "Name": "Jane A. Doe",
                "Email": "jane@example.com",
                "Title": "Creative Operations Manager",
                "Company": "Acme Studio",
                "Phone": "+1 (555) 123-4567",
                "Source": "Inbound Demo",
                "Country": "United States of America",
                "Created At": "2026-04-01",
            },
        ]
    )

    normalized = normalize_leads(prepare_input_dataframe(raw_df))
    accepted, _ = split_accepted_and_rejected(normalized)
    deduplicated, duplicate_rejected = deduplicate_leads(accepted)

    assert len(deduplicated) == 1
    assert len(duplicate_rejected) == 1

    merged = deduplicated.iloc[0]
    assert merged["email"] == "jane@example.com"
    assert merged["full_name"] == "Jane A. Doe"
    assert merged["title"] == "Creative Operations Manager"
    assert merged["company"] == "Acme Studio"
    assert merged["phone"] == "+15551234567"
    assert merged["country"] == "United States"
    assert merged["lead_date"] == "2026-04-01"
    assert merged["source_row_number"] == 1

    rejected_row = duplicate_rejected.iloc[0]
    assert rejected_row["drop_reason"] == "duplicate_email_merged"
    assert rejected_row["email"] == "jane@example.com"


def test_deduplicate_leads_keeps_first_seen_source_and_raw_email() -> None:
    raw_df = pd.DataFrame(
        [
            {
                "Name": "John Smith",
                "Email": "  JOHN@EXAMPLE.COM ",
                "Title": "Brand Director",
                "Company": "Northwind",
                "Phone": "5551234567",
                "Source": "Webinar",
                "Country": "Canada",
                "Created At": "2026-04-02",
            },
            {
                "Name": "John Smith",
                "Email": "john@example.com",
                "Title": "Brand Director",
                "Company": "Northwind Agency",
                "Phone": "5551234567",
                "Source": "Referral",
                "Country": "CA",
                "Created At": "2026-04-03",
            },
        ]
    )

    normalized = normalize_leads(prepare_input_dataframe(raw_df))
    accepted, _ = split_accepted_and_rejected(normalized)
    deduplicated, duplicate_rejected = deduplicate_leads(accepted)

    assert len(deduplicated) == 1
    assert len(duplicate_rejected) == 1

    merged = deduplicated.iloc[0]
    assert merged["source"] == "Webinar"
    assert merged["raw_email"] == "JOHN@EXAMPLE.COM"
    assert merged["company"] == "Northwind Agency"
    assert merged["country"] == "Canada"


def test_deduplicate_leads_returns_empty_duplicate_rejections_when_no_duplicates() -> None:
    normalized = normalize_leads(prepare_input_dataframe(make_raw_dataframe()))
    accepted, _ = split_accepted_and_rejected(normalized)

    deduplicated, duplicate_rejected = deduplicate_leads(accepted)

    assert len(deduplicated) == 2
    assert duplicate_rejected.empty


def test_deduplicate_leads_prefers_more_complete_primary_row_before_merging() -> None:
    raw_df = pd.DataFrame(
        [
            {
                "Name": "Alex",
                "Email": "alex@example.com",
                "Title": "",
                "Company": "",
                "Phone": "",
                "Source": "Inbound Demo",
                "Country": "USA",
                "Created At": "2026-04-04",
            },
            {
                "Name": "Alex Johnson",
                "Email": "alex@example.com",
                "Title": "Studio Manager",
                "Company": "Creative House",
                "Phone": "555-000-1234",
                "Source": "Inbound Demo",
                "Country": "United States",
                "Created At": "2026-04-05",
            },
        ]
    )

    normalized = normalize_leads(prepare_input_dataframe(raw_df))
    accepted, _ = split_accepted_and_rejected(normalized)
    deduplicated, duplicate_rejected = deduplicate_leads(accepted)

    assert len(deduplicated) == 1
    assert len(duplicate_rejected) == 1

    merged = deduplicated.iloc[0]
    assert merged["full_name"] == "Alex Johnson"
    assert merged["first_name"] == "Alex"
    assert merged["last_name"] == "Johnson"
    assert merged["title"] == "Studio Manager"
    assert merged["company"] == "Creative House"