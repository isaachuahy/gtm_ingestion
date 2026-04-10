import pandas as pd
import pytest

from pipeline import normalize_leads, prepare_input_dataframe


def make_raw_dataframe() -> pd.DataFrame:
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
        "email",
        "title",
        "company",
        "phone",
        "source",
        "country",
        "lead_date",
    ]

    assert prepared.loc[0, "full_name"] == "Jane Doe"
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
    assert normalized.loc[0, "email"] == "jane@example.com"
    assert normalized.loc[0, "title"] == "Creative Operations Manager"
    assert normalized.loc[0, "company"] == "Acme Studio"
    assert normalized.loc[0, "source"] == "Inbound Demo"
    assert normalized.loc[0, "country"] == "USA"


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


def test_normalize_leads_invalid_email_becomes_null() -> None:
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
            }
        ]
    )

    normalized = normalize_leads(prepare_input_dataframe(raw_df))

    assert pd.isna(normalized.loc[0, "email"])
