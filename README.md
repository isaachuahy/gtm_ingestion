# Air GTM Engineering Challenge

## Overview

This project implements a lightweight batch pipeline that ingests messy lead data from CSV, normalizes key fields, applies mock enrichment, scores leads using configurable business rules, and outputs Salesforce-aligned files for downstream lead creation.

For this exercise, the raw input CSV uses the following columns: `Name`, `Email`, `Title`, `Company`, `Phone`, `Source`, `Country`, and `Created At`.

The solution is intentionally scoped for the exercise timebox: simple enough to run locally, but structured to reflect realistic GTM data preparation stages.

The primary objective is to maximize accepted rows while avoiding obviously low-quality records that would create poor downstream CRM data.

Accepted rows are written to a single `clean_leads` dataset. Salesforce publishability is tracked separately via a `salesforce_ready` flag so accepted leads can still be retained even when they need additional remediation before creation in Salesforce.

## What the pipeline does

1. Ingests raw lead data from `messy_leads.csv`
2. Normalizes names, emails, titles, phone numbers, source values, countries, and dates
3. Rejects unusable records based on explicit validation rules
4. Deduplicates accepted leads by normalized email and merges duplicates field-by-field
5. Enriches accepted leads via a mocked enrichment client
6. Scores leads using configurable rules from `scoring_rules.json`
7. Flags whether each accepted lead is ready for downstream Salesforce creation
8. Writes cleaned outputs and a summary report

## Input contract

The current pipeline is designed around a fixed input schema from `messy_leads.csv`:

| Raw column | Canonical meaning | Notes |
| --- | --- | --- |
| `Name` | `full_name` | Split on a best-effort basis into `first_name` and `last_name` |
| `Email` | `email` | Required for acceptance |
| `Title` | `title` | Used in scoring |
| `Company` | `company` | Used for readiness, enrichment, and scoring |
| `Phone` | `phone` | Normalized conservatively |
| `Source` | `source` | Normalized conservatively |
| `Country` | `country` | Normalized conservatively |
| `Created At` | `lead_date` | Normalized to ISO date where parseable |

## Files

- `main.py` — entrypoint
- `pipeline.py` — core pipeline logic
- `scoring_rules.json` — scoring configuration
- `outputs/clean_leads.csv` — sample generated accepted leads output for demonstration
- `outputs/clean_leads.json` — sample generated JSON output for demonstration
- `outputs/rejected_leads.csv` — sample generated rejected or duplicate rows output for demonstration
- `outputs/summary_report.json` — sample generated aggregate report for demonstration

## Repository Structure
```text
gtm_ingestion/
├── README.md
├── APPROACH.md
├── requirements.txt
├── messy_leads.csv
├── scoring_rules.json
├── main.py
├── pipeline.py
├── outputs/                  # sample generated outputs retained for demonstration
└── tests/
    └── test_pipeline.py
```

## How to run

### Install dependencies

```bash
pip install -r requirements.txt
```

### Run the pipeline

```bash
python main.py
```

Outputs will be written to the `outputs/` directory.

## Assumptions

### Validation

- A lead must have a valid email to be considered accepted for downstream Salesforce lead creation
- Missing company, phone, or title does not automatically invalidate a lead
- Invalid or missing email results in rejection
- Email is the only hard acceptance criterion in this exercise because it is the strongest identity anchor for deduplication and lightweight enrichment within scope

### Deduplication

- Accepted leads are deduplicated by normalized email
- Duplicate rows are merged field-by-field where possible so the final record retains the most complete valid data
- If duplicate rows conflict, the pipeline falls back to the row with the highest count of valid, non-empty fields after normalization
- Rejected duplicate rows are retained with explicit drop reasons for traceability

### Salesforce readiness

- Acceptance and Salesforce readiness are intentionally separated
- `clean_leads` contains all accepted records under the pipeline's validation policy
- `salesforce_ready` indicates whether an accepted record meets the stricter downstream bar for lead creation under this project's assumed Salesforce contract
- In this project, a lead is considered `salesforce_ready` when it has a valid `email`, a non-empty `last_name`, and a non-empty `company`
- Rows that are accepted but not Salesforce-ready should remain available for remediation rather than being dropped

### Normalization

- Emails are lowercased, whitespace-trimmed, and validated
- Raw `Name` values are preserved as `full_name` and split into `first_name` and `last_name` on a best-effort basis
- Titles, companies, and sources are whitespace-normalized
- Countries are normalized conservatively rather than aggressively remapped
- Dates are converted to ISO format (`YYYY-MM-DD`) where parseable
- Phone normalization is intentionally conservative and does not attempt full international parsing without reliable context

### Enrichment

- Enrichment is mocked in-process via `MockEnrichmentClient`
- The client simulates request construction and deterministic responses based on email domain or company name
- Enrichment returns fields such as `industry`, `company_size`, and `company_domain`

### Scoring

Lead scores are rule-based and configurable in `scoring_rules.json`.

Scoring currently considers:

- title function keywords
- seniority keywords
- company size
- industry bonuses
- penalties for free email domains, missing company, and failed enrichment

Free email domains are penalized only mildly. They can still produce usable leads, but they are generally weaker signals when paired with sparse or low-confidence company data.

### Operational assumptions

- The pipeline is intended for low-volume batch processing, roughly hundreds to low thousands of rows per run
- A reasonable operating cadence is hourly or ad hoc batch execution
- Rejected rows are not discarded; they are intended to feed a remediation queue for manual review and possible reprocessing

## Output schema

The cleaned output is designed to be easy to map into Salesforce-style lead ingestion workflows.

The table below describes the logical output schema. Structured values may be serialized differently between CSV and JSON outputs.

| Field | Type | Nullable | Mapping |
| --- | --- | --- | --- |
| `first_name` | `string` | yes | Salesforce-aligned |
| `last_name` | `string` | yes | Salesforce-aligned |
| `full_name` | `string` | yes | Pipeline metadata |
| `email` | `string` | no | Salesforce-aligned |
| `title` | `string` | yes | Salesforce-aligned |
| `company` | `string` | yes | Salesforce-aligned |
| `phone` | `string` | yes | Salesforce-aligned |
| `source` | `string` | yes | Salesforce-aligned |
| `country` | `string` | yes | Salesforce-aligned |
| `lead_date` | `date string` | yes | Pipeline metadata |
| `industry` | `string` | yes | Salesforce-aligned |
| `company_size` | `string` | yes | Custom or enrichment-derived |
| `company_domain` | `string` | yes | Custom or enrichment-derived |
| `enrichment_status` | `string` | no | Pipeline metadata |
| `lead_score` | `integer` | no | Custom or pipeline metadata |
| `score_reasons` | `list[string]` | no | Pipeline metadata |
| `salesforce_ready` | `boolean` | no | Pipeline metadata |

Notes:

- `Salesforce-aligned` means the field is easy to map onto a Salesforce Lead record, but exact required fields still depend on the target Salesforce org configuration
- `source` is likely candidate for mapping onto Salesforce `LeadSource`
- `company_size` may map to a standard Salesforce field such as employee count if represented numerically, or to a custom field if represented categorically
- `lead_score`, `score_reasons`, `company_domain`, and `enrichment_status` are best treated as pipeline metadata or custom CRM fields rather than assumed standard Salesforce lead fields
- `score_reasons` is a logical list type and may be serialized as a string in CSV output
- `salesforce_ready` should be computed from a stricter downstream readiness policy than the base acceptance rule, defined here as `email + last_name + company`

Rejected output includes the original row plus:

- `source_row_number`
- `drop_reason`

Rejected rows are retained for manual remediation rather than silently discarded.

## Tradeoffs and limitations

This implementation was intentionally scoped for the exercise rather than built as a production service.

Not included:

- real external enrichment provider integration
- retry/backoff logic
- persistent storage
- workflow orchestration
- advanced observability
- full international phone parsing
- Salesforce org-specific field mappings or custom-object configuration

## How I would productionize this

With more time, I would extend this into a more robust batch service by:

- integrating a real enrichment provider behind the same client interface
- adding retry, timeout, and rate-limit handling
- persisting raw, cleaned, enriched, and published datasets separately
- adding structured logging and pipeline metrics
- supporting configurable field mappings for Salesforce Bulk API payload generation
- externalizing validation and normalization policies for easier non-code updates
