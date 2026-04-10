# Air GTM Engineering Challenge

## Overview

This project implements a lightweight batch pipeline that ingests messy lead data from CSV, normalizes key fields, applies mock enrichment, scores leads using configurable business rules, and outputs CRM-ready files for downstream Salesforce ingestion.

The solution is intentionally scoped for the exercise timebox: simple enough to run locally, but structured to reflect realistic GTM data preparation stages.

## What the pipeline does

1. Ingests raw lead data from `messy_leads.csv`
2. Normalizes names, emails, titles, phone numbers, countries, and dates
3. Rejects unusable records based on explicit validation rules
4. Deduplicates accepted leads by normalized email
5. Enriches accepted leads via a mocked enrichment client
6. Scores leads using configurable rules from `scoring_rules.json`
7. Writes cleaned outputs and a summary report

## Files

- `main.py` — entrypoint
- `pipeline.py` — core pipeline logic
- `scoring_rules.json` — scoring configuration
- `outputs/clean_leads.csv` — cleaned CRM-ready leads
- `outputs/clean_leads.json` — cleaned leads as JSON
- `outputs/rejected_leads.csv` — rejected or duplicate rows with drop reasons
- `outputs/summary_report.json` — aggregate pipeline summary

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

- A lead must have a valid email to be considered CRM-ingestible
- Missing company, phone, or title does not automatically invalidate a lead
- Invalid or missing email results in rejection
- Duplicate accepted leads are deduplicated by normalized email

### Normalization

- Emails are lowercased, whitespace-trimmed, and validated
- Names, titles, and companies are whitespace-normalized and title-cased
- Countries are mapped to canonical names where possible
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

## Output schema

The cleaned output is designed to be easy to map into Salesforce-style lead ingestion workflows and includes:

- `first_name`
- `last_name`
- `full_name`
- `email`
- `title`
- `company`
- `phone`
- `country`
- `lead_date`
- `industry`
- `company_size`
- `company_domain`
- `enrichment_status`
- `lead_score`
- `score_reasons`

Rejected output includes the original row plus:

- `source_row_number`
- `drop_reason`

## Tradeoffs and limitations

This implementation was intentionally scoped for the exercise rather than built as a production service.

Not included:

- real external enrichment provider integration
- retry/backoff logic
- persistent storage
- workflow orchestration
- advanced observability
- full international phone parsing

## How I would productionize this

With more time, I would extend this into a more robust batch service by:

- integrating a real enrichment provider behind the same client interface
- adding retry, timeout, and rate-limit handling
- persisting raw, cleaned, enriched, and published datasets separately
- adding structured logging and pipeline metrics
- supporting configurable field mappings for Salesforce Bulk API payload generation
- externalizing validation and normalization policies for easier non-code updates
