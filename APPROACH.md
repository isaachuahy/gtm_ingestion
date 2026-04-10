# Approach and Design Notes

## Goal

Build a lightweight lead-preparation pipeline for downstream Salesforce lead creation while staying appropriately scoped for a 2–3 hour exercise.

The objective is not to build a production platform. The objective is to show sound engineering judgment around:

- messy external data
- normalization and validation policy
- external enrichment boundaries
- interpretable lead scoring
- clean outputs and operational reporting

The primary objective is to maximize accepted rows without letting obviously low-quality records pollute downstream CRM workflows.

To support that goal, the pipeline keeps a single accepted dataset and tracks Salesforce publishability separately through a `salesforce_ready` flag.

The raw input contract for this exercise is a CSV with the columns `Name`, `Email`, `Title`, `Company`, `Phone`, `Source`, `Country`, and `Created At`.

## Scope decisions

I intentionally chose a local batch pipeline in Python using pandas because it provides the best signal-to-effort tradeoff for this exercise:

- fast CSV ingestion and transformation
- readable implementation
- easy summary reporting
- straightforward local execution

This is also an appropriate shape for the assumed operating pattern: low-volume runs, roughly hundreds to low thousands of rows at a time, on an hourly or ad hoc cadence.

I intentionally did **not** build:

- a web service
- a database-backed system
- a real enrichment HTTP mock server
- orchestration or queueing infrastructure

Those are reasonable production directions, but they would be mis-scoped for this assignment.

## Pipeline stages

### 1. Ingestion

Read the raw CSV, attach a `source_row_number` for traceability, and preserve the raw `Source` field for downstream provenance and possible Salesforce lead-source mapping.

### 2. Normalization

Normalize:

- names
- email
- title
- company
- phone
- source
- country
- dates

The goal is to produce a stable canonical schema without making speculative repairs to critical identity fields.
In practice, that means preserving the raw `Name` as `full_name`, splitting it into `first_name` and `last_name` on a best-effort basis, and normalizing noisy fields like phone and country conservatively.

### 3. Validation

A lead is considered minimally ingestible if it has a valid email.

Reasoning:

- email is the strongest operational identifier for lightweight Salesforce lead creation, deduplication, and enrichment within this scope
- maximizing accepted rows does not require perfect records, but it does require one stable identity anchor
- missing company or title lowers lead quality but does not make the lead unusable
- invalid contact identity fields should not be guessed

Rejected rows are retained with explicit `drop_reason` values.
They are intended to feed a manual remediation queue rather than being silently discarded.

This creates three practical states:

- `accepted`: the row passes the base validation policy and is retained in `clean_leads`
- `salesforce_ready`: the row is accepted and also meets the stricter downstream creation bar for this project, defined as `email + last_name + company`
- `rejected`: the row fails the base acceptance policy or is removed during deduplication with an explicit reason

I intentionally separate acceptance from Salesforce readiness so the pipeline can preserve useful but incomplete leads instead of forcing a binary publish-or-drop decision too early.

### 4. Deduplication

Accepted leads are deduplicated by normalized email.

When duplicates exist, I merge rows field-by-field where possible so the final accepted record preserves the most complete valid data.

If duplicate rows conflict on a field, I fall back to the row with the highest count of valid, non-empty fields after normalization.

### 5. Enrichment

Enrichment is modeled as an external dependency boundary through an `EnrichmentClient` abstraction.

The mock client simulates:

- a request payload
- deterministic provider logic
- a structured response

This keeps the pipeline realistic while avoiding unnecessary infrastructure.

### 6. Scoring

Lead scoring is rule-based and interpretable.

Rules are stored in `scoring_rules.json` so business logic can be adjusted without changing pipeline code.

Each score also includes `score_reasons` to make downstream review easier.

The scoring model is designed for prioritization rather than hard acceptance. For example, free email domains are penalized only mildly because they can still represent usable leads, especially when other fields are strong.

Enrichment failures do affect score because they reduce confidence in company-level context and make downstream prioritization less reliable.

### 7. Output

The pipeline writes:

- accepted records in a single cleaned dataset
- a `salesforce_ready` flag for downstream publishability
- rejected rows with reasons
- JSON output for flexibility
- a summary report with key quality and pipeline metrics

## Key assumptions

- Email is required for acceptance
- Missing company is allowed but penalized in scoring
- Free email domains are allowed but scored only slightly lower
- Phone parsing is intentionally conservative
- Country normalization should remain conservative because the input may be noisy or abbreviated in inconsistent ways
- Date parsing uses best-effort normalization to ISO format
- Rejected rows should be retained for manual remediation and possible reprocessing
- The pipeline is intended for low-volume batch usage rather than high-scale streaming ingestion
- Salesforce readiness is stricter than acceptance and is defined in this project as `email + last_name + company`

## What I would change in production

If this were moved into a real GTM engineering workflow, I would likely add:

- provider-backed enrichment with retries and rate-limit handling
- explicit raw, clean, enriched, and published dataset separation
- configurable field mappings for Salesforce
- better observability and run metrics
- validation and normalization rules stored outside code
- stronger phone and address normalization using dedicated libraries or services
