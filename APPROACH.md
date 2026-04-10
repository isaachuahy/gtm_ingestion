# Approach and Design Notes

## Goal

Build a lightweight lead-preparation pipeline that is realistic for GTM operations while staying appropriately scoped for a 2–3 hour exercise.

The objective is not to build a production platform. The objective is to show sound engineering judgment around:

- messy external data
- normalization and validation policy
- external enrichment boundaries
- interpretable lead scoring
- clean outputs and operational reporting

## Scope decisions

I intentionally chose a local batch pipeline in Python using pandas because it provides the best signal-to-effort tradeoff for this exercise:

- fast CSV ingestion and transformation
- readable implementation
- easy summary reporting
- straightforward local execution

I intentionally did **not** build:

- a web service
- a database-backed system
- a real enrichment HTTP mock server
- orchestration or queueing infrastructure

Those are reasonable production directions, but they would be mis-scoped for this assignment.

## Pipeline stages

### 1. Ingestion

Read the raw CSV and attach a `source_row_number` for traceability.

### 2. Normalization

Normalize:

- names
- email
- title
- company
- phone
- country
- dates

The goal is to produce a stable canonical schema without making speculative repairs to critical identity fields.

### 3. Validation

A lead is considered minimally ingestible if it has a valid email.

Reasoning:

- email is the strongest operational identifier for CRM lead creation and deduplication
- missing company or title lowers lead quality but does not make the lead unusable
- invalid contact identity fields should not be guessed

Rejected rows are retained with explicit `drop_reason` values.

### 4. Deduplication

Accepted leads are deduplicated by normalized email.

When duplicates exist, I keep the most complete row based on the number of populated non-critical fields.

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

### 7. Output

The pipeline writes:

- accepted CRM-ready records
- rejected rows with reasons
- JSON output for flexibility
- a summary report with key quality and pipeline metrics

## Key assumptions

- Email is required for acceptance
- Missing company is allowed but penalized in scoring
- Free email domains are allowed but scored lower
- Phone parsing is intentionally conservative
- Country normalization uses simple alias mapping
- Date parsing uses best-effort normalization to ISO format

## What I would change in production

If this were moved into a real GTM engineering workflow, I would likely add:

- provider-backed enrichment with retries and rate-limit handling
- explicit raw, clean, enriched, and published dataset separation
- configurable field mappings for Salesforce
- better observability and run metrics
- validation and normalization rules stored outside code
- stronger phone and address normalization using dedicated libraries or services
