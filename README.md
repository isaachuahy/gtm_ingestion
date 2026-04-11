# Air GTM Ingestion

Lightweight batch pipeline for preparing messy lead CSV data for downstream Salesforce ingestion.

## What it does

- ingests raw CSV lead data
- normalizes names, emails, phones, countries, and dates
- rejects rows without a valid email
- deduplicates accepted leads by normalized email
- enriches retained leads through a mocked provider client with retry handling
- scores leads with configurable business rules from `scoring_rules.json`
- keeps accepted leads in `clean_leads.csv`, including rows that are not yet Salesforce-ready
- builds a separate Salesforce export view from only `salesforce_ready` leads

## Repository structure

```text
gtm_ingestion/
├── main.py              # thin entrypoint: load, run, write
├── pipeline.py          # pipeline orchestration and transformation stages
├── enrichment.py        # enrichment request/response models, mock provider, retry logic
├── salesforce.py        # Salesforce export mapping
├── scoring_rules.json   # configurable lead scoring rules
├── country_mappings.json
├── messy_leads.csv      # sample input
├── README.md
├── APPROACH.md
└── tests/
```

## Input

The pipeline expects a CSV with these columns:

- `Name`
- `Email`
- `Title`
- `Company`
- `Phone`
- `Source`
- `Country`
- `Created At`


## How to run

Create and activate a virtual environment:

```bash
python -m venv .venv
source .venv/bin/activate
```

Install dependencies:

```bash
pip install -r requirements.txt
```

Run the pipeline:

```bash
python main.py
```

Outputs are written to `outputs/`.


## Assumptions

- input arrives as a single local CSV with a fixed column contract and is small enough to process in memory with pandas
- normalized email is the primary acceptance and deduplication key for this exercise
- `salesforce_ready` is intentionally narrower than `accepted`
- accepted leads may still be useful even if they need remediation before Salesforce import
- enrichment is mocked and deterministic, so the pipeline focuses on integration shape rather than provider accuracy
- email is sufficient for enrichment so transcient errors are handled. But did not handle case if provider does not have enriched information
- scoring rules in `scoring_rules.json` represent current business priorities, and rule order is intentional for first-match categories
- Salesforce custom field names shown here are illustrative but concrete enough to demonstrate export mapping shape

## How To Scale / Productionize

### Data Input

- move from a single local CSV to configurable input sources such as S3, GCS, or a managed upload endpoint
- add stronger schema validation, contract versioning, and file-level quality checks before processing begins
- support multiple input batches with run IDs and idempotent reprocessing

### Latency And Throughput

- keep the current batch design for periodic lead imports where end-to-end latency is measured in minutes, not milliseconds
- if volume grows, batch enrichment calls where the provider allows it and add concurrency with rate-limit awareness
- add caching for repeated company/domain lookups to reduce unnecessary provider calls

### Batch Vs Streaming

- batch is the right default for this assignment because the input is file-based and the downstream system is a CRM bulk import workflow
- move to streaming only if there is a real near-real-time requirement such as immediate sales routing or live lead qualification
- if streaming becomes necessary, use a queue or event bus and keep normalization, enrichment, scoring, and export as separate workers

### Database And Storage

- persist raw rows, normalized rows, enrichment attempts, scored rows, rejected rows, and export artifacts in durable storage
- store run metadata and rule versions so outputs are auditable and reproducible
- add a relational database or warehouse for reporting, remediation workflows, and downstream analytics

### Provider And Retry Resilience

- replace the mock client with real provider adapters behind the same request/response boundary
- add structured retries with backoff, timeout handling, and provider-specific error classification
- capture failed enrichments for replay instead of relying only on in-memory retry

### Salesforce Integration

- replace CSV-only export with Salesforce Bulk API integration
- externalize field mappings so org-specific schema changes do not require code changes
- capture sync status, failed records, and retryable export errors separately from transformation logic

### Observability And Operations

- add structured logging, metrics, and alerting around rejection rates, enrichment success rates, and scoring distribution
- surface data quality reports so unusual drops in readiness or enrichment can be investigated quickly
- add automated regression tests for representative messy input samples and contract-level output checks

## Notes

- designed for local batch use, not high-scale streaming
- enrichment is mocked for the exercise and can be replaced with real providers later
- clean lead retention and Salesforce export are intentionally treated as separate concerns

## Processing stages

1. prepare input schema
2. normalize fields
3. accept or reject rows based on email validity
4. deduplicate accepted rows by normalized email
5. enrich retained leads through a mocked enrichment boundary
6. score leads using `scoring_rules.json`
7. finalize the clean lead schema and compute `salesforce_ready`
8. build a summary report and Salesforce export

## Acceptance and retention rules

A row is accepted if it has a valid normalized email.

A row is marked `salesforce_ready` if it has:

- `email`
- `last_name`
- `company`

Accepted leads that are not `salesforce_ready` are still kept in `clean_leads.csv` for remediation or follow-up. They are not treated as rejected rows.

Rejected rows are only rows dropped during validation or deduplication, and they are retained with a `drop_reason`.

## Enrichment

The enrichment layer is provider-shaped but mocked.

- the pipeline builds an `EnrichmentRequest` per retained lead
- `MockEnrichmentClient` simulates a third-party provider response
- the current enrichment response includes:
  - `industry`
  - `company_size`
  - `company_domain`
  - `enrichment_status`
    one of:
    - `enriched`
    - `not_found`
    - `failed`
- retry handling is applied around transient provider errors
- enrichment failure does not reject an otherwise accepted lead

## Scoring

Lead scores are driven by `scoring_rules.json`.

The current implementation:

- starts from `base_score`
- adds one title-function match
- adds one seniority match
- adds any configured `industry` and `company_size` points
- subtracts penalties such as free email domain, missing company, and failed enrichment
- clamps the final score to the configured min/max bounds

Within `title_function_scores` and `seniority_scores`, the first matching rule in config order wins. 
In practice, that means rule order defines priority inside each category.

## Outputs

### `clean_leads.csv`

Accepted leads with normalized, enriched, and scored fields.

Core fields:

- `source_row_number`
- `first_name`
- `last_name`
- `full_name`
- `email`
- `title`
- `company`
- `phone`
- `source`
- `country`
- `lead_date`
- `industry`
- `company_size`
- `company_domain`
- `enrichment_status`
- `lead_score`
- `score_reasons`
- `salesforce_ready`

### `rejected_leads.csv`

Rejected or deduplicated rows plus:

- `source_row_number`
- `drop_reason`

### `summary_report.json`

The summary report currently writes these concrete keys:

- `raw_rows`
- `clean_rows`
- `rejected_rows`
- `salesforce_ready_rows`
- `clean_but_not_salesforce_ready_rows`
- `average_lead_score`
- `percent_enriched_from_raw`
- `enrichment_status_counts`
- `rejection_reason_counts`

### `salesforce_export.csv`

Salesforce export defaults to only `salesforce_ready` rows.

Current field mapping:

- `first_name` -> `FirstName`
- `last_name` -> `LastName`
- `email` -> `Email`
- `company` -> `Company`
- `title` -> `Title`
- `phone` -> `Phone`
- `source` -> `LeadSource`
- `country` -> `Country`
- `industry` -> `Industry__c`
- `company_size` -> `Company_Size__c`
- `company_domain` -> `Company_Domain__c`
- `lead_score` -> `Lead_Score__c`
- `score_reasons` -> `Score_Reasons__c`
- `lead_date` -> `Lead_Date__c`
- `source_row_number` -> `Source_Row_Number__c`

`score_reasons` is stored internally as a list and serialized into a semicolon-delimited string in the Salesforce export.

