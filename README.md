# Air GTM Ingestion

Lightweight batch pipeline for preparing messy lead data for downstream Salesforce ingestion.

## What it does

- ingests raw CSV lead data
- normalizes and validates key fields
- deduplicates accepted leads by email
- enriches accepted leads through a mocked provider client
- scores leads with configurable business rules
- produces cleaned outputs, rejected rows, and a summary report
- prepares a Salesforce-aligned export view

## Repository structure

```text
gtm_ingestion/
├── main.py              # thin entrypoint: load, run, write
├── pipeline.py          # pipeline orchestration
├── enrichment.py        # enrichment request/response models, client interface, mock provider, retry logic
├── salesforce.py        # Salesforce export mapping
├── scoring_rules.json   # configurable scoring rules
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

## Processing stages

1. prepare input schema
2. normalize fields
3. validate minimum acceptance requirements
4. deduplicate by normalized email
5. enrich accepted leads through a mocked enrichment client
6. score leads using `scoring_rules.json`
7. finalize clean output and Salesforce export

## Acceptance rules

A row is accepted if it has a valid email.

A row is marked `salesforce_ready` if it has:

- `email`
- `last_name`
- `company`

Rejected rows are retained with a `drop_reason`.

## Enrichment

The enrichment stage is provider-shaped but mocked.

- pipeline builds an enrichment request per accepted lead
- a mock client simulates a third-party API request and response
- at least these enriched fields are added:
  - `industry`
  - `company_size`
  - `company_domain`
  - `enrichment_status`
- retry/error handling is applied around enrichment calls

## Outputs

### `clean_leads.csv` / `clean_leads.json`
Accepted leads with normalized, enriched, and scored fields.

Core fields:

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
Original rejected or duplicate rows plus:

- `source_row_number`
- `drop_reason`

### `summary_report.json`
Run-level metrics such as:

- raw rows
- accepted rows
- rejected rows
- duplicate rows merged
- Salesforce-ready count
- enrichment success/failure counts

### `salesforce_export.csv`
Salesforce-aligned export view for bulk ingestion.

Typical mappings:

- `first_name` -> `FirstName`
- `last_name` -> `LastName`
- `email` -> `Email`
- `company` -> `Company`
- `title` -> `Title`
- `phone` -> `Phone`
- `source` -> `LeadSource`
- enriched/scoring fields -> custom fields such as `Industry__c`, `Company_Size__c`, `Lead_Score__c`

## How to run

Install dependencies:

```bash
pip install -r requirements.txt
```

Run the pipeline:

```bash
python main.py
```

Outputs are written to `outputs/`.

## Notes

- designed for local batch use, not high-scale streaming
- enrichment is mocked for the exercise; provider clients can be swapped later
- pipeline keeps accepted leads even when enrichment fails
