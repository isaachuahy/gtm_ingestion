# Approach

## Goal

Build a small but realistic GTM lead-preparation pipeline that shows:

- ability to wrangle messy external data
- familiarity with enrichment workflows and APIs, even when mocked
- clear downstream preparation for Salesforce ingestion
- sound engineering structure without overbuilding

## Why this architecture

I split the repository by responsibility:

- `main.py`: execution entrypoint
- `pipeline.py`: end-to-end pipeline orchestration
- `enrichment.py`: provider boundary, mock client, request/response models, retry behavior
- `salesforce.py`: export mapping for downstream ingestion

This keeps file I/O and runtime setup separate from reusable transformation logic.

## Core design decisions

### 1. Batch pipeline over service

This assignment is better served by a local batch pipeline than a web app or database-backed system.

Reasoning:

- lower implementation overhead
- easier to run and test locally
- appropriate for low-volume CSV ingestion
- enough surface area to demonstrate engineering judgment

### 2. Acceptance separated from Salesforce readiness

I keep two thresholds:

- accepted: valid email exists
- `salesforce_ready`: email + last name + company

This preserves potentially useful leads instead of forcing an early publish-or-drop decision.

### 3. Enrichment modeled as an external dependency boundary

The enrichment stage is intentionally designed to look like a real provider integration.

Internally, the pipeline creates an `EnrichmentRequest` and expects an `EnrichmentResponse`.
A mock client then simulates a provider such as ZoomInfo or Clearbit.

This is more representative than hardcoding extra columns directly in the pipeline.

### 4. Mock both request and response

The assignment asks for API familiarity, so the mock does not just infer fields locally and stop there.
It simulates:

- request construction
- provider-shaped response parsing
- deterministic enrichment behavior
- failure cases

This gives the repo the shape of a real enrichment workflow while staying offline.

### 5. Retry logic kept separate from provider implementation

Retry behavior is an operational policy, not provider-specific business logic.
So the client handles a single enrichment attempt, while retry/error handling wraps the client.

This makes it easier to swap providers without rewriting resilience behavior in each client.

### 6. Salesforce export is explicit

I treat “clean lead retention” and “Salesforce export mapping” as separate concerns.

The pipeline keeps accepted leads for remediation and analysis, then builds a Salesforce-aligned export view for bulk ingestion.
This makes the downstream integration story clearer.

## Tradeoffs

Intentionally included:

- provider-style enrichment abstraction
- deterministic mock enrichment
- retry/error handling
- Salesforce-aligned export mapping
- concise batch-oriented structure

Intentionally not included:

- real third-party API integration
- persistent storage
- async/concurrent processing
- workflow orchestration
- org-specific Salesforce schema management
- advanced observability

## Production extensions

With more time, I would extend this by:

- implementing real provider clients behind the same enrichment interface
- adding rate-limit handling, timeouts, and structured logging
- supporting batch enrichment endpoints where providers allow it
- externalizing more validation and mapping rules into config
