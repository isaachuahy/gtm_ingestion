from __future__ import annotations

"""Provider-shaped enrichment boundary for the GTM ingestion pipeline.

This module keeps provider request/response models, a mock client, and
retry policy separate from pipeline orchestration.
"""

from dataclasses import dataclass, field
from typing import Any, Protocol

class EnrichmentError(Exception):
    """Base enrichment failure."""

class TransientEnrichmentError(EnrichmentError):
    """Failure that may succeed on a later retry."""

# enrichment_status can be "enriched", "not_found", or "failed"

@dataclass(slots=True)
class EnrichmentRequest:
    """
    The enrichment request model. 
    Fields should be populated with as much information as possible to maximize the chances of a successful enrichment.
    """
    email: str | None
    company: str | None
    full_name: str | None
    title: str | None
    country: str | None
    source_row_number: int | None = None


@dataclass(slots=True)
class EnrichmentResponse:
    """
    The enrichment response model.
    """
    industry: str | None
    company_size: str | None
    company_domain: str | None
    enrichment_status: str
    provider_name: str
    provider_record_found: bool
    error_message: str | None = None
    raw_payload: dict[str, Any] = field(default_factory=dict)


class EnrichmentClient(Protocol):
    def enrich(self, request: EnrichmentRequest) -> EnrichmentResponse:
        """
        Run a single provider enrichment attempt.
        """

# We would create MockEnrichmentClients for each provider
@dataclass(slots=True)
class MockEnrichmentClient:
    """Deterministic offline enrichment client for local development."""

    provider_name: str = "ZoomInfoTest"

    def enrich(self, request: EnrichmentRequest) -> EnrichmentResponse:
        company = normalize_text(request.company)
        email = normalize_text(request.email)
        title = normalize_text(request.title)

        if not company:
            return EnrichmentResponse(
                industry=None,
                company_size=None,
                company_domain=None,
                enrichment_status="not_found",
                provider_name=self.provider_name,
                provider_record_found=False,
                error_message="missing_company",
                raw_payload={},
            )
        
        # From here, this is just simulating business logic behind external enrichment APIs

        # We'll make "air inc" always fail with a transient error for fun to test our retry logic in the pipeline
        if company == "air inc":
            raise TransientEnrichmentError("mock_transient_provider_error")

        company_lower = (company or "").lower()
        title_lower = (title or "").lower()

        if company:
            company_domain = f"{''.join(character for character in company_lower if character.isalnum())}.com"
        elif email and "@" in email:
            company_domain = email.split("@", 1)[1].lower()
        else:
            company_domain = None

        if "agency" in company_lower or "studio" in company_lower:
            industry = "agency"
        elif "media" in company_lower or "publisher" in company_lower:
            industry = "media"
        elif "software" in company_lower or "saas" in company_lower:
            industry = "software"
        elif "creative" in title_lower or "brand" in title_lower:
            industry = "media"
        else:
            industry = "unknown"

        if any(word in company_lower for word in ("global", "international", "systems")):
            company_size = "enterprise"
        elif any(word in company_lower for word in ("labs", "media", "agency", "studio")):
            company_size = "mid_market"
        else:
            company_size = "small_business"

        return EnrichmentResponse(
            industry=industry,
            company_size=company_size,
            company_domain=company_domain,
            enrichment_status="enriched",
            provider_name=self.provider_name,
            provider_record_found=True,
            raw_payload={
                "matched_on": {
                    "company": company,
                    "email": email,
                    "title": title,
                }
            },
        )


def enrich_with_retry(
    client: EnrichmentClient,
    request: EnrichmentRequest,
    max_attempts: int = 3,
) -> EnrichmentResponse:
    """
    Run enrichment with retries on transient failures.
    """
    if max_attempts < 1:
        raise ValueError("max_attempts must be at least 1")

    # Keep the last retryable error so we can surface a useful failure message
    # if all transient attempts are exhausted.
    last_error: TransientEnrichmentError | None = None

    for _ in range(max_attempts):
        try:
            return client.enrich(request)
        except TransientEnrichmentError as error:
            last_error = error

    # We can differentiate between a definitive "not found" (where the provider returns a valid response indicating no match) 
    # and a failure to enrich due to transient errors (where we exhausted all retries without success).

    # By default, return a failed enrichment response if all attempts fail, without raising an exception to the caller -
    # I did this because I don't want it to catastrophically fail the entire pipeline if enrichment fails
    return EnrichmentResponse(
        industry=None,
        company_size=None,
        company_domain=None,
        enrichment_status="failed",
        provider_name=getattr(client, "provider_name", "unknown_provider"),
        provider_record_found=False,
        error_message=str(last_error) if last_error else "unknown_enrichment_failure",
        raw_payload={},
    )


def normalize_text(value: Any) -> str | None:
    if value is None:
        return None

    text = str(value).strip()
    return text or None
