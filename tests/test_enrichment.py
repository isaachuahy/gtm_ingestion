import pytest

from enrichment import (
    EnrichmentRequest,
    EnrichmentResponse,
    MockEnrichmentClient,
    TransientEnrichmentError,
    enrich_with_retry,
)


def make_request(
    *,
    email: str | None = "jane@example.com",
    company: str | None = "Acme Studio",
    full_name: str | None = "Jane Doe",
    title: str | None = "Creative Operations Manager",
    country: str | None = "United States",
) -> EnrichmentRequest:
    return EnrichmentRequest(
        email=email,
        company=company,
        full_name=full_name,
        title=title,
        country=country,
    )


def test_mock_enrichment_client_returns_not_found_when_email_missing() -> None:
    client = MockEnrichmentClient()

    response = client.enrich(make_request(email=None))

    assert response.industry is None
    assert response.company_size is None
    assert response.company_domain is None
    assert response.enrichment_status == "not_found"
    assert response.provider_name == "ZoomInfoTest"
    assert response.provider_record_found is False
    assert response.error_message == "missing_email"


def test_mock_enrichment_client_returns_enriched_fields_for_agency_like_company() -> None:
    client = MockEnrichmentClient()

    response = client.enrich(make_request(company="Acme Studio"))

    assert response.industry == "agency"
    assert response.company_size == "mid_market"
    assert response.company_domain == "acmestudio.com"
    assert response.enrichment_status == "enriched"
    assert response.provider_name == "ZoomInfoTest"
    assert response.provider_record_found is True
    assert response.error_message is None
    assert response.raw_payload["matched_on"]["company"] == "Acme Studio"


def test_mock_enrichment_client_uses_email_domain_when_company_missing() -> None:
    client = MockEnrichmentClient()

    response = client.enrich(make_request(company=None, title="Analyst"))

    assert response.industry == "unknown"
    assert response.company_size == "small_business"
    assert response.company_domain == "example.com"
    assert response.enrichment_status == "enriched"
    assert response.provider_record_found is True


def test_mock_enrichment_client_raises_transient_error_for_air_inc() -> None:
    client = MockEnrichmentClient()

    with pytest.raises(TransientEnrichmentError, match="mock_transient_provider_error"):
        client.enrich(make_request(company="air inc"))


def test_enrich_with_retry_retries_transient_error_then_returns_success() -> None:
    # FlakyClient is a test client that simulates a transient failure on the first attempt, then succeeds on the second attempt. 
    # This allows us to test that enrich_with_retry properly retries after a transient error and eventually returns a successful 
    # response without raising an exception to the caller.
    class FlakyClient:
        provider_name = "FlakyProvider"

        def __init__(self) -> None:
            self.attempts = 0

        def enrich(self, request: EnrichmentRequest) -> EnrichmentResponse:
            self.attempts += 1
            if self.attempts == 1:
                raise TransientEnrichmentError("temporary_provider_issue")

            return EnrichmentResponse(
                industry="media",
                company_size="mid_market",
                company_domain="acme.com",
                enrichment_status="enriched",
                provider_name=self.provider_name,
                provider_record_found=True,
            )

    client = FlakyClient()

    response = enrich_with_retry(client, make_request(), max_attempts=3)

    assert client.attempts == 2
    assert response.enrichment_status == "enriched"
    assert response.error_message is None
    assert response.provider_name == "FlakyProvider"
    assert response.provider_record_found is True


def test_enrich_with_retry_returns_failed_response_after_retry_exhaustion() -> None:
    client = MockEnrichmentClient()
    request = make_request(company="air inc")

    response = enrich_with_retry(client, request, max_attempts=2)

    assert response.industry is None
    assert response.company_size is None
    assert response.company_domain is None
    assert response.enrichment_status == "failed"
    assert response.provider_name == "ZoomInfoTest"
    assert response.provider_record_found is False
    assert response.error_message == "mock_transient_provider_error"


def test_enrich_with_retry_requires_at_least_one_attempt() -> None:
    with pytest.raises(ValueError, match="max_attempts must be at least 1"):
        enrich_with_retry(MockEnrichmentClient(), make_request(), max_attempts=0)
