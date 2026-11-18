"""SEC EDGAR API client.

Docs: https://www.sec.gov/search-filings/edgar-application-programming-interfaces

SEC rules: User-Agent header ("Company Name/email") required; max 10 req/s.
"""

import time

import requests

_MAX_ATTEMPTS = 6
_TIMEOUT_SEC = 30
_RETRYABLE_STATUSES = (403, 429, 500, 502, 503)



class SECClientError(RuntimeError):
    """A SEC EDGAR request failed after retries or returned bad JSON."""


class SECClient:
    """HTTP client for the structured SEC EDGAR JSON APIs.

    Endpoints: company_tickers.json (companies), submissions (filings),
    companyfacts (XBRL facts), frames (one concept across companies).
    Methods return the parsed JSON payload.
    """

    TICKERS_URL = "https://www.sec.gov/files/company_tickers.json"
    SUBMISSIONS_URL = "https://data.sec.gov/submissions/CIK{cik}.json"
    COMPANYFACTS_URL = "https://data.sec.gov/api/xbrl/companyfacts/CIK{cik}.json"
    FRAMES_URL = "https://data.sec.gov/api/xbrl/frames/{taxonomy}/{tag}/{unit}/{period}.json"

    def __init__(self, user_agent: str, max_requests_per_sec: int = 8):
        """Args:
            user_agent: "Company Name admin@email.com" (SEC policy).
            max_requests_per_sec: limit below the SEC max of 10 req/s.
        """
        if not user_agent or not user_agent.strip():
            raise ValueError(
                "A User-Agent in the format 'Company Name admin@email.com' is "
                "required by SEC policy."
            )
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": user_agent,
            "Accept-Encoding": "gzip, deflate",
        })
        self._min_interval = 1.0 / max_requests_per_sec
        self._last_request_ts = 0.0

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    @staticmethod
    def format_cik(cik: int | str) -> str:
        """Format a CIK to 10 digits with leading zeros."""
        return str(cik).zfill(10)

    def _throttle(self) -> None:
        """Sleep if needed to respect max_requests_per_sec."""
        wait = self._min_interval - (time.time() - self._last_request_ts)
        if wait > 0:
            time.sleep(wait)
        self._last_request_ts = time.time()

    def _get(self, url: str) -> dict:
        """GET with rate limiting, timeout and retry on 403/429/5xx."""
        for attempt in range(1, _MAX_ATTEMPTS + 1):
            self._throttle()
            try:
                resp = self.session.get(url, timeout=_TIMEOUT_SEC)
            except requests.RequestException as exc:
                if attempt < _MAX_ATTEMPTS:
                    time.sleep(2 ** attempt)  # backoff: 2s, 4s, 8s
                    continue
                raise SECClientError(f"Request failed: {url}") from exc

            if resp.status_code == 200:
                try:
                    return resp.json()
                except ValueError as exc:
                    raise SECClientError(f"Response is not valid JSON: {url}") from exc

            if resp.status_code in _RETRYABLE_STATUSES and attempt < _MAX_ATTEMPTS:
                time.sleep(2 ** attempt)  # backoff: 2s, 4s, 8s
                continue
            resp.raise_for_status()

        raise SECClientError(f"Request failed after {_MAX_ATTEMPTS} attempts: {url}")

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def get_companies(self) -> dict:
        """All companies registered with the SEC (ticker <-> CIK), as JSON.

        Source: https://www.sec.gov/files/company_tickers.json
        """
        return self._get(self.TICKERS_URL)

    def get_company_submissions(self, cik: int | str) -> dict:
        """All filings submitted by a company, as JSON.

        Source: https://data.sec.gov/submissions/CIK##########.json

        Args:
            cik: company CIK (auto-padded to 10 digits).
        """
        return self._get(self.SUBMISSIONS_URL.format(cik=self.format_cik(cik)))

    def get_company_facts(self, cik: int | str) -> dict:
        """All XBRL facts (financial data) of a company, as JSON.

        Source: https://data.sec.gov/api/xbrl/companyfacts/CIK##########.json

        Args:
            cik: company CIK (auto-padded to 10 digits).
        """
        return self._get(self.COMPANYFACTS_URL.format(cik=self.format_cik(cik)))

    def get_company_frames(self, taxonomy: str, tag: str, unit: str, period: str) -> dict:
        """All reported values for one concept and period, as JSON.

        Source: https://data.sec.gov/api/xbrl/frames/{taxonomy}/{tag}/{unit}/{period}.json

        Args:
            taxonomy: XBRL taxonomy, e.g. "us-gaap".
            tag: XBRL concept, e.g. "Assets".
            unit: unit of measure, e.g. "USD" or "shares".
            period: calendar period, e.g. "CY2023" or "CY2023Q3I"
                (I = instantaneous).
        """
        return self._get(self.FRAMES_URL.format(
            taxonomy=taxonomy, tag=tag, unit=unit, period=period
        ))
