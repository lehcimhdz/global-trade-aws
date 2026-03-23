"""
Comtrade API client — handles rate limiting and response normalization.

The public API is unauthenticated but rate-limited to ~1 req/s.
All methods return the parsed JSON dict (or raise on HTTP error).
"""
from __future__ import annotations

import time
import logging
from typing import Any, Dict, Optional

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

BASE_URL = "https://comtradeapi.un.org/public/v1"
DEFAULT_TIMEOUT = 30
REQUEST_DELAY = 1.1  # seconds between requests to respect rate limit

logger = logging.getLogger(__name__)


def _build_session() -> requests.Session:
    session = requests.Session()
    retry = Retry(
        total=3,
        backoff_factor=2,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"],
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount("https://", adapter)
    return session


_session = _build_session()


def _get(url: str, params: Dict[str, Any]) -> Dict:
    # Remove None values so they don't appear as "None" in the query string
    clean_params = {k: v for k, v in params.items() if v is not None}
    logger.info("GET %s params=%s", url, clean_params)
    time.sleep(REQUEST_DELAY)
    resp = _session.get(url, params=clean_params, timeout=DEFAULT_TIMEOUT)
    resp.raise_for_status()
    return resp.json()


# ---------------------------------------------------------------------------
# One function per endpoint
# ---------------------------------------------------------------------------

def get_preview(
    typeCode: str,
    freqCode: str,
    clCode: str,
    reporterCode: Optional[str] = None,
    period: Optional[str] = None,
    partnerCode: Optional[str] = None,
    cmdCode: Optional[str] = None,
    flowCode: Optional[str] = None,
) -> Dict:
    url = f"{BASE_URL}/preview/{typeCode}/{freqCode}/{clCode}"
    return _get(url, {
        "reporterCode": reporterCode,
        "period": period,
        "partnerCode": partnerCode,
        "cmdCode": cmdCode,
        "flowCode": flowCode,
    })


def get_preview_tariffline(
    typeCode: str,
    freqCode: str,
    clCode: str,
    reporterCode: Optional[str] = None,
    period: Optional[str] = None,
    partnerCode: Optional[str] = None,
    partner2Code: Optional[str] = None,
    cmdCode: Optional[str] = None,
    flowCode: Optional[str] = None,
    customsCode: Optional[str] = None,
    motCode: Optional[str] = None,
) -> Dict:
    url = f"{BASE_URL}/previewTariffline/{typeCode}/{freqCode}/{clCode}"
    return _get(url, {
        "reporterCode": reporterCode,
        "period": period,
        "partnerCode": partnerCode,
        "partner2Code": partner2Code,
        "cmdCode": cmdCode,
        "flowCode": flowCode,
        "customsCode": customsCode,
        "motCode": motCode,
    })


def get_world_share(
    typeCode: str,
    freqCode: str,
    period: Optional[str] = None,
    reporterCode: Optional[str] = None,
) -> Dict:
    url = f"{BASE_URL}/getWorldShare/{typeCode}/{freqCode}"
    return _get(url, {"period": period, "reporterCode": reporterCode})


def get_metadata(
    typeCode: str,
    freqCode: str,
    clCode: str,
) -> Dict:
    url = f"{BASE_URL}/getMetadata/{typeCode}/{freqCode}/{clCode}"
    return _get(url, {})


def get_mbs(
    series_type: Optional[str] = None,
    year: Optional[str] = None,
    country_code: Optional[str] = None,
    period: Optional[str] = None,
    period_type: Optional[str] = None,
    table_type: Optional[str] = None,
    fmt: Optional[str] = None,
) -> Dict:
    url = f"{BASE_URL}/getMBS"
    return _get(url, {
        "series_type": series_type,
        "year": year,
        "country_code": country_code,
        "period": period,
        "period_type": period_type,
        "table_type": table_type,
        "format": fmt,
    })


def get_da_tariffline(
    typeCode: str,
    freqCode: str,
    clCode: str,
) -> Dict:
    url = f"{BASE_URL}/getDATariffline/{typeCode}/{freqCode}/{clCode}"
    return _get(url, {})


def get_da(
    typeCode: str,
    freqCode: str,
    clCode: str,
) -> Dict:
    url = f"{BASE_URL}/getDA/{typeCode}/{freqCode}/{clCode}"
    return _get(url, {})


def get_comtrade_releases() -> Dict:
    url = f"{BASE_URL}/getComtradeReleases"
    return _get(url, {})
