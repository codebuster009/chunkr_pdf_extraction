# app/chunkr_client.py
import json
import time
from typing import Any, Dict, Optional

import httpx
from httpx import HTTPStatusError
from .config import settings

# SDK (from docs): used for quick upload/poll sanity checks
try:
    from chunkr_ai import Chunkr
except Exception:  # pragma: no cover
    Chunkr = None  # optional


# ----- Legacy endpoints (needed for structured extraction with schema+instructions) -----
CHUNKR_LEGACY_BASE_URL = (settings.URL.rstrip("/") if settings.URL else "https://legacy-api.chunkr.ai")
LEGACY_CREATE_ENDPOINT = "/api/v1/task"
LEGACY_STATUS_ENDPOINT = "/api/v1/task/{task_id}"


# ======== Your structured schema & instructions ========
AIRLINE_JSON_SCHEMA: Dict[str, Any] = {
    "title": "AirfreightRateEmail",
    "type": "object",
    "properties": [
        {"name": "valid_until", "type": "string", "description": "YYYY-MM-DD or empty string"},
        {"name": "currency", "type": "string", "description": "3-letter code or symbol, or empty string"},
        {"name": "rates.stackable.per_kg", "type": "string"},
        {"name": "rates.stackable.min_charge", "type": "string"},
        {"name": "rates.non-stackable.per_kg", "type": "string"},
        {"name": "rates.non-stackable.min_charge", "type": "string"},
        {"name": "rates.hazardous.per_kg", "type": "string"},
        {"name": "rates.hazardous.min_charge", "type": "string"},
        {"name": "rates.mix.per_kg", "type": "string"},
        {"name": "rates.mix.min_charge", "type": "string"},
        {"name": "rates.general.per_kg", "type": "string"},
        {"name": "rates.general.min_charge", "type": "string"},
        {"name": "screeningPrices.primaryScreeningPrice.per_kg", "type": "string"},
        {"name": "screeningPrices.primaryScreeningPrice.min_charge", "type": "string"},
        {"name": "screeningPrices.secondaryScreeningPrice.per_kg", "type": "string"},
        {"name": "screeningPrices.secondaryScreeningPrice.min_charge", "type": "string"},
        {"name": "FFWH.fuelSurcharge.per_kg", "type": "string"},
        {"name": "FFWH.fuelSurcharge.min_charge", "type": "string"},
        {"name": "FFWH.freightCharge.per_kg", "type": "string"},
        {"name": "FFWH.freightCharge.min_charge", "type": "string"},
        {"name": "FFWH.warRiskSurcharge.per_kg", "type": "string"},
        {"name": "FFWH.warRiskSurcharge.min_charge", "type": "string"},
        {"name": "FFWH.handlingFee.per_kg", "type": "string"},
        {"name": "FFWH.handlingFee.min_charge", "type": "string"},
    ],
}

AIRLINE_INSTRUCTIONS = """You are extracting structured airfreight rate data from an airline rate email/PDF.

Return ONLY valid JSON that exactly matches the provided JSON schema field names. Each numeric field must be a string containing a number or "null". Use "" for empty string fields.

RULES:
- Normalize decimals: convert commas to dots, e.g., "0,1676" -> "0.1676".
- Treat ">" "/" ":" as separators. Example: "Min >35.91 per kg >0.1676" => min_charge: "35.91", per_kg: "0.1676".
- Strip inequality signs (">", "<=", etc.) from numbers; extract numeric values only.
- "currency" should be a symbol or 3-letter code found in the text; otherwise "".

- FF/WW/H mapping:
  - If a single combined "FFW" (e.g., "FFW: £0.60", "FFW 0.60", "SMART FFW") appears, map to FFWH.freightCharge.per_kg, and min if present. Set FFWH.fuelSurcharge and FFWH.warRiskSurcharge to "null" unless explicitly separate.
  - If "FF" appears separately and "W" or "H" separately, map them respectively to FFWH.freightCharge, FFWH.warRiskSurcharge, FFWH.handlingFee.

- Screening mapping:
  - Map "X-Ray Fee" to screeningPrices.secondaryScreeningPrice.
  - Map "Security Charge" or generic screening to primaryScreeningPrice only if distinct from X-Ray. If both appear with similar rates, fill secondary (X-Ray) and set primary to "null"/"" to avoid duplication.

- Handling fee mapping (FFWH.handlingFee) ONLY when an explicit loose-handling label occurs:
  Accept (case-insensitive, punctuation allowed): "Loose Handling Fee", "Loose Handling", "Loose-handling fee", "Loose handling charge".
  If a number appears on same line/sentence, map it (per_kg if clearly per-kg; else min_charge).
  Do NOT map unrelated handling terms (e.g., "Unit Handling Fee", "Processing", "Storage Charge", "POD Fee", "DG Check").

- Rates bucket:
  - Put explicit tiers under rates.<key> (stackable, non-stackable, hazardous, mix, general). If an item belongs to FFW/FF/WH buckets, prioritize FFWH over rates.

- valid_until:
  - Today’s date is 2025-09-23. If validity is relative (e.g., "+14 Days"), compute absolute date (YYYY-MM-DD). If absolute, use it; if missing, "".

- Do not invent values. Do not emit extra keys. Output must be valid JSON per the schema."""


class ChunkrLegacyClient:
    def __init__(self, api_key: str, base_url: str):
        self.api_key = api_key
        self.base_url = base_url.rstrip("/")

    def _legacy_headers(self) -> Dict[str, str]:
        # Per docs: raw API key (no "Bearer")
        return {"Authorization": self.api_key}

    async def create_structured_task_legacy(self, file_bytes: bytes, filename: str) -> str:
        """
        POST multipart with explicit content-types:
          - file: application/pdf
          - json_schema: application/json
          - instructions: text/plain
        Includes retry/backoff for 502/503/504.
        """
        files = {
            "file": (filename, file_bytes, "application/pdf"),
            "json_schema": ("json_schema", json.dumps(AIRLINE_JSON_SCHEMA), "application/json"),
            "instructions": ("instructions", AIRLINE_INSTRUCTIONS, "text/plain"),
        }
        data = {"model": "Fast", "ocr_strategy": "Auto"}

        max_attempts = 5
        backoff = 1.0
        last_err = None

        for attempt in range(1, max_attempts + 1):
            try:
                async with httpx.AsyncClient(timeout=90.0, follow_redirects=True) as client:
                    resp = await client.post(
                        f"{self.base_url}{LEGACY_CREATE_ENDPOINT}",
                        headers=self._legacy_headers(),
                        data=data,
                        files=files,
                    )
                resp.raise_for_status()
                payload = resp.json()
                task_id = payload.get("task_id") or payload.get("id") or (payload.get("task") or {}).get("id")
                if not task_id:
                    raise RuntimeError(f"Chunkr legacy: missing task id in response: {payload}")
                return task_id
            except HTTPStatusError as e:
                code = e.response.status_code
                last_err = f"{code}: {(e.response.text or '')[:500]}"
                if code in (502, 503, 504) and attempt < max_attempts:
                    time.sleep(backoff)
                    backoff = min(backoff * 2, 8)
                    continue
                raise
            except Exception as e:
                last_err = str(e)
                if attempt < max_attempts:
                    time.sleep(backoff)
                    backoff = min(backoff * 2, 8)
                    continue
                raise RuntimeError(f"Chunkr legacy create failed after retries. Last error: {last_err}")

    async def poll_task_until_complete_legacy(
        self,
        task_id: str,
        *,
        max_tries: int = 120,
        delay_seconds: float = 2.0,
    ) -> Dict[str, Any]:
        url = f"{self.base_url}{LEGACY_STATUS_ENDPOINT.format(task_id=task_id)}"
        async with httpx.AsyncClient(timeout=30.0) as client:
            for _ in range(max_tries):
                resp = await client.get(url, headers=self._legacy_headers())
                if resp.status_code == 401:
                    raise PermissionError("Chunkr legacy unauthorized (401) - check API key")
                resp.raise_for_status()
                data = resp.json()
                status_value = data.get("status") or data.get("state")
                # legacy structured: expect 'extracted_json' when done
                if status_value in {"completed", "done", "Succeeded"} or data.get("extracted_json"):
                    return data
                if status_value in {"failed", "error", "Failed"}:
                    raise RuntimeError(f"Chunkr legacy task failed: {data}")
                await asyncio.sleep(delay_seconds)
        raise TimeoutError("Chunkr legacy task poll timed out")


# Singleton(s)
_legacy_client: Optional[ChunkrLegacyClient] = None
_sdk_client: Optional["Chunkr"] = None


def get_legacy_client() -> ChunkrLegacyClient:
    global _legacy_client
    if _legacy_client is None:
        _legacy_client = ChunkrLegacyClient(api_key=settings.API_KEY, base_url=CHUNKR_LEGACY_BASE_URL)
    return _legacy_client


def get_sdk_client() -> Optional["Chunkr"]:
    # Optional helper to confirm connectivity via official SDK (no schema)
    global _sdk_client
    if Chunkr is None:
        return None
    if _sdk_client is None:
        # From docs: can pass api_key and custom URL via env or params
        if settings.URL:
            _sdk_client = Chunkr(api_key=settings.API_KEY, chunkr_url=settings.URL, raise_on_failure=settings.RAISE_ON_FAILURE)
        else:
            _sdk_client = Chunkr(api_key=settings.API_KEY, raise_on_failure=settings.RAISE_ON_FAILURE)
    return _sdk_client
