# chunkr_client.py
import base64
import json
import asyncio
from typing import Any, Dict, Optional, Tuple

import httpx
from .config import settings

# New API (JSON body) – kept for general parsing if needed
CHUNKR_BASE_URL = "https://api.chunkr.ai"
PARSE_ENDPOINT = "/api/v1/tasks/parse"
TASK_STATUS_ENDPOINT = "/api/v1/tasks/{task_id}"

# Legacy API (multipart form) – supports json_schema + instructions
CHUNKR_LEGACY_BASE_URL = "https://legacy-api.chunkr.ai"
LEGACY_CREATE_ENDPOINT = "/api/v1/task"
LEGACY_STATUS_ENDPOINT = "/api/v1/task/{task_id}"

AIRLINE_JSON_SCHEMA = {
    "title": "AirfreightRateEmail",
    "type": "object",
    "properties": [
        {"name": "valid_until", "type": "string", "description": "YYYY-MM-DD or empty string"},
        {"name": "currency", "type": "string", "description": "3-letter code or symbol, or empty string"},

        # Flattened dot-path properties (so we can reconstruct nesting)
        # Rates buckets
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

        # Screening
        {"name": "screeningPrices.primaryScreeningPrice.per_kg", "type": "string"},
        {"name": "screeningPrices.primaryScreeningPrice.min_charge", "type": "string"},
        {"name": "screeningPrices.secondaryScreeningPrice.per_kg", "type": "string"},
        {"name": "screeningPrices.secondaryScreeningPrice.min_charge", "type": "string"},

        # FFWH
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

class ChunkrHttpClient:
    def __init__(self, api_key: str, base_url: str = CHUNKR_BASE_URL) -> None:
        self.api_key = api_key
        self.base_url = base_url.rstrip("/")

    # -------- NEW API (kept) --------
    def _headers(self) -> Dict[str, str]:
        # Docs: Authorization is the raw API key (no "Bearer")
        return {"Authorization": self.api_key, "Content-Type": "application/json"}

    async def create_parse_task(self, file_bytes: bytes, filename: str) -> str:
        payload = {
            "file": base64.b64encode(file_bytes).decode("utf-8"),
            "file_name": filename[:255],
        }
        async with httpx.AsyncClient(timeout=30.0) as client:
            resp = await client.post(
                f"{self.base_url}{PARSE_ENDPOINT}", json=payload, headers=self._headers()
            )
        resp.raise_for_status()
        data = resp.json()
        task_id = data.get("task_id") or data.get("id") or (data.get("task") or {}).get("id")
        if not task_id:
            raise RuntimeError("Chunkr: missing task id in response")
        return task_id

    async def poll_task_until_complete(
        self, task_id: str, *, max_tries: int = 60, delay_seconds: float = 2.0
    ) -> Dict[str, Any]:
        url = f"{self.base_url}{TASK_STATUS_ENDPOINT.format(task_id=task_id)}"
        async with httpx.AsyncClient(timeout=30.0) as client:
            for _ in range(max_tries):
                resp = await client.get(url, headers=self._headers())
                if resp.status_code == 401:
                    raise PermissionError("Chunkr unauthorized (401) - check API key")
                if resp.status_code == 404:
                    await asyncio.sleep(delay_seconds)
                    continue
                resp.raise_for_status()
                data = resp.json()
                status_value = data.get("status") or data.get("state")
                if status_value in {"completed", "done", "Succeeded"} or data.get("output") or data.get("extracted_json"):
                    return data
                if status_value in {"failed", "error", "Failed"}:
                    raise RuntimeError(f"Chunkr task failed: {data}")
                await asyncio.sleep(delay_seconds)
        raise TimeoutError("Chunkr task poll timed out")

    # -------- LEGACY API (structured extraction) --------
    def _legacy_headers(self) -> Dict[str, str]:
        # legacy also expects raw API key
        return {"Authorization": self.api_key}

    async def create_structured_task_legacy(self, file_bytes: bytes, filename: str) -> str:
        """
        POST multipart form with:
          file, model, ocr_strategy, json_schema (as JSON string), instructions (text)
        """
        files = {
            "file": (filename, file_bytes, "application/octet-stream"),
        }
        data = {
            "model": "Fast",
            "ocr_strategy": "Auto",
            "json_schema": json.dumps(AIRLINE_JSON_SCHEMA),
            "instructions": AIRLINE_INSTRUCTIONS,
        }
        async with httpx.AsyncClient(timeout=60.0) as client:
            resp = await client.post(
                f"{CHUNKR_LEGACY_BASE_URL}{LEGACY_CREATE_ENDPOINT}",
                headers=self._legacy_headers(),
                files=files,
                data=data,
            )
        resp.raise_for_status()
        payload = resp.json()
        task_id = payload.get("task_id") or payload.get("id") or (payload.get("task") or {}).get("id")
        if not task_id:
            raise RuntimeError(f"Chunkr legacy: missing task id in response: {payload}")
        return task_id

    async def poll_task_until_complete_legacy(
        self, task_id: str, *, max_tries: int = 90, delay_seconds: float = 2.0
    ) -> Dict[str, Any]:
        url = f"{CHUNKR_LEGACY_BASE_URL}{LEGACY_STATUS_ENDPOINT.format(task_id=task_id)}"
        async with httpx.AsyncClient(timeout=30.0) as client:
            for _ in range(max_tries):
                resp = await client.get(url, headers=self._legacy_headers())
                if resp.status_code == 401:
                    raise PermissionError("Chunkr legacy unauthorized (401) - check API key")
                if resp.status_code == 404:
                    await asyncio.sleep(delay_seconds)
                    continue
                resp.raise_for_status()
                data = resp.json()
                status_value = data.get("status")
                if status_value in {"completed", "done", "Succeeded"} or data.get("extracted_json"):
                    return data
                if status_value in {"failed", "error", "Failed"}:
                    raise RuntimeError(f"Chunkr legacy task failed: {data}")
                await asyncio.sleep(delay_seconds)
        raise TimeoutError("Chunkr legacy task poll timed out")


client_instance: Optional[ChunkrHttpClient] = None

def get_client() -> ChunkrHttpClient:
    global client_instance
    if client_instance is None:
        client_instance = ChunkrHttpClient(api_key=settings.API_KEY)
    return client_instance
