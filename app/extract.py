# app/extract.py
import json
import re
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional

def _set_in(d: Dict[str, Any], path: List[str], value: Any) -> None:
    cur = d
    for p in path[:-1]:
        if p not in cur or not isinstance(cur[p], dict):
            cur[p] = {}
        cur = cur[p]
    cur[path[-1]] = value

def _dotpaths_to_nested(extracted_fields: List[Dict[str, Any]]) -> Dict[str, Any]:
    out: Dict[str, Any] = {}
    for f in extracted_fields or []:
        name = f.get("name")
        value = f.get("value")
        if not name:
            continue
        parts = name.split(".")
        _set_in(out, parts, value)
    return out

def _string_num_or_none(v: Optional[str]) -> Optional[str]:
    if v is None:
        return "null"
    s = str(v).strip()
    return s if s != "" else "null"

REF_DATE = datetime(2025, 9, 23)  # reference date used for relative validity like "+14 days"

def _normalize_valid_until(v: str) -> str:
    if not v:
        return ""
    s = v.strip()
    if re.match(r"^\d{4}-\d{2}-\d{2}$", s):
        return s
    m = re.match(r"^\+(\d{1,3})\s*day[s]?$", s, re.I)
    if m:
        days = int(m.group(1))
        dt = REF_DATE + timedelta(days=days)
        return dt.strftime("%Y-%m-%d")
    for fmt in ("%d-%m-%Y", "%d/%m/%Y", "%m/%d/%Y", "%d.%m.%Y", "%Y/%m/%d", "%d %b %Y", "%d %B %Y"):
        try:
            return datetime.strptime(s, fmt).strftime("%Y-%m-%d")
        except Exception:
            pass
    return s

def extract_airline_rate_fields(chunkr_response: Dict[str, Any]) -> Dict[str, Any]:
    ej = (chunkr_response or {}).get("extracted_json") or {}
    fields = ej.get("extracted_fields") or []
    flat = _dotpaths_to_nested(fields)

    def mk_rate(node: Dict[str, Any]) -> Dict[str, Any]:
        return {
            "per_kg": _string_num_or_none(node.get("per_kg", "")),
            "min_charge": _string_num_or_none(node.get("min_charge", "")),
        }

    result: Dict[str, Any] = {
        "valid_until": _normalize_valid_until(flat.get("valid_until", "")) if isinstance(flat.get("valid_until", ""), str) else "",
        "currency": flat.get("currency", "") if isinstance(flat.get("currency", ""), str) else "",
        "rates": {
            "stackable":     mk_rate(flat.get("rates", {}).get("stackable", {}) if isinstance(flat.get("rates", {}), dict) else {}),
            "non-stackable": mk_rate(flat.get("rates", {}).get("non-stackable", {}) if isinstance(flat.get("rates", {}), dict) else {}),
            "hazardous":     mk_rate(flat.get("rates", {}).get("hazardous", {}) if isinstance(flat.get("rates", {}), dict) else {}),
            "mix":           mk_rate(flat.get("rates", {}).get("mix", {}) if isinstance(flat.get("rates", {}), dict) else {}),
            "general":       mk_rate(flat.get("rates", {}).get("general", {}) if isinstance(flat.get("rates", {}), dict) else {}),
        },
        "screeningPrices": {
            "primaryScreeningPrice":   mk_rate(flat.get("screeningPrices", {}).get("primaryScreeningPrice", {}) if isinstance(flat.get("screeningPrices", {}), dict) else {}),
            "secondaryScreeningPrice": mk_rate(flat.get("screeningPrices", {}).get("secondaryScreeningPrice", {}) if isinstance(flat.get("screeningPrices", {}), dict) else {}),
        },
        "FFWH": {
            "fuelSurcharge":     mk_rate(flat.get("FFWH", {}).get("fuelSurcharge", {}) if isinstance(flat.get("FFWH", {}), dict) else {}),
            "freightCharge":     mk_rate(flat.get("FFWH", {}).get("freightCharge", {}) if isinstance(flat.get("FFWH", {}), dict) else {}),
            "warRiskSurcharge":  mk_rate(flat.get("FFWH", {}).get("warRiskSurcharge", {}) if isinstance(flat.get("FFWH", {}), dict) else {}),
            "handlingFee":       mk_rate(flat.get("FFWH", {}).get("handlingFee", {}) if isinstance(flat.get("FFWH", {}), dict) else {}),
        },
    }
    return result
