import json
import re
from datetime import datetime

# ---------- helpers ----------

MONTHS = {
    "jan":1,"january":1,"feb":2,"february":2,"mar":3,"march":3,"apr":4,"april":4,
    "may":5,"jun":6,"june":6,"jul":7,"july":7,"aug":8,"august":8,"sep":9,"sept":9,"september":9,
    "oct":10,"october":10,"nov":11,"november":11,"dec":12,"december":12
}

CURRENCY_SYMBOLS = ["£","€","$","₹","¥"]
CURRENCY_SYMBOL_TO_CODE = {"£":"GBP","€":"EUR","$":"USD","₹":"INR","¥":"JPY"}

PRIMARY_SYNONYMS = [
    r"primary\s+security",
    r"\bsecurity\s+charge\b",
    r"\bsecurity\b(?!.*secondary)",  # security alone (avoid "secondary")
]
SECONDARY_SYNONYMS = [
    r"secondary\s+screening",
    r"\bx[- ]?ray\b",
    r"\bexplosive\s+trace\s+det(ector|ection)\b",
]
FUEL_SYNONYMS = [
    r"fuel\s*surcharge",
    r"\bfs\b(?::|\s)",  # FS: or FS <num>
]
WAR_SYNONYMS = [
    r"\bwar\s*surcharge\b",
    r"\bwrs\b(?::|\s)",
    r"\bwar\s*risk\b",
]
FFW_SYNONYMS = [
    r"\bffw\b(?::|\s)",
    r"\bfreight\s*(?:fee|charge)\b",
]
LOOSE_HANDLING_SYNONYMS = [
    r"loose[\s-]*handling(?:\s*(?:fee|charge))?",
]

def _as_dict(task_output):
    """
    Normalize Chunkr SDK output to dict.
    - SDK output.json() may return str.
    """
    if hasattr(task_output, "json") and callable(task_output.json):
        data = task_output.json()
        if isinstance(data, (bytes, bytearray)):
            try: data = data.decode("utf-8")
            except Exception: pass
        if isinstance(data, str):
            return json.loads(data)
        if isinstance(data, dict):
            return data
    if isinstance(task_output, (bytes, bytearray)):
        try: task_output = task_output.decode("utf-8")
        except Exception: pass
    if isinstance(task_output, str):
        return json.loads(task_output)
    if isinstance(task_output, dict):
        return task_output
    raise TypeError(f"Unsupported output type: {type(task_output).__name__}")

def _collect_text(chunks):
    """
    Concatenate every segment's text/markdown/content and also table HTML text.
    """
    blobs = []
    for ch in chunks.get("chunks", []):
        for seg in ch.get("segments", []):
            t = seg.get("text") or seg.get("markdown") or seg.get("content") or ""
            if t:
                # strip basic html tags from tiny table snippets to make regex matching easier
                t_clean = re.sub(r"<[^>]+>", " ", t)
                blobs.append(t_clean)
    return "\n".join(blobs)

def _num(s):
    if not s: return None
    s = s.strip()
    s = s.replace(",", "")  # normalize 1,000.50 -> 1000.50
    m = re.search(r"-?\d+(?:\.\d+)?", s)
    return m.group(0) if m else None

def _pair_from_line(line):
    """
    Extract two numbers if present, typical 'rate | min' or 'per kg ... min ...' patterns.
    """
    nums = re.findall(r"[£€$₹¥]?\s*([0-9]+(?:\.[0-9]+)?)", line)
    if len(nums) >= 2: return nums[0], nums[1]
    if len(nums) == 1: return nums[0], None
    return None, None

def _find_by_synonyms(bigtext, patterns, nil_ok=True):
    """
    Find a per-kg (first number) and min (second number) by scanning lines
    that match any synonym pattern.
    Returns (perkg, min) as strings or (None, None).
    If NIL present and nil_ok, return (None, None).
    """
    perkg, minc = None, None
    for raw_line in bigtext.splitlines():
        line = raw_line.strip()
        l = line.lower()
        if any(re.search(p, l, re.I) for p in patterns):
            if nil_ok and re.search(r"\bNIL\b", line, re.I):
                return (None, None)
            # Try common stylings: "£0.245 | £26.50", "0.245 per kg, min 26.50"
            # Prefer the first two numbers on the line.
            p, m = _pair_from_line(line)
            # If we saw "per kg" then bias first number as per kg.
            if p or m:
                return (p, m)
    return (perkg, minc)

def _find_currency(bigtext):
    # Prefer symbols; if multiple, choose the most frequent
    found_symbols = [sym for sym in CURRENCY_SYMBOLS if sym in bigtext]
    if found_symbols:
        # Count by occurrence
        sym = max(found_symbols, key=lambda s: bigtext.count(s))
        return CURRENCY_SYMBOL_TO_CODE.get(sym, "")
    # Fallback: 3-letter currency code in text (GBP, USD, EUR, etc.)
    m = re.search(r"\b([A-Z]{3})\b", bigtext)
    return m.group(1) if m else ""

def _find_valid_until(bigtext):
    # Typical "Effective 12th September 2025" / "EFFECTIVE 12TH SEPTEMBER 2025"
    m = re.search(r"(?:EFFECTIVE|Effective)\s*[: ]\s*(\d{1,2})(?:st|nd|rd|th)?\s+([A-Za-z]+)\s+(\d{4})", bigtext)
    if not m:
        return ""
    d = int(m.group(1))
    mon_name = m.group(2).lower()
    y = int(m.group(3))
    mon = MONTHS.get(mon_name)
    if not mon:
        return ""
    try:
        return datetime(y, mon, d).strftime("%Y-%m-%d")
    except Exception:
        return ""

def _string_or_null(x):
    return str(x) if (x is not None and x != "") else None

# ---------- main entry ----------

def extract_airline_from_chunks(task_output) -> dict:
    """
    Convert Chunkr SDK output (chunks) into your normalized airline JSON.
    Numbers are returned as strings (or null) to match your Node pipeline.
    """
    chunks = _as_dict(task_output)
    text = _collect_text(chunks)

    valid_until = _find_valid_until(text)
    currency    = _find_currency(text) or ""

    # Charges
    fuel_perkg, fuel_min = _find_by_synonyms(text, FUEL_SYNONYMS, nil_ok=True)
    war_perkg,  war_min  = _find_by_synonyms(text, WAR_SYNONYMS,  nil_ok=True)
    ffw_perkg,  ffw_min  = _find_by_synonyms(text, FFW_SYNONYMS,  nil_ok=False)

    # Screening
    prim_perkg, prim_min = _find_by_synonyms(text, PRIMARY_SYNONYMS, nil_ok=False)
    sec_perkg,  sec_min  = _find_by_synonyms(text, SECONDARY_SYNONYMS, nil_ok=False)

    # Loose Handling only (strict)
    handle_perkg, handle_min = _find_by_synonyms(text, LOOSE_HANDLING_SYNONYMS, nil_ok=False)

    return {
        "valid_until": valid_until or "",
        "currency": currency,
        "rates": {
            # Leave these null unless you later add specific mapping rules for tables → buckets.
            "stackable":     {"per_kg": None, "min_charge": None},
            "non-stackable": {"per_kg": None, "min_charge": None},
            "hazardous":     {"per_kg": None, "min_charge": None},
            "mix":           {"per_kg": None, "min_charge": None},
            "general":       {"per_kg": None, "min_charge": None},
        },
        "screeningPrices": {
            "primaryScreeningPrice":   {"per_kg": _string_or_null(prim_perkg), "min_charge": _string_or_null(prim_min)},
            "secondaryScreeningPrice": {"per_kg": _string_or_null(sec_perkg),  "min_charge": _string_or_null(sec_min)},
        },
        "FFWH": {
            "fuelSurcharge":    {"per_kg": _string_or_null(fuel_perkg), "min_charge": _string_or_null(fuel_min)},
            "freightCharge":    {"per_kg": _string_or_null(ffw_perkg),  "min_charge": _string_or_null(ffw_min)},
            "warRiskSurcharge": {"per_kg": _string_or_null(war_perkg),  "min_charge": _string_or_null(war_min)},
            "handlingFee":      {"per_kg": _string_or_null(handle_perkg), "min_charge": _string_or_null(handle_min)},
        },
    }
