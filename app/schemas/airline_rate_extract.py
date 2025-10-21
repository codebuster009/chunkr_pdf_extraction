from pydantic import BaseModel, Field
from typing import Optional


class MiniPricing(BaseModel):
    """Normalized pricing structure for all per-kg and minimum-charge fields."""
    per_kg: float = Field(
        default=0.0,
        description=(
            "Per-kg or per-volume rate. Must always be a number. "
            "Normalize commas ('0,2394' → 0.2394). Strip currency symbols. "
            "Example: '£0.65/kg' → 0.65."
        ),
    )
    min_charge: float = Field(
        default=0.0,
        description=(
            "Minimum charge amount. Must always be a number. "
            "If missing, set to 0. Example: 'Min £35.91' → 35.91."
        ),
    )


class AirlineRateExtract(BaseModel):
    """
    Extract detailed airline rate and surcharge data from promotional PDFs or rate sheets.

    RULES:
    - All numeric fields must be numbers; if missing → 0.
    - Parse 'Min → 35.91 per kg → 0.1676' as { min_charge: 35.91, per_kg: 0.1676 }.
    - Currency should be detected and normalized (GBP/£, USD/$, AED, EUR/€).
    - Do NOT copy X-Ray or Handling values into Base Freight. Keep category separation clean.
    """

    currency: Optional[str] = Field(
        default=None,
        description=(
            "Currency code or symbol (e.g., GBP/£, USD/$, AED, EUR/€). "
            "If multiple appear, choose the one associated with the base freight rate."
        ),
    )

    base_freight: Optional[MiniPricing] = Field(
        default=None,
        description=(
            "Base freight rate. Map any of: 'FFW', 'SMART FFW', 'Base Freight Rate', "
            "'Freight Rate', 'Rate £/kg', 'Base Rate', 'Linehaul', or 'Airfreight'. "
            "Example: 'Base Freight Rate AED 2.60/kg' → { per_kg: 2.60, min_charge: 0 }."
        ),
    )

    fuel_surcharge: Optional[MiniPricing] = Field(
        default=None,
        description=(
            "Fuel surcharge. May be per kg or percentage. "
            "If a percentage is shown (e.g., 25%), return numeric value (0.25). "
            "Example: 'Fuel Surcharge 0.25/kg' → { per_kg: 0.25, min_charge: 0 }."
        ),
    )

    war_risk_surcharge: Optional[MiniPricing] = Field(
        default=None,
        description=(
            "War Risk Surcharge. Map only if explicitly labeled (e.g., 'WRS', 'War Risk', 'War Surcharge'). "
            "Include both per-kg and min charge if available."
        ),
    )

    handling_fee: Optional[MiniPricing] = Field(
        default=None,
        description=(
            "Loose Handling Fee — only include rows clearly labeled 'Loose Handling' or 'Loose Handling Fee'. "
            "Ignore other handling types (e.g., Unit, Val, PIL, or Security Handling)."
        ),
    )

    primary_screening: Optional[MiniPricing] = Field(
        default=None,
        description=(
            "Primary screening — typically 'X-Ray'. "
            "Map any line with 'X-Ray', 'Primary Screening', or 'Security (X-Ray)'. "
            "Example: 'X-Ray £0.10/kg Min £15' → { per_kg: 0.10, min_charge: 15 }."
        ),
    )

    secondary_screening: Optional[MiniPricing] = Field(
        default=None,
        description=(
            "Secondary screening — only if explicitly labeled as such. "
            "If not found, leave null."
        ),
    )

    transit_time_days: Optional[int] = Field(
        default=None,
        description=(
            "Transit time in days. Example: 'Transit Time: 2-3 Days' → 3. "
            "Ignore text related to rate validity."
        ),
    )

    rate_validity_until: Optional[str] = Field(
        default=None,
        description=(
            "Rate validity date (YYYY-MM-DD). "
            "If shown as '+14 Days', compute from 2025-09-23 as base date."
        ),
    )
