"""`{{ macro }}` placeholders in config strings, resolved against a Clock."""

import re
from datetime import timedelta

from medallion.controller.clock import Clock

_PLACEHOLDER = re.compile(r"\{\{\s*(\w+)\s*\}\}")


def macro_values(clock: Clock) -> dict[str, str]:
    today = clock.today()
    return {
        "today": today.isoformat(),
        "yesterday": (today - timedelta(days=1)).isoformat(),
        "current_year": str(today.year),
        "last_year": str(today.year - 1),
    }


def resolve_macros(obj, values: dict[str, str]):
    """Resolve placeholders in every string of a YAML tree; unknown macros raise KeyError."""
    if isinstance(obj, dict):
        return {k: resolve_macros(v, values) for k, v in obj.items()}
    if isinstance(obj, list):
        return [resolve_macros(v, values) for v in obj]
    if isinstance(obj, str):
        return _PLACEHOLDER.sub(lambda m: values[m.group(1)], obj)
    return obj
