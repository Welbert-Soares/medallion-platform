"""
Resolves {{ macro }} placeholders in YAML string values.
Supported macros: today, yesterday, current_year, last_year.
"""
from datetime import datetime, timedelta

_MACROS = {
    "today":        lambda: datetime.now().strftime("%Y-%m-%d"),
    "yesterday":    lambda: (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d"),
    "current_year": lambda: str(datetime.now().year),
    "last_year":    lambda: str(datetime.now().year - 1),
}


def resolve_macros(value: str) -> str:
    for name, fn in _MACROS.items():
        value = value.replace(f"{{{{ {name} }}}}", fn())
    return value


def resolve_all(obj):
    """Recursively resolves macros in all string values of a dict/list/str."""
    if isinstance(obj, dict):
        return {k: resolve_all(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [resolve_all(v) for v in obj]
    if isinstance(obj, str):
        return resolve_macros(obj)
    return obj
