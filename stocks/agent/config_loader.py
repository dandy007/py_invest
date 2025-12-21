from __future__ import annotations

import copy
from pathlib import Path
from typing import Any, Dict

import yaml

AGENT_DIR = Path(__file__).parent.resolve()
CONFIG_FILE = AGENT_DIR / "config.yaml"

DEFAULT_SENTIMENT_PROMPT = (
    "Proved strukturovanou sentiment analyzu akcie {ticker}. "
    "Vyuzij dostupne databazove nastroje pro nacteni financnich metrik a "
    "zprav minimalne za poslednich {lookback_days} dni. "
    "Nezapomen na rizika, valuaci, rust, momentum a recent news. "
    "Na konci vrat cisty JSON:\n"
    '{"sentiment_score": <0-100>, "rating": "<text>", '
    '"summary": "<2-3 vety s duvody>", '
    '"confidence": "low|medium|high"}'
)

DEFAULT_CONFIG: Dict[str, Any] = {
    "model": "google/gemini-2.0-flash-001",
    "active_personality": "default",
    "personalities": {
        "default": {
            "name": "Analyst",
            "system_prompt": (
                "Jsi profesionalni financni analytik. Odpovidas vecne a strucne. "
                "Pouzivas data z databaze k podpore svych tvrzeni. "
                "Kdyz nemas dost informaci, ptej se na upresneni."
            ),
        }
    },
    "sentiment_job": {
        "enabled": False,
        "model": None,
        "system_prompt": (
            "Jsi kvantitativni analytik, ktery pripravuje sentiment skore pro "
            "spravu portfolia. Vyuzij dostupne databazove nastroje, porovnavej "
            "zpravy a vzdy vysvetli klicove katalyzatory."
        ),
        "prompt": DEFAULT_SENTIMENT_PROMPT,
        "lookback_days": 7,
        "max_articles": 12,
        "max_tickers_per_run": 5,
        "refresh_days": 1,
        "min_market_cap": 0,
    },
    "tool_logging": {
        "enabled": True,
        "log_arguments": True,
        "log_response": True,
        "max_response_chars": 1200,
    },
}


def _deep_merge(defaults: Dict[str, Any], existing: Dict[str, Any]) -> Dict[str, Any]:
    """Recursively merge dictionaries."""
    result = copy.deepcopy(defaults)
    for key, value in existing.items():
        if (
            key in result
            and isinstance(result[key], dict)
            and isinstance(value, dict)
        ):
            result[key] = _deep_merge(result[key], value)
        else:
            result[key] = value
    return result


def load_config() -> Dict[str, Any]:
    """Load YAML config and ensure defaults."""
    if CONFIG_FILE.exists():
        with open(CONFIG_FILE, "r", encoding="utf-8") as f:
            raw = yaml.safe_load(f) or {}
    else:
        raw = {}

    return _deep_merge(DEFAULT_CONFIG, raw)


def save_config(config: Dict[str, Any]) -> None:
    """Persist config back to YAML."""
    CONFIG_FILE.parent.mkdir(parents=True, exist_ok=True)
    with open(CONFIG_FILE, "w", encoding="utf-8") as f:
        yaml.dump(
            config,
            f,
            allow_unicode=True,
            default_flow_style=False,
            sort_keys=False,
        )


__all__ = [
    "AGENT_DIR",
    "CONFIG_FILE",
    "DEFAULT_CONFIG",
    "DEFAULT_SENTIMENT_PROMPT",
    "load_config",
    "save_config",
]
