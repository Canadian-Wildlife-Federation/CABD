from __future__ import annotations

from pathlib import Path
import yaml

_BASE = Path(__file__).resolve().parent / "config"

def load_config(name: str) -> dict:
    if name == "data_sources":
        p = _BASE / "data_sources.yaml"
    else:
        p = _BASE / f"{name}.yaml"
    return yaml.safe_load(p.read_text(encoding="utf-8"))
