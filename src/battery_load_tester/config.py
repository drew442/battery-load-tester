import json
import os
from pathlib import Path
from typing import Any

import yaml
from pydantic import BaseModel, Field


class Settings(BaseModel):
    model_config = {"extra": "ignore"}

    host: str = "127.0.0.1"
    port: int = 8000
    db_path: str = "./battery_load_tester.db"

    rc3563_port: str = "COM5"
    rc3563_baudrate: int = 115200
    rc3563_serial_timeout_s: float = Field(default=0.2, gt=0.0)
    demo_mode: bool = False
    sample_period_s: float = 0.2

    load_detect_voltage_drop_v: float = Field(default=0.15, gt=0.0)
    battery_present_voltage_v: float = Field(default=1.0, gt=0.0)
    min_test_duration_s: float = Field(default=0.5, gt=0.0)
    pre_trigger_samples: int = Field(default=6, ge=0, le=100)


def _load_config_file(config_path: Path) -> dict[str, Any]:
    if not config_path.exists():
        raise FileNotFoundError(
            f"Config file not found: {config_path}. "
            "Create config.yaml (or set BLT_CONFIG_FILE to another path)."
        )

    suffix = config_path.suffix.lower()
    raw = config_path.read_text(encoding="utf-8")

    if suffix in {".yaml", ".yml"}:
        parsed = yaml.safe_load(raw) or {}
    elif suffix == ".json":
        parsed = json.loads(raw)
    else:
        raise ValueError(
            f"Unsupported config extension '{suffix}'. Use .yaml/.yml or .json."
        )

    if not isinstance(parsed, dict):
        raise ValueError("Config root must be a mapping/object.")
    return parsed


def load_settings(config_file: str | None = None) -> Settings:
    path_value = config_file or os.getenv("BLT_CONFIG_FILE", "config.yaml")
    config_path = Path(path_value)
    data = _load_config_file(config_path)
    return Settings.model_validate(data)
