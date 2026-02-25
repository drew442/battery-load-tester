from datetime import datetime, timezone

from pydantic import BaseModel, Field


class Measurement(BaseModel):
    ts: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    voltage_v: float
    resistance_ohm: float | None = Field(default=None, ge=0.0)


class LoadTestRecord(BaseModel):
    id: int | None = None
    started_at: datetime
    ended_at: datetime
    duration_s: float
    start_voltage_v: float
    end_voltage_v: float
    min_voltage_v: float
    start_resistance_ohm: float | None = Field(default=None, ge=0.0)
    min_resistance_ohm: float | None = Field(default=None, ge=0.0)
    max_resistance_ohm: float | None = Field(default=None, ge=0.0)
    delta_voltage_v: float
    delta_resistance_ohm: float | None = None
    sample_count: int = Field(ge=0)
    note: str | None = Field(default=None, max_length=120)


class LoadTestSample(BaseModel):
    test_id: int
    t_s: float = Field(ge=0.0)
    voltage_v: float
    resistance_ohm: float | None = Field(default=None, ge=0.0)


class RuntimeStatus(BaseModel):
    connected: bool = False
    under_load: bool = False
    last_measurement: Measurement | None = None
    total_tests: int = 0
    packet_rate_hz: float = 0.0
    buffer_depth: int = 0


class BatteryGroup(BaseModel):
    id: int | None = None
    name: str
    chemistry: str
    module_nominal_v: float = Field(gt=0.0)
    capacity_ah: float = Field(gt=0.0)
    sticker_energy_wh: float | None = Field(default=None, gt=0.0)
    test_temperature_c: float
    target_load_current_a: float = Field(gt=0.0)
    pulse_duration_s: float = Field(gt=0.0)
    rest_time_s: float = Field(gt=0.0)
    created_at: datetime


class GroupRankingEntry(BaseModel):
    rank: int
    test_id: int
    note: str
    score: float
    min_resistance_ohm: float | None = None
    voltage_drop_v: float
