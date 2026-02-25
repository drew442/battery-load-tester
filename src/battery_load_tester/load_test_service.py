import asyncio
import logging
from collections import deque
from dataclasses import dataclass
from datetime import datetime
import time

from .models import LoadTestRecord, LoadTestSample, Measurement, RuntimeStatus
from .storage import TestStorage

logger = logging.getLogger(__name__)


@dataclass
class ActivePulse:
    started_at: datetime
    start_voltage_v: float
    min_voltage_v: float
    start_resistance_ohm: float | None
    min_resistance_ohm: float | None
    max_resistance_ohm: float | None
    samples: list[Measurement]


class LoadTestService:
    def __init__(
        self,
        storage: TestStorage,
        load_detect_voltage_drop_v: float,
        battery_present_voltage_v: float,
        min_test_duration_s: float,
        pre_trigger_samples: int,
    ) -> None:
        self._storage = storage
        self._load_detect_voltage_drop_v = load_detect_voltage_drop_v
        self._battery_present_voltage_v = battery_present_voltage_v
        self._min_test_duration_s = min_test_duration_s
        self._pre_trigger_samples = max(pre_trigger_samples, 0)
        self._status = RuntimeStatus(total_tests=storage.count_tests())
        self._active_pulse: ActivePulse | None = None
        self._pre_trigger_buffer: deque[Measurement] = deque(maxlen=self._pre_trigger_samples)
        self._baseline_voltage_v: float | None = None
        self._pkt_window_start = time.monotonic()
        self._pkt_count = 0
        self._lock = asyncio.Lock()

    async def on_measurement(self, measurement: Measurement) -> None:
        async with self._lock:
            self._pkt_count += 1
            now = time.monotonic()
            elapsed = now - self._pkt_window_start
            if elapsed >= 1.0:
                self._status.packet_rate_hz = self._pkt_count / elapsed
                self._pkt_count = 0
                self._pkt_window_start = now
            self._status.buffer_depth = len(self._pre_trigger_buffer)
            self._status.connected = True
            self._status.last_measurement = measurement
            battery_present = abs(measurement.voltage_v) >= self._battery_present_voltage_v

            if not battery_present:
                self._baseline_voltage_v = None
                self._pre_trigger_buffer.clear()
                if self._active_pulse is not None:
                    self._active_pulse.samples.append(measurement)
                    await self._finalize_pulse()
                self._status.under_load = False
                return

            if self._baseline_voltage_v is None:
                self._baseline_voltage_v = measurement.voltage_v
            elif self._active_pulse is None:
                # Track open-circuit baseline only while not under load.
                self._baseline_voltage_v = 0.9 * self._baseline_voltage_v + 0.1 * measurement.voltage_v

            baseline = self._baseline_voltage_v if self._baseline_voltage_v is not None else measurement.voltage_v
            voltage_drop = max(baseline - measurement.voltage_v, 0.0)
            is_under_load = voltage_drop >= self._load_detect_voltage_drop_v

            if is_under_load:
                if self._active_pulse is None:
                    buffered = list(self._pre_trigger_buffer)
                    self._active_pulse = ActivePulse(
                        started_at=measurement.ts,
                        start_voltage_v=measurement.voltage_v,
                        min_voltage_v=measurement.voltage_v,
                        start_resistance_ohm=measurement.resistance_ohm,
                        min_resistance_ohm=measurement.resistance_ohm,
                        max_resistance_ohm=measurement.resistance_ohm,
                        samples=buffered + [measurement],
                    )
                else:
                    self._active_pulse.min_voltage_v = min(
                        self._active_pulse.min_voltage_v,
                        measurement.voltage_v,
                    )
                    if measurement.resistance_ohm is not None:
                        if self._active_pulse.min_resistance_ohm is None:
                            self._active_pulse.min_resistance_ohm = measurement.resistance_ohm
                        else:
                            self._active_pulse.min_resistance_ohm = min(
                                self._active_pulse.min_resistance_ohm,
                                measurement.resistance_ohm,
                            )
                        if self._active_pulse.max_resistance_ohm is None:
                            self._active_pulse.max_resistance_ohm = measurement.resistance_ohm
                        else:
                            self._active_pulse.max_resistance_ohm = max(
                                self._active_pulse.max_resistance_ohm,
                                measurement.resistance_ohm,
                            )
                    self._active_pulse.samples.append(measurement)
                self._status.under_load = True
                return

            if self._active_pulse is not None:
                # Keep a trailing edge sample so the graph includes pulse release.
                self._active_pulse.samples.append(measurement)
                await self._finalize_pulse()
            else:
                self._pre_trigger_buffer.append(measurement)
            self._status.under_load = False

    async def mark_disconnected(self) -> None:
        async with self._lock:
            self._status.connected = False
            self._status.under_load = False

    async def get_status(self) -> RuntimeStatus:
        async with self._lock:
            return self._status.model_copy(deep=True)

    async def refresh_total_tests(self) -> None:
        async with self._lock:
            self._status.total_tests = self._storage.count_tests()

    async def _finalize_pulse(self) -> None:
        pulse = self._active_pulse
        self._active_pulse = None
        if pulse is None:
            return

        if not pulse.samples:
            return
        core_samples = [sample for sample in pulse.samples if sample.ts >= pulse.started_at]
        if len(core_samples) > 1:
            tail = core_samples[-1]
            # Drop trailing sample captured after load release.
            if tail.voltage_v > (pulse.start_voltage_v + self._load_detect_voltage_drop_v * 0.5):
                core_samples = core_samples[:-1]
        if not core_samples:
            return
        start_sample = core_samples[0]
        end_sample = core_samples[-1]
        last_sample = pulse.samples[-1]
        duration = (end_sample.ts - pulse.started_at).total_seconds()
        if duration < self._min_test_duration_s:
            return
        core_resistances = [s.resistance_ohm for s in core_samples if s.resistance_ohm is not None]
        start_res = start_sample.resistance_ohm
        end_res = end_sample.resistance_ohm
        delta_res = None
        if start_res is not None and end_res is not None:
            delta_res = end_res - start_res

        record = LoadTestRecord(
            started_at=pulse.started_at,
            ended_at=last_sample.ts,
            duration_s=duration,
            start_voltage_v=start_sample.voltage_v,
            end_voltage_v=end_sample.voltage_v,
            min_voltage_v=min(s.voltage_v for s in core_samples),
            start_resistance_ohm=start_res,
            min_resistance_ohm=min(core_resistances) if core_resistances else None,
            max_resistance_ohm=max(core_resistances) if core_resistances else None,
            delta_voltage_v=end_sample.voltage_v - start_sample.voltage_v,
            delta_resistance_ohm=delta_res,
            sample_count=len(pulse.samples),
        )
        saved = self._storage.add_test(record)
        if saved.id is not None:
            self._storage.add_samples(
                [
                    LoadTestSample(
                        test_id=saved.id,
                        t_s=max((sample.ts - pulse.started_at).total_seconds(), 0.0),
                        voltage_v=sample.voltage_v,
                        resistance_ohm=sample.resistance_ohm,
                    )
                    for sample in pulse.samples
                ]
            )
        self._status.total_tests += 1
        logger.info(
            "Saved load test: duration=%.3fs min_v=%.3fV min_r=%s",
            duration,
            record.min_voltage_v,
            f"{record.min_resistance_ohm:.6f}ohm" if record.min_resistance_ohm is not None else "n/a",
        )
