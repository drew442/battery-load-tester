import asyncio
import logging
import random
import struct
from datetime import datetime, timezone
from typing import AsyncIterator

import serial

from .models import Measurement

logger = logging.getLogger(__name__)

PACKET_LENGTH = 10


def parse_rc3563_packet(pkt: bytes) -> Measurement | None:
    if len(pkt) != PACKET_LENGTH:
        return None

    try:
        status_display, _r_range_code, r_display_bytes, sign_code, _v_range_code, v_display_bytes = struct.unpack(
            "BB3sBB3s",
            pkt,
        )
    except struct.error:
        return None

    if sign_code not in (0, 1):
        return None
    r_display_code = (status_display & 0xF0) >> 4
    v_display_code = status_display & 0x0F

    r_display_value = struct.unpack("<I", r_display_bytes + b"\x00")[0] / 10000.0
    v_display_value = struct.unpack("<I", v_display_bytes + b"\x00")[0] / 10000.0

    sign = 1.0 if sign_code == 1 else -1.0
    voltage = sign * v_display_value

    resistance_ohm: float | None
    if r_display_code == 0x05:
        resistance_ohm = r_display_value * 0.001
    elif r_display_code == 0x09:
        resistance_ohm = r_display_value
    else:
        # Preserve datapoints even if display code is unfamiliar.
        resistance_ohm = r_display_value if r_display_value > 0 else None

    if v_display_code == 0x08:
        # Voltage overflow/no reading.
        voltage = 0.0

    return Measurement(
        ts=datetime.now(timezone.utc),
        voltage_v=voltage,
        resistance_ohm=resistance_ohm,
    )


class RC3563Stream:
    def __init__(
        self,
        port: str,
        baudrate: int,
        serial_timeout_s: float,
        sample_period_s: float,
    ) -> None:
        self._port = port
        self._baudrate = baudrate
        self._serial_timeout_s = serial_timeout_s
        self._sample_period_s = sample_period_s

    async def measurements(self) -> AsyncIterator[Measurement]:
        while True:
            try:
                with serial.Serial(self._port, self._baudrate, timeout=self._serial_timeout_s) as ser:
                    logger.info("Connected to RC3563 on %s", self._port)
                    while True:
                        pkt = await asyncio.to_thread(ser.read, PACKET_LENGTH)
                        if len(pkt) == 0:
                            continue
                        if len(pkt) != PACKET_LENGTH:
                            await asyncio.to_thread(ser.reset_input_buffer)
                            continue
                        measurement = parse_rc3563_packet(pkt)
                        if measurement is not None:
                            yield measurement
            except Exception as exc:  # pragma: no cover - hardware dependent
                logger.warning("RC3563 stream error: %s", exc)
                await asyncio.sleep(2.0)


class DemoStream:
    def __init__(self, sample_period_s: float) -> None:
        self._sample_period_s = sample_period_s
        self._tick = 0

    async def measurements(self) -> AsyncIterator[Measurement]:
        while True:
            self._tick += 1
            phase = self._tick % 120

            if phase < 70:
                voltage = random.uniform(7.7, 8.0)
                resistance = random.uniform(0.02, 0.08)
            else:
                voltage = random.uniform(6.1, 7.2)
                resistance = random.uniform(0.03, 0.12)

            yield Measurement(
                ts=datetime.now(timezone.utc),
                voltage_v=round(voltage, 3),
                resistance_ohm=round(resistance, 4),
            )
            await asyncio.sleep(self._sample_period_s)
