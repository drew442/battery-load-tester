import struct

from battery_load_tester.rc3563 import parse_rc3563_packet


def _u24(value: int) -> bytes:
    return value.to_bytes(4, "little")[:3]


def test_parse_valid_packet() -> None:
    pkt = struct.pack(
        "BB3sBB3s",
        0x94,  # R display code=0x9 (ohm), V display code=0x4 (normal)
        0x03,  # R range auto
        _u24(6500),  # 0.65 ohm
        0x01,  # positive
        0x03,  # V range auto
        _u24(78000),  # 7.8 V
    )
    m = parse_rc3563_packet(pkt)
    assert m is not None
    assert m.voltage_v == 7.8
    assert m.resistance_ohm == 0.65
    assert m.current_a == 0.0


def test_parse_overflow_resistance_packet() -> None:
    pkt = struct.pack(
        "BB3sBB3s",
        0xA4,  # R display code=0xA (overflow), V display code=0x4
        0x03,
        _u24(0),
        0x01,
        0x03,
        _u24(76000),
    )
    m = parse_rc3563_packet(pkt)
    assert m is not None
    assert m.voltage_v == 7.6
    assert m.resistance_ohm is None
    assert m.current_a == 0.0


def test_parse_invalid_packet() -> None:
    assert parse_rc3563_packet(b"\x00\x01") is None
