from datetime import datetime, timezone

from battery_load_tester.models import LoadTestRecord, LoadTestSample
from battery_load_tester.storage import TestStorage


def test_storage_round_trip(tmp_path) -> None:
    store = TestStorage(str(tmp_path / "tests.db"))
    saved = store.add_test(
        LoadTestRecord(
            started_at=datetime.now(timezone.utc),
            ended_at=datetime.now(timezone.utc),
            duration_s=1.2,
            start_voltage_v=7.8,
            end_voltage_v=7.1,
            min_voltage_v=7.0,
            start_resistance_ohm=0.78,
            min_resistance_ohm=0.61,
            max_resistance_ohm=0.92,
            delta_voltage_v=-0.7,
            delta_resistance_ohm=-0.16,
            sample_count=8,
            note="first pass",
        )
    )

    assert saved.id is not None
    store.add_samples(
        [
            LoadTestSample(
                test_id=saved.id,
                t_s=0.0,
                voltage_v=7.8,
                resistance_ohm=0.78,
            ),
            LoadTestSample(
                test_id=saved.id,
                t_s=0.2,
                voltage_v=7.4,
                resistance_ohm=0.62,
            ),
        ]
    )
    rows = store.list_tests(limit=5)
    assert len(rows) == 1
    assert rows[0].delta_voltage_v == -0.7
    assert rows[0].delta_resistance_ohm == -0.16
    assert rows[0].sample_count == 8
    assert rows[0].start_resistance_ohm == 0.78
    assert rows[0].min_resistance_ohm == 0.61
    assert rows[0].max_resistance_ohm == 0.92
    assert rows[0].note == "first pass"

    samples = store.get_samples(saved.id)
    assert len(samples) == 2
    assert samples[1].resistance_ohm == 0.62

    store.update_note(saved.id, "updated note")
    updated = store.list_tests(limit=1)[0]
    assert updated.note == "updated note"


def test_group_and_sorting(tmp_path) -> None:
    store = TestStorage(str(tmp_path / "groups.db"))
    a = store.add_test(
        LoadTestRecord(
            started_at=datetime.now(timezone.utc),
            ended_at=datetime.now(timezone.utc),
            duration_s=1.0,
            start_voltage_v=7.9,
            end_voltage_v=7.2,
            min_voltage_v=7.1,
            start_resistance_ohm=0.79,
            min_resistance_ohm=0.7,
            max_resistance_ohm=0.82,
            delta_voltage_v=-0.7,
            delta_resistance_ohm=-0.09,
            sample_count=7,
            note="mod-a",
        )
    )
    b = store.add_test(
        LoadTestRecord(
            started_at=datetime.now(timezone.utc),
            ended_at=datetime.now(timezone.utc),
            duration_s=2.0,
            start_voltage_v=7.8,
            end_voltage_v=6.9,
            min_voltage_v=6.8,
            start_resistance_ohm=0.71,
            min_resistance_ohm=0.65,
            max_resistance_ohm=0.75,
            delta_voltage_v=-0.9,
            delta_resistance_ohm=-0.06,
            sample_count=8,
            note="mod-b",
        )
    )
    group = store.create_group("set-1", "nimh", 7.2, 6.5, 46.8, 25.0, 10.0, 2.0, 30.0)
    assert group.id is not None
    store.add_tests_to_group(group.id, [a.id, b.id])
    grouped = store.list_group_tests(group.id, sort_by="min_resistance_ohm", order="asc")
    assert grouped[0].id == b.id
    groups = store.list_groups()
    assert groups[0]["test_count"] == 2
