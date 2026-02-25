import sqlite3
from datetime import datetime
from pathlib import Path

from .models import BatteryGroup, LoadTestRecord, LoadTestSample

SORTABLE_TEST_COLUMNS = {
    "id": "id",
    "started_at": "started_at",
    "duration_s": "duration_s",
    "start_voltage_v": "start_voltage_v",
    "end_voltage_v": "end_voltage_v",
    "min_voltage_v": "min_voltage_v",
    "start_resistance_ohm": "start_resistance_ohm",
    "min_resistance_ohm": "min_resistance_ohm",
    "max_resistance_ohm": "max_resistance_ohm",
    "delta_voltage_v": "delta_voltage_v",
    "delta_resistance_ohm": "delta_resistance_ohm",
    "sample_count": "sample_count",
}


class TestStorage:
    def __init__(self, db_path: str) -> None:
        self._db_path = Path(db_path)
        self._init_db()

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self._db_path)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA foreign_keys = ON")
        return conn

    def _init_db(self) -> None:
        with self._connect() as conn:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS load_tests (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    started_at TEXT NOT NULL,
                    ended_at TEXT NOT NULL,
                    duration_s REAL NOT NULL,
                    start_voltage_v REAL NOT NULL,
                    end_voltage_v REAL NOT NULL,
                    min_voltage_v REAL NOT NULL,
                    start_current_a REAL NOT NULL,
                    end_current_a REAL NOT NULL,
                    peak_current_a REAL NOT NULL,
                    start_resistance_ohm REAL,
                    min_resistance_ohm REAL,
                    max_resistance_ohm REAL,
                    delta_voltage_v REAL NOT NULL,
                    delta_current_a REAL NOT NULL,
                    delta_resistance_ohm REAL,
                    sample_count INTEGER NOT NULL DEFAULT 0,
                    note TEXT
                )
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS load_test_samples (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    test_id INTEGER NOT NULL,
                    t_s REAL NOT NULL,
                    voltage_v REAL NOT NULL,
                    current_a REAL NOT NULL,
                    resistance_ohm REAL,
                    FOREIGN KEY(test_id) REFERENCES load_tests(id)
                )
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS groups (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    name TEXT NOT NULL UNIQUE,
                    chemistry TEXT NOT NULL,
                    module_nominal_v REAL NOT NULL,
                    capacity_ah REAL NOT NULL,
                    sticker_energy_wh REAL,
                    test_temperature_c REAL NOT NULL,
                    target_load_current_a REAL NOT NULL,
                    pulse_duration_s REAL NOT NULL,
                    rest_time_s REAL NOT NULL,
                    created_at TEXT NOT NULL
                )
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS group_tests (
                    group_id INTEGER NOT NULL,
                    test_id INTEGER NOT NULL,
                    PRIMARY KEY (group_id, test_id),
                    FOREIGN KEY(group_id) REFERENCES groups(id) ON DELETE CASCADE,
                    FOREIGN KEY(test_id) REFERENCES load_tests(id) ON DELETE CASCADE
                )
                """
            )
            conn.execute("CREATE INDEX IF NOT EXISTS idx_group_tests_group_id ON group_tests(group_id)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_group_tests_test_id ON group_tests(test_id)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_load_test_samples_test_id ON load_test_samples(test_id)")
            self._migrate_schema(conn)

    @staticmethod
    def _migrate_schema(conn: sqlite3.Connection) -> None:
        cols = {
            row["name"]
            for row in conn.execute("PRAGMA table_info(load_tests)").fetchall()
        }
        if "current_a" in cols:
            # Legacy column no longer used; keep for backward compatibility.
            pass
        if "start_resistance_ohm" not in cols:
            conn.execute("ALTER TABLE load_tests ADD COLUMN start_resistance_ohm REAL")
        if "min_resistance_ohm" not in cols:
            conn.execute("ALTER TABLE load_tests ADD COLUMN min_resistance_ohm REAL")
        if "max_resistance_ohm" not in cols:
            conn.execute("ALTER TABLE load_tests ADD COLUMN max_resistance_ohm REAL")
        if "start_current_a" not in cols:
            # Legacy column for older schemas; new inserts no longer use it.
            conn.execute("ALTER TABLE load_tests ADD COLUMN start_current_a REAL NOT NULL DEFAULT 0")
        if "end_current_a" not in cols:
            # Legacy column for older schemas; new inserts no longer use it.
            conn.execute("ALTER TABLE load_tests ADD COLUMN end_current_a REAL NOT NULL DEFAULT 0")
        if "delta_voltage_v" not in cols:
            conn.execute("ALTER TABLE load_tests ADD COLUMN delta_voltage_v REAL NOT NULL DEFAULT 0")
        if "delta_current_a" not in cols:
            # Legacy column for older schemas; new inserts no longer use it.
            conn.execute("ALTER TABLE load_tests ADD COLUMN delta_current_a REAL NOT NULL DEFAULT 0")
        if "delta_resistance_ohm" not in cols:
            conn.execute("ALTER TABLE load_tests ADD COLUMN delta_resistance_ohm REAL")
        if "sample_count" not in cols:
            conn.execute("ALTER TABLE load_tests ADD COLUMN sample_count INTEGER NOT NULL DEFAULT 0")
        if "note" not in cols:
            conn.execute("ALTER TABLE load_tests ADD COLUMN note TEXT")

        group_cols = {
            row["name"]
            for row in conn.execute("PRAGMA table_info(groups)").fetchall()
        }
        if "capacity_ah" not in group_cols:
            conn.execute("ALTER TABLE groups ADD COLUMN capacity_ah REAL NOT NULL DEFAULT 6.5")
        if "test_temperature_c" not in group_cols:
            conn.execute("ALTER TABLE groups ADD COLUMN test_temperature_c REAL NOT NULL DEFAULT 25.0")
        if "target_load_current_a" not in group_cols:
            conn.execute("ALTER TABLE groups ADD COLUMN target_load_current_a REAL NOT NULL DEFAULT 10.0")
        if "pulse_duration_s" not in group_cols:
            conn.execute("ALTER TABLE groups ADD COLUMN pulse_duration_s REAL NOT NULL DEFAULT 2.0")
        if "sticker_energy_wh" not in group_cols:
            conn.execute("ALTER TABLE groups ADD COLUMN sticker_energy_wh REAL")
        if "rest_time_s" not in group_cols:
            conn.execute("ALTER TABLE groups ADD COLUMN rest_time_s REAL NOT NULL DEFAULT 30.0")

    def add_test(self, record: LoadTestRecord) -> LoadTestRecord:
        with self._connect() as conn:
            cur = conn.execute(
                """
                INSERT INTO load_tests (
                    started_at,
                    ended_at,
                    duration_s,
                    start_voltage_v,
                    end_voltage_v,
                    min_voltage_v,
                    start_resistance_ohm,
                    min_resistance_ohm,
                    max_resistance_ohm,
                    delta_voltage_v,
                    delta_resistance_ohm,
                    sample_count,
                    note
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    record.started_at.isoformat(),
                    record.ended_at.isoformat(),
                    record.duration_s,
                    record.start_voltage_v,
                    record.end_voltage_v,
                    record.min_voltage_v,
                    record.start_resistance_ohm,
                    record.min_resistance_ohm,
                    record.max_resistance_ohm,
                    record.delta_voltage_v,
                    record.delta_resistance_ohm,
                    record.sample_count,
                    record.note,
                ),
            )
            return record.model_copy(update={"id": int(cur.lastrowid)})

    def add_samples(self, samples: list[LoadTestSample]) -> None:
        if not samples:
            return
        with self._connect() as conn:
            conn.executemany(
                """
                INSERT INTO load_test_samples (
                    test_id,
                    t_s,
                    voltage_v,
                    current_a,
                    resistance_ohm
                ) VALUES (?, ?, ?, ?, ?)
                """,
                [
                    (
                        sample.test_id,
                        sample.t_s,
                        sample.voltage_v,
                        0.0,
                        sample.resistance_ohm,
                    )
                    for sample in samples
                ],
            )

    def list_tests(self, limit: int = 100) -> list[LoadTestRecord]:
        return self.list_tests_sorted(limit=limit, sort_by="id", order="desc")

    def list_tests_sorted(
        self,
        limit: int = 100,
        sort_by: str = "id",
        order: str = "desc",
        since_id: int | None = None,
    ) -> list[LoadTestRecord]:
        sort_column = SORTABLE_TEST_COLUMNS.get(sort_by, "id")
        sort_order = "ASC" if order.lower() == "asc" else "DESC"
        with self._connect() as conn:
            if since_id is not None:
                rows = conn.execute(
                    """
                    SELECT id, started_at, ended_at, duration_s, start_voltage_v, end_voltage_v,
                           min_voltage_v,
                           start_resistance_ohm, min_resistance_ohm, max_resistance_ohm,
                           delta_voltage_v, delta_resistance_ohm, sample_count, note
                    FROM load_tests
                    WHERE id > ?
                    ORDER BY id ASC
                    LIMIT ?
                    """,
                    (since_id, limit),
                ).fetchall()
            else:
                rows = conn.execute(
                    f"""
                    SELECT id, started_at, ended_at, duration_s, start_voltage_v, end_voltage_v,
                           min_voltage_v,
                           start_resistance_ohm, min_resistance_ohm, max_resistance_ohm,
                           delta_voltage_v, delta_resistance_ohm, sample_count, note
                    FROM load_tests
                    ORDER BY {sort_column} {sort_order}
                    LIMIT ?
                    """,
                    (limit,),
                ).fetchall()

        return [self._row_to_record(row) for row in rows]

    def count_tests(self) -> int:
        with self._connect() as conn:
            row = conn.execute("SELECT COUNT(*) AS c FROM load_tests").fetchone()
            return int(row["c"])

    def get_samples(self, test_id: int) -> list[LoadTestSample]:
        with self._connect() as conn:
            rows = conn.execute(
                """
                SELECT test_id, t_s, voltage_v, resistance_ohm
                FROM load_test_samples
                WHERE test_id = ?
                ORDER BY t_s ASC
                """,
                (test_id,),
            ).fetchall()

        return [
            LoadTestSample(
                test_id=int(row["test_id"]),
                t_s=float(row["t_s"]),
                voltage_v=float(row["voltage_v"]),
                resistance_ohm=float(row["resistance_ohm"])
                if row["resistance_ohm"] is not None
                else None,
            )
            for row in rows
        ]

    def update_note(self, test_id: int, note: str | None) -> None:
        note_value = (note or "").strip()
        if note_value == "":
            note_value = None
        if note_value is not None and len(note_value) > 120:
            note_value = note_value[:120]
        with self._connect() as conn:
            conn.execute(
                "UPDATE load_tests SET note = ? WHERE id = ?",
                (note_value, test_id),
            )

    def clear_results(self) -> None:
        with self._connect() as conn:
            conn.execute("DELETE FROM load_test_samples")
            conn.execute("DELETE FROM group_tests")
            conn.execute("DELETE FROM load_tests")

    def delete_test(self, test_id: int) -> None:
        with self._connect() as conn:
            conn.execute("DELETE FROM load_test_samples WHERE test_id = ?", (test_id,))
            conn.execute("DELETE FROM group_tests WHERE test_id = ?", (test_id,))
            conn.execute("DELETE FROM load_tests WHERE id = ?", (test_id,))

    def create_group(
        self,
        name: str,
        chemistry: str,
        module_nominal_v: float,
        capacity_ah: float,
        sticker_energy_wh: float | None,
        test_temperature_c: float,
        target_load_current_a: float,
        pulse_duration_s: float,
        rest_time_s: float,
    ) -> BatteryGroup:
        with self._connect() as conn:
            now = datetime.utcnow().isoformat()
            cur = conn.execute(
                """
                INSERT INTO groups (
                    name, chemistry, module_nominal_v, capacity_ah, sticker_energy_wh,
                    test_temperature_c, target_load_current_a, pulse_duration_s, rest_time_s, created_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    name.strip(),
                    chemistry.strip().lower(),
                    module_nominal_v,
                    capacity_ah,
                    sticker_energy_wh,
                    test_temperature_c,
                    target_load_current_a,
                    pulse_duration_s,
                    rest_time_s,
                    now,
                ),
            )
            group_id = int(cur.lastrowid)
            row = conn.execute(
                """
                SELECT id, name, chemistry, module_nominal_v, capacity_ah, sticker_energy_wh,
                       test_temperature_c, target_load_current_a, pulse_duration_s, rest_time_s, created_at
                FROM groups
                WHERE id = ?
                """,
                (group_id,),
            ).fetchone()
        return self._row_to_group(row)

    def list_groups(self) -> list[dict]:
        with self._connect() as conn:
            rows = conn.execute(
                """
                SELECT g.id, g.name, g.chemistry, g.module_nominal_v, g.capacity_ah, g.sticker_energy_wh, g.created_at,
                       g.test_temperature_c, g.target_load_current_a, g.pulse_duration_s, g.rest_time_s,
                       COUNT(gt.test_id) AS test_count
                FROM groups g
                LEFT JOIN group_tests gt ON gt.group_id = g.id
                GROUP BY g.id, g.name, g.chemistry, g.module_nominal_v, g.capacity_ah,
                         g.sticker_energy_wh, g.test_temperature_c, g.target_load_current_a, g.pulse_duration_s, g.rest_time_s, g.created_at
                ORDER BY g.created_at DESC, g.id DESC
                """
            ).fetchall()
        return [
            {
                "group": self._row_to_group(row),
                "test_count": int(row["test_count"]),
            }
            for row in rows
        ]

    def delete_group(self, group_id: int) -> None:
        with self._connect() as conn:
            conn.execute("DELETE FROM groups WHERE id = ?", (group_id,))

    def clear_groups(self) -> None:
        with self._connect() as conn:
            conn.execute("DELETE FROM group_tests")
            conn.execute("DELETE FROM groups")

    def add_tests_to_group(self, group_id: int, test_ids: list[int]) -> None:
        if not test_ids:
            return
        with self._connect() as conn:
            conn.executemany(
                """
                INSERT OR IGNORE INTO group_tests (group_id, test_id)
                VALUES (?, ?)
                """,
                [(group_id, test_id) for test_id in test_ids],
            )

    def remove_test_from_group(self, group_id: int, test_id: int) -> None:
        with self._connect() as conn:
            conn.execute(
                "DELETE FROM group_tests WHERE group_id = ? AND test_id = ?",
                (group_id, test_id),
            )

    def get_group(self, group_id: int) -> BatteryGroup | None:
        with self._connect() as conn:
            row = conn.execute(
                """
                SELECT id, name, chemistry, module_nominal_v, capacity_ah, sticker_energy_wh,
                       test_temperature_c, target_load_current_a, pulse_duration_s, rest_time_s, created_at
                FROM groups
                WHERE id = ?
                """,
                (group_id,),
            ).fetchone()
        if row is None:
            return None
        return self._row_to_group(row)

    def list_group_tests(
        self,
        group_id: int,
        sort_by: str = "id",
        order: str = "desc",
    ) -> list[LoadTestRecord]:
        sort_column = SORTABLE_TEST_COLUMNS.get(sort_by, "id")
        sort_order = "ASC" if order.lower() == "asc" else "DESC"
        with self._connect() as conn:
            rows = conn.execute(
                f"""
                SELECT t.id, t.started_at, t.ended_at, t.duration_s, t.start_voltage_v, t.end_voltage_v,
                       t.min_voltage_v,
                       t.start_resistance_ohm, t.min_resistance_ohm, t.max_resistance_ohm,
                       t.delta_voltage_v, t.delta_resistance_ohm, t.sample_count, t.note
                FROM load_tests t
                INNER JOIN group_tests gt ON gt.test_id = t.id
                WHERE gt.group_id = ?
                ORDER BY t.{sort_column} {sort_order}
                """,
                (group_id,),
            ).fetchall()
        return [self._row_to_record(row) for row in rows]

    @staticmethod
    def _row_to_record(row: sqlite3.Row) -> LoadTestRecord:
        return LoadTestRecord(
            id=int(row["id"]),
            started_at=datetime.fromisoformat(row["started_at"]),
            ended_at=datetime.fromisoformat(row["ended_at"]),
            duration_s=float(row["duration_s"]),
            start_voltage_v=float(row["start_voltage_v"]),
            end_voltage_v=float(row["end_voltage_v"]),
            min_voltage_v=float(row["min_voltage_v"]),
            start_resistance_ohm=float(row["start_resistance_ohm"])
            if row["start_resistance_ohm"] is not None
            else None,
            min_resistance_ohm=float(row["min_resistance_ohm"])
            if row["min_resistance_ohm"] is not None
            else None,
            max_resistance_ohm=float(row["max_resistance_ohm"])
            if row["max_resistance_ohm"] is not None
            else None,
            delta_voltage_v=float(row["delta_voltage_v"]),
            delta_resistance_ohm=float(row["delta_resistance_ohm"])
            if row["delta_resistance_ohm"] is not None
            else None,
            sample_count=int(row["sample_count"]),
            note=row["note"],
        )

    @staticmethod
    def _row_to_group(row: sqlite3.Row) -> BatteryGroup:
        return BatteryGroup(
            id=int(row["id"]),
            name=row["name"],
            chemistry=row["chemistry"],
            module_nominal_v=float(row["module_nominal_v"]),
            capacity_ah=float(row["capacity_ah"]),
            sticker_energy_wh=float(row["sticker_energy_wh"]) if row["sticker_energy_wh"] is not None else None,
            test_temperature_c=float(row["test_temperature_c"]),
            target_load_current_a=float(row["target_load_current_a"]),
            pulse_duration_s=float(row["pulse_duration_s"]),
            rest_time_s=float(row["rest_time_s"]),
            created_at=datetime.fromisoformat(row["created_at"]),
        )
