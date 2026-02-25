from pathlib import Path

from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates
from pydantic import BaseModel, Field

from .load_test_service import LoadTestService
from .models import GroupRankingEntry, LoadTestRecord
from .storage import TestStorage

TEMPLATES = Jinja2Templates(directory=str(Path(__file__).parent / "templates"))


class NotePayload(BaseModel):
    note: str | None = Field(default=None, max_length=120)


class CreateGroupPayload(BaseModel):
    name: str = Field(min_length=1, max_length=80)
    chemistry: str = Field(min_length=1, max_length=30)
    module_nominal_v: float = Field(gt=0.0)
    capacity_ah: float = Field(gt=0.0)
    sticker_energy_wh: float | None = Field(default=None, gt=0.0)
    test_temperature_c: float
    target_load_current_a: float = Field(gt=0.0)
    pulse_duration_s: float = Field(gt=0.0)
    rest_time_s: float = Field(default=30.0, gt=0.0)


class GroupTestsPayload(BaseModel):
    test_ids: list[int]


def _normalize_low(values: list[float]) -> list[float]:
    if not values:
        return []
    vmin = min(values)
    vmax = max(values)
    span = vmax - vmin
    if span <= 1e-12:
        return [1.0 for _ in values]
    return [(vmax - v) / span for v in values]


def _rank_group_tests(tests: list[LoadTestRecord]) -> list[GroupRankingEntry]:
    resistance_values = [
        (test.min_resistance_ohm if test.min_resistance_ohm is not None else float("inf"))
        for test in tests
    ]
    voltage_drop_values = [max(test.start_voltage_v - test.end_voltage_v, 0.0) for test in tests]

    resistance_scores = _normalize_low(resistance_values)
    voltage_drop_scores = _normalize_low(voltage_drop_values)

    scored = []
    for idx, test in enumerate(tests):
        score = 0.6 * resistance_scores[idx] + 0.4 * voltage_drop_scores[idx]
        scored.append(
            GroupRankingEntry(
                rank=0,
                test_id=test.id if test.id is not None else -1,
                note=test.note or "",
                score=score,
                min_resistance_ohm=test.min_resistance_ohm,
                voltage_drop_v=max(test.start_voltage_v - test.end_voltage_v, 0.0),
            )
        )
    scored.sort(key=lambda item: item.score, reverse=True)
    for i, item in enumerate(scored, start=1):
        item.rank = i
    return scored


def _relative_spread(values: list[float]) -> float:
    if not values:
        return 0.0
    mean = sum(values) / len(values)
    if abs(mean) < 1e-12:
        return 0.0
    return (max(values) - min(values)) / abs(mean)


def build_app(service: LoadTestService, storage: TestStorage) -> FastAPI:
    app = FastAPI(title="Battery Load Tester")

    @app.get("/", response_class=HTMLResponse)
    async def home(request: Request) -> HTMLResponse:
        return TEMPLATES.TemplateResponse(request, "index.html", {})

    @app.get("/api/status")
    async def get_status():
        return await service.get_status()

    @app.get("/api/tests")
    async def get_tests(
        limit: int = 50,
        sort_by: str = "id",
        order: str = "desc",
        since_id: int | None = None,
    ):
        return storage.list_tests_sorted(limit=limit, sort_by=sort_by, order=order, since_id=since_id)

    @app.delete("/api/tests")
    async def clear_tests():
        storage.clear_results()
        await service.refresh_total_tests()
        return {"ok": True}

    @app.delete("/api/tests/{test_id}")
    async def delete_test(test_id: int):
        storage.delete_test(test_id)
        await service.refresh_total_tests()
        return {"ok": True}

    @app.get("/api/tests/{test_id}/samples")
    async def get_test_samples(test_id: int):
        return storage.get_samples(test_id=test_id)

    @app.put("/api/tests/{test_id}/note")
    async def update_test_note(test_id: int, payload: NotePayload):
        storage.update_note(test_id=test_id, note=payload.note)
        return {"ok": True}

    @app.get("/api/groups")
    async def get_groups():
        return storage.list_groups()

    @app.post("/api/groups")
    async def create_group(payload: CreateGroupPayload):
        return storage.create_group(
            name=payload.name,
            chemistry=payload.chemistry,
            module_nominal_v=payload.module_nominal_v,
            capacity_ah=payload.capacity_ah,
            sticker_energy_wh=payload.sticker_energy_wh,
            test_temperature_c=payload.test_temperature_c,
            target_load_current_a=payload.target_load_current_a,
            pulse_duration_s=payload.pulse_duration_s,
            rest_time_s=payload.rest_time_s,
        )

    @app.delete("/api/groups")
    async def clear_groups():
        storage.clear_groups()
        return {"ok": True}

    @app.delete("/api/groups/{group_id}")
    async def delete_group(group_id: int):
        storage.delete_group(group_id)
        return {"ok": True}

    @app.post("/api/groups/{group_id}/tests")
    async def add_tests_to_group(group_id: int, payload: GroupTestsPayload):
        storage.add_tests_to_group(group_id, payload.test_ids)
        return {"ok": True}

    @app.get("/api/groups/{group_id}/tests")
    async def get_group_tests(group_id: int, sort_by: str = "id", order: str = "desc"):
        return storage.list_group_tests(group_id=group_id, sort_by=sort_by, order=order)

    @app.delete("/api/groups/{group_id}/tests/{test_id}")
    async def remove_group_test(group_id: int, test_id: int):
        storage.remove_test_from_group(group_id=group_id, test_id=test_id)
        return {"ok": True}

    @app.get("/api/groups/{group_id}/rank")
    async def rank_group(group_id: int):
        group = storage.get_group(group_id)
        if group is None:
            raise HTTPException(status_code=404, detail="Group not found")
        if group.chemistry.lower() != "nimh":
            raise HTTPException(
                status_code=400,
                detail="Ranking currently supported for NiMH groups only.",
            )

        tests = storage.list_group_tests(group_id=group_id, sort_by="id", order="asc")
        if not tests:
            raise HTTPException(status_code=400, detail="Group has no tests")

        missing_notes = [test.id for test in tests if not (test.note or "").strip()]
        if missing_notes:
            raise HTTPException(
                status_code=400,
                detail={
                    "message": "Ranking cannot continue because one or more grouped results are missing notes.",
                    "why": "Each note should identify which physical module produced that result so the ranking can be traced back correctly.",
                    "action": "Add a clear label in the Note field for every listed test, then run ranking again.",
                    "missing_note_test_ids": missing_notes,
                },
            )

        ranked = _rank_group_tests(tests)
        warnings = [
            "Ranking is relative and based on pulse behavior (resistance + voltage sag), not full capacity SOH."
        ]
        dur_spread = _relative_spread([t.duration_s for t in tests])
        if dur_spread > 0.15:
            warnings.append("Pulse duration spread is high (>15%); ranking confidence is reduced.")
        return {
            "group": group,
            "warnings": warnings,
            "ranking_method": "score = 0.6 * low-resistance rank + 0.4 * low-voltage-sag rank",
            "ranked_results": ranked,
        }

    return app
