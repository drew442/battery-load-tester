import asyncio
import contextlib
import logging

import uvicorn

from .api import build_app
from .config import Settings, load_settings
from .load_test_service import LoadTestService
from .rc3563 import DemoStream, RC3563Stream
from .storage import TestStorage

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
)
logger = logging.getLogger(__name__)


def _build_stream(settings: Settings):
    if settings.demo_mode:
        logger.info("Running in demo mode")
        return DemoStream(sample_period_s=settings.sample_period_s)
    return RC3563Stream(
        port=settings.rc3563_port,
        baudrate=settings.rc3563_baudrate,
        serial_timeout_s=settings.rc3563_serial_timeout_s,
        sample_period_s=settings.sample_period_s,
    )


async def _monitor_loop(stream, service: LoadTestService) -> None:
    try:
        async for measurement in stream.measurements():
            await service.on_measurement(measurement)
    except asyncio.CancelledError:
        raise
    except Exception as exc:  # pragma: no cover - runtime safety
        logger.exception("Monitor loop crashed: %s", exc)
    finally:
        await service.mark_disconnected()


def create_app():
    settings = load_settings()
    storage = TestStorage(settings.db_path)
    service = LoadTestService(
        storage=storage,
        load_detect_voltage_drop_v=settings.load_detect_voltage_drop_v,
        battery_present_voltage_v=settings.battery_present_voltage_v,
        min_test_duration_s=settings.min_test_duration_s,
        pre_trigger_samples=settings.pre_trigger_samples,
    )
    stream = _build_stream(settings)
    app = build_app(service=service, storage=storage)

    @app.on_event("startup")
    async def startup_event() -> None:
        app.state.monitor_task = asyncio.create_task(_monitor_loop(stream, service))

    @app.on_event("shutdown")
    async def shutdown_event() -> None:
        monitor_task = getattr(app.state, "monitor_task", None)
        if monitor_task is not None:
            monitor_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await monitor_task

    return app


def run() -> None:
    settings = load_settings()
    uvicorn.run(
        "battery_load_tester.main:create_app",
        factory=True,
        host=settings.host,
        port=settings.port,
        reload=False,
    )


if __name__ == "__main__":
    run()
