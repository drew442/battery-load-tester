# Battery Load Tester

Starter project for automated battery module load testing using:

- RC3563 (USB/serial measurement stream)
- 555 timer board (fixed load duration)
- 30A MOSFET heater board (switching load)
- H7 halogen globes (resistive load)

Initial target battery is the Toyota Prius (2005) NiMH 6S module.

## What this scaffold includes

- FastAPI backend
- RC3563 serial reader abstraction
- Load-test event detection based on measured current
- SQLite persistence of test results
- Basic web dashboard with live status + recent tests
- Demo mode to run without hardware

## Quick start

1. Create and activate a virtual environment.
2. Install dependencies:
   - `pip install -e .[dev]`
3. Edit `config.yaml` and set `rc3563_port` for your machine.
4. Run the app:
   - `battery-load-tester`
5. Open:
   - `http://127.0.0.1:8000`

## Configuration file

- The app reads `config.yaml` from the repo root by default.
- You can also use JSON (`config.json`) by setting `BLT_CONFIG_FILE`.

Default `config.yaml`:

```yaml
host: 127.0.0.1
port: 8000
db_path: ./battery_load_tester.db

rc3563_port: COM4
rc3563_baudrate: 115200
rc3563_serial_timeout_s: 0.2
demo_mode: false
sample_period_s: 0.2

load_detect_voltage_drop_v: 0.15
battery_present_voltage_v: 1.0
min_test_duration_s: 0.5
pre_trigger_samples: 6
```

## Notes on hardware/software behavior

- The physical button and 555 timer control the load timing in hardware.
- Software does not trigger the load in this first revision.
- RC3563 data is read as 10-byte binary packets at 115200 baud.
- RC3563 provides voltage and resistance; load current is not measured.
- Software detects start/end of a load pulse by watching voltage drop from a baseline.
- Each saved result now stores `R Start`, `R Min`, `R Max`, and a short operator note.
- A load test record is created when voltage drops by the configured threshold, then returns toward baseline.

## Grouping and ranking

- You can select results and add them to a named group.
- Group ranking is blocked until all grouped results have notes.
- You can use the group `Rest Time` and the UI wait/test indicator before pressing the physical test button.
- Group specification now requires:
  - chemistry
  - module nominal voltage
  - capacity (Ah)
  - sticker energy (Wh, optional)
  - test temperature (C)
  - target load current (A)
  - pulse duration (s)
  - rest time before test (s)

## Next implementation targets

- Add module identity workflow (scan/enter module serial before each pulse).
- Add pass/fail thresholds and charting.
- Add export (CSV/JSON) and calibration metadata.
