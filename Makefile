UV ?= uv

.PHONY: setup test discover capture-bench-offline capture-verify

setup:
	$(UV) venv .venv
	$(UV) pip install -r requirements.lock
	$(UV) pip install -e .

test:
	$(UV) run python -m pytest

discover:
	$(UV) run pm_data discover

capture-bench-offline:
	$(UV) run pm_data capture-bench --offline --fixtures-dir testdata/fixtures

capture-verify:
	$(UV) run pm_data capture-verify --run-dir $(RUN_DIR)
