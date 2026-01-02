UV ?= uv

.PHONY: setup test run report contract-test discover run-offline report-offline

setup:
	$(UV) venv .venv
	$(UV) pip install -r requirements.lock
	$(UV) pip install -e .

test:
	$(UV) run pytest

run:
	$(UV) run pm_arb scan

report:
	$(UV) run pm_arb report

contract-test:
	$(UV) run pm_arb contract-test

discover:
	$(UV) run pm_arb discover

run-offline:
	$(UV) run pm_arb scan --offline --fixtures-dir testdata/fixtures

report-offline:
	$(UV) run pm_arb report
