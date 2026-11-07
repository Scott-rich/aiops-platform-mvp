PYTHON ?= .venv/bin/python
OUTPUT ?= /tmp/aiops-demo-$(shell date +%Y%m%d-%H%M%S)
ifeq ($(PYTHON),.venv/bin/python)
DEMO_ENV = .venv/bin/python
endif

.PHONY: demo check research-env acceptance cluster-plan
.venv/bin/python: requirements-runtime-lock.txt
	python3 -m venv .venv
	.venv/bin/python -m pip install -r requirements-runtime-lock.txt
	.venv/bin/python -m pip check

demo: $(DEMO_ENV)
	$(PYTHON) -m scripts.research.demo --output "$(OUTPUT)"

research-env: $(DEMO_ENV)
	$(PYTHON) -m pip install -r requirements-lock.txt
	$(PYTHON) -m pip check

check:
	$(PYTHON) -m ruff check app research scripts/research
	$(PYTHON) -m ruff format --check app research scripts/research
	$(PYTHON) -m mypy
	$(PYTHON) -m coverage run -m pytest tests research/tests -q
	$(PYTHON) -m coverage report

acceptance:
	$(PYTHON) -m scripts.acceptance --output "$(OUTPUT)"

cluster-plan:
	$(PYTHON) -m scripts.research.cluster_experiment --output "$(OUTPUT)"
