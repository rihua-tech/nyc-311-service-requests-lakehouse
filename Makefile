PYTHON ?= python

.PHONY: install test lint check

install:
	$(PYTHON) -m pip install -r requirements.txt

test:
	$(PYTHON) -m pytest

lint:
	$(PYTHON) -m ruff check .

check: test lint
