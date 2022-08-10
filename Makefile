.venv:
	virtualenv .venv -p python3

requirements: .venv
	. .venv/bin/activate
	pip install -r requirements_dev.txt

test: requirements
	python -m pytest

coverage: requirements
	python -m pytest --cov=harvester
