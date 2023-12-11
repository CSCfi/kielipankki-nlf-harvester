# Getting Started

## Requirements
This package is guaranteed to work with Python 3.8. This is dictated by Airflow
not supporting newer versions of Python at this time.

You also need to have pip and virtualenv installed. The rest of the
requirements can then be installed with `make requirements`.

## Quickstart
Print all binding IDs in a collection:
```
python harvester_cli.py binding_ids [--url=https://digi.kansalliskirjasto.fi/interfaces/OAI-PMH] COLLECTION_ID
```
If harvesting the default NLF API, the url can be omitted. E.g.
```
python harvester_cli.py binding-ids col-681
```

For more commands, see `python harvester_cli.py --help`.


# Development

## Installing Development Requirements
To install additional requirements needed when development work, install
requirements from `requirements_dev.txt`. Using a virtualenv for this is
recommended:
```
virtualenv .venv -p python3
. .venv/bin/activate
make requirements
```

If you want to test the Airflow pipelines too, you need to install some
additional packages:
```
pip install -r pipeline/requirements.txt
```

## Running Unit Tests
Without coverage information:
```
python -m pytest
```

With coverage:
```
python -m pytest --cov=harvester
```
