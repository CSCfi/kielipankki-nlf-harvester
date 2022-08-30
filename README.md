# NFL Data Harvester
This tool allows fetching data from the National Library of Finland. This tool
interacts with the OAI-PMH API of NFL, fetching information about the available
bindings, downloading them and checking their integrity.

## Getting Started

### Requirements
This package is guaranteed to work with Python 3.6, but newer versions are
likely to work too. In addition, you need pip and virtualenv. The rest of the
requirements can then be installed with `make requirements`.

### Quickstart
Print all binding IDs in a collection:
```
python harvester_cli.py binding_ids [--url=https://digi.kansalliskirjasto.fi/interfaces/OAI-PMH] COLLECTION_ID
```
If harvesting the default NLF API, the url can be omitted. E.g.
```
python harvester_cli.py binding-ids col-681
```


## Development

### Installing Development Requirements
To install additional requirements needed when development work, install
requirements from `requirements_dev.txt`. Using a virtualenv for this is
recommended:
```
virtualenv .venv -p python3
. .venv/bin/activate
pip install -r requirements_dev.txt
```

### Running Unit Tests
Without coverage information:
```
python -m pytest
```

With coverage:
```
python -m pytest --cov=harvester
```

### Generating Documentation
The module supports creating documentation from docstrings using Sphinx. The
generated docs are very rudimentary at this point, but they can be generated as
follows:
```
sphinx-apidoc -o docs/ harvester
cd docs
make html
```
Afterwards the documentation can be viewed from ``docs/_build/index.html``.
