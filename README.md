# NFL Data Harvester
This tool allows fetching data from the National Library of Finland. This tool
interacts with the OAI-PMH API of NFL, fetching information about the available
bindings, downloading them and checking their integrity.

## Getting Started

### Requirements
This package is guaranteed to work with Python 3.8, but any version starting
from 3.6 is likely to work. In addition, you need pip and virtualenv. The rest
of the requirements can then be installed with `make requirements`.

### Quickstart
Print all binding IDs in a collection:
```
python harvester_cli.py binding_ids [--url=https://digi.kansalliskirjasto.fi/interfaces/OAI-PMH] COLLECTION_ID
```
If harvesting the default NLF API, the url can be omitted. E.g.
```
python harvester_cli.py binding-ids col-681
```
