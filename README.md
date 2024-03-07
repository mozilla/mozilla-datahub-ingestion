# mozilla-datahub-ingestion

This repository contains code for sending metadata from Mozilla-specific platforms to a DataHub instance.

The production instance is https://mozilla.acryl.io 

## Setting up a new DataHub instance

The recipes that handle Looker and BigQuery metadata are managed via UI Ingestion and stored by SRE. Ask in 
`#data-help` for assistance.

To bootstrap the custom platforms we ingest metadata for, run the `platform_recipe.dhub.yaml` recipe:

`$ DATAHUB_GMS_URL=... DATAHUB_GMS_TOKEN=... datahub ingest -c recipes/platform_recipe.dhub.yaml`

All other recipes can be found in the `recipes` directory and can be run similarly using the `datahub ingest` command.

## Development

```
├── recipes (.dhub.yaml recipe files - https://datahubproject.io/docs/metadata-ingestion#recipes)
├── sync (source code for metadata fetching and ingestion)
│   ├── datahub (source code for DataHub utils and custom Ingestion Sources - https://datahubproject.io/docs/metadata-ingestion/adding-source)
└── tests (source code and sample data for tests)
```

To install a local instance of DataHub, see [DataHub's Quickstart guide](https://datahubproject.io/docs/quickstart/).

Start a DataHub instance locally: `datahub docker quickstart`
The initial run will install various packages and can take well over 30 minutes. DataHub will keep running in the background.

Build the YAML configuration files for syncing to DataHub from specific ingestion sources: `python3 -m sync.datahub.<ingestion_source>`
For available ingestion sources see [`sync/datahub/`](https://github.com/mozilla/mozilla-datahub-ingestion/tree/main/sync/datahub).

Ingest data from a specific source: `DATAHUB_GMS_URL="http://localhost:8080" DATAHUB_GMS_TOKEN=None datahub ingest -c recipes/<ingestion_source>.dhub.yaml`.

The local DataHub instance can by default be accessed via: http://localhost:9002/

### Prerequisites 

- [Python](https://www.python.org/) (version 3.10+)
- [Docker](https://www.docker.com/): DataHub uses Docker for local development and
  deployment.
    - [docker](https://docs.docker.com/engine/installation/#supported-platforms)
    - [docker compose](https://docs.docker.com/compose/install/)


### Setup
1. Create a virtual environment: `$ python -m venv venv`

2. Install project dependencies: `$ pip install -r requirements.txt`. This should include the [DataHub CLI](https://datahubproject.io/docs/quickstart/).

3. Install the module locally: `$ pip install -e .`


### Linting

To test whether the code conforms to the linting rules, you can
run `make lint` to check Python and Yaml styles.

Running `make format` will auto-format the code according to the
[style rules](https://black.readthedocs.io/en/stable/the_black_code_style/current_style.html).

## Appendix 
DataHub - https://datahubproject.io/

Recipe - https://datahubproject.io/docs/metadata-ingestion#recipes
