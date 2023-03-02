# mozilla-datahub-ingestion

This repository contains code for sending metadata from Mozilla-specific platforms to a DataHub instance.

## Setting up a new DataHub instance

The recipes that handle Looker and BigQuery metadata are managed via UI Ingestion and stored by SRE. Ask in 
`#data-help` for assistance.

To bootstrap the custom platforms we ingest metadata for, run the `platform_recipe.dhub.yaml` recipe:

`$ DATAHUB_GMS_URL=... DATAHUB_GMS_TOKEN=... datahub ingest -c recipes/platform_recipe.dhub.yaml`

All other recipes can be found in the `recipes` directory and can be run similarly using the `datahub ingest` command.

## Development

To install a local instance of DataHub, see [DataHub's Quickstart guide](https://datahubproject.io/docs/quickstart/).

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

TODO: 
- Architecture diagram
- Reference copy on metadata