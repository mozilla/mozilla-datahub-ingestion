# mozilla-datahub-ingestion

This repository contains code for sending metadata from Mozilla-specific platforms to a DataHub instance.

The main entrypoint is the datahub-sync CLI, see `datahub-sync --help` and the Setup section for
more details.

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

3. Install the CLI locally: `$ pip install -e .`

4. Run `$ datahub-sync --help` for usage.


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
- Tests
- Airflow integration
- Reference copy on metadata