# mozilla-datahub-ingestion-prototype

This repository contains scripts for sending metadata to a DataHub instance.

Rough project structure (some files omitted):
```
|-- recipe_templates/               -- templates for generated recipes
|-- recipes/                        -- recipes run in production
|-- sync/                           -- source for the scripts (CLI) 
|-- lookml_repositories.sample.yaml -- configuration for lookml repositories
|-- requirements.in                 -- pinned dependencies
|-- requirements.txt                -- generated dependencies
```

## Development

To install a local instance of DataHub, see [DataHub's Quickstart guide](https://datahubproject.io/docs/quickstart/).

### Prerequisites 

- [Python](https://www.python.org/) (version 3.7+)
- [Docker](https://www.docker.com/): DataHub uses Docker for local development and
  deployment.
    - [docker](https://docs.docker.com/engine/installation/#supported-platforms)
    - [docker compose](https://docs.docker.com/compose/install/)


### Setup
1. Create a virtual environment: `$ python -m venv venv`


2. Install project dependencies: `$ pip install -r requirements.txt`

This should include the [DataHub CLI](https://datahubproject.io/docs/quickstart/): The DataHub CLI is
  a command line interface for interacting with DataHub.


3. Install the CLI locally: `$ pip install -e .`


4. Run `$ datahub-sync --help` for usage.


## Appendix 

Recipe - https://datahubproject.io/docs/metadata-ingestion#recipes

