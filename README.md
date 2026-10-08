# mozilla-datahub-ingestion

This repository contains code for sending metadata from Mozilla-specific platforms to a DataHub instance.

The production instance is https://mozilla.acryl.io 

## Setting up a new DataHub instance

The recipes that handle Looker and BigQuery metadata are managed via UI Ingestion and stored by SRE. Ask in 
`#data-help` for assistance.

To bootstrap the custom platforms we ingest metadata for, run the `platform_recipe.dhub.yaml` recipe:

`$ DATAHUB_GMS_URL=... DATAHUB_GMS_TOKEN=... datahub ingest -c recipes/platform_recipe.dhub.yaml`

The Redash usage source writes to structured properties that must exist first. Create or update them with:

`$ DATAHUB_GMS_URL=... DATAHUB_GMS_TOKEN=... datahub ingest -c recipes/redash_structured_properties_recipe.dhub.yaml`

All other recipes can be found in the `recipes` directory and can be run similarly using the `datahub ingest` command.

## Scheduled ingestion

The custom sources in this repo run on a schedule in one of two places.

### CircleCI

The `nightly` workflow in `.circleci/config.yml` runs daily at 00:00 UTC on `main`. It runs the
unit tests, then one `datahub-ingest` job per recipe:

| Job | Recipe | What it writes |
|---|---|---|
| `glean-source` | `glean_recipe.dhub.yaml` | Glean pings as datasets, with lineage to their BigQuery tables |
| `legacytelemetry-source` | `legacy_recipe.dhub.yaml` | Legacy telemetry pings as datasets, with lineage to their BigQuery tables |
| `metrichub-glossary-source` | `metrichub_recipe.dhub.yaml` | Metric Hub metrics as glossary terms, then links terms to tables with `datahub dataset upsert` |
| `bigquery-etl-source` | `bigquery_etl_recipe.dhub.yaml` | Links from BigQuery tables to their bigquery-etl source and Airflow DAG |

Each job installs `requirements.txt` and runs the recipe from the checkout, so changes take
effect on the first nightly run after they merge. `DATAHUB_GMS_URL` is set in the config and
`DATAHUB_GMS_TOKEN` comes from the CircleCI project's environment variables. To schedule
another source here, add its recipe and a `datahub-ingest` job to the `nightly` workflow.

### Acryl UI ingestion

The Redash usage source (`sync.datahub.redash_usage_source.RedashUsageSource`) runs as a
UI-managed source in Acryl under Ingestion > Sources instead, because it reads BigQuery with the
service account credentials that the BigQuery source already stores there as secrets. It writes
usage, owners, and structured properties to the Redash charts and dashboards ingested by the
Redash source, reading the `stmo.object_views_daily` and `stmo.object_owners` views from
bigquery-etl.

The source's settings in Acryl:

- Recipe: the same as `recipes/redash_usage_recipe.dhub.yaml`, without the sink (the executor adds
  one) and with a `credential` block whose values and `${SECRET}` references are copied from the
  BigQuery source's recipe.
- Extra pip packages: `git+https://github.com/mozilla/mozilla-datahub-ingestion.git@<sha>`. The
  executor installs this repo from git on every run, so pinning a commit means changes on `main`
  only take effect when the SHA in the source is updated.
- Schedule: daily at 12:00 UTC, after the `bqetl_stmo` DAG loads `stmo_external` at 08:00 UTC (best-effort).
  The order relative to the Redash source doesn't matter, since the two write different aspects.

The executor runs a newer DataHub CLI than `requirements.txt` pins (1.7 on Python 3.11 as of
October 2026), so test changes to this source against that version as well. Apply
`redash_structured_properties_recipe.dhub.yaml` before deploying a change that adds or renames a
structured property, or DataHub rejects the source's patches.

Redash, Looker, BigQuery, and other sources are also UI-managed in Acryl, but they are
built-in DataHub sources and aren't part of this repo.

## Development

```
├── recipes (.dhub.yaml recipe files - https://datahubproject.io/docs/metadata-ingestion#recipes)
├── sync (source code for metadata fetching and ingestion)
│   ├── datahub (source code for DataHub utils and custom Ingestion Sources - https://datahubproject.io/docs/metadata-ingestion/adding-source)
└── tests (source code and sample data for tests)
```

To install a local instance of DataHub, see [DataHub's Quickstart guide](https://datahubproject.io/docs/quickstart/).

Start a DataHub instance locally: Launch Docker Desktop, then run `datahub docker quickstart`
The initial run will install various packages and can take well over 30 minutes. DataHub will keep running in the background.

Ingest data from a specific source: `DATAHUB_GMS_URL="http://localhost:8080" DATAHUB_GMS_TOKEN=None datahub ingest -c recipes/<ingestion_source>.dhub.yaml`.

The local DataHub instance can by default be accessed via: http://localhost:9002/

### Prerequisites 

- [Python](https://www.python.org/) (version 3.10)
- [Docker](https://www.docker.com/): DataHub uses Docker for local development and
  deployment.
    - [docker](https://docs.docker.com/engine/installation/#supported-platforms)
    - [docker compose](https://docs.docker.com/compose/install/)


### Setup
1. Create a virtual environment: `$ python -m venv venv`

2. Activate the virtual environment: `$ source venv/bin/activate`

3. Install project dependencies: `$ pip install -r requirements.txt`. This should include the [DataHub CLI](https://datahubproject.io/docs/quickstart/).

4. Install the module locally: `$ pip install -e .`


### Linting

To test whether the code conforms to the linting rules, you can
run `make lint` to check Python and Yaml styles.

Running `make format` will auto-format the code according to the
[style rules](https://black.readthedocs.io/en/stable/the_black_code_style/current_style.html).

## Appendix 
DataHub - https://datahubproject.io/

Recipe - https://datahubproject.io/docs/metadata-ingestion#recipes
