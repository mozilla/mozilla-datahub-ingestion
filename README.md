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

The custom sources in this repo run as UI-managed sources in Acryl under Ingestion > Sources:

| Source | Recipe | Schedule | What it writes |
|---|---|---|---|
| Glean | `glean_recipe.dhub.yaml` | daily 00:00 UTC | Glean pings as datasets, with lineage to their BigQuery tables |
| Legacy telemetry | `legacy_recipe.dhub.yaml` | daily 00:00 UTC | Legacy telemetry pings as datasets, with lineage to their BigQuery tables |
| Metric Hub glossary | `metrichub_glossary_source_recipe.dhub.yaml` | daily 00:00 UTC | Metric Hub metrics as glossary terms, and links from terms to the BigQuery tables they read from |
| bigquery-etl | `bigquery_etl_recipe.dhub.yaml` | daily 00:00 UTC | Links from BigQuery tables to their bigquery-etl source and Airflow DAG |
| Redash usage | `redash_usage_recipe.dhub.yaml` | daily 12:00 UTC | Usage, owners, and structured properties on Redash charts and dashboards |
| Deepnote | `deepnote_recipe.dhub.yaml` | daily 00:00 UTC | Prod Streamlit apps on Deepnote as dashboards, with lineage to their BigQuery tables |

Each source's settings in Acryl:

- Recipe: the recipe file without its `sink` (the executor adds one).
- Extra pip packages: `git+https://github.com/mozilla/mozilla-datahub-ingestion.git@main`. The
  executor installs this repo from git on every run, so changes take effect on the first run
  after they merge. The Redash usage source pins a commit SHA instead of `main`, so its changes
  only take effect when the SHA in the source is updated.

To schedule another source, add its recipe to `recipes` and create a source the same way.

The executor runs a newer DataHub CLI than `requirements.txt` pins (1.7 on Python 3.11 as of
October 2026), so test changes against that version as well.

Redash, Looker, BigQuery, and other sources are also UI-managed in Acryl, but they are
built-in DataHub sources and aren't part of this repo.

### Redash usage

The Redash usage source (`sync.datahub.redash_usage_source.RedashUsageSource`) writes usage,
owners, and structured properties to the Redash charts and dashboards ingested by the Redash
source, reading the `stmo.object_views_daily` and `stmo.object_owners` views from bigquery-etl.
Views, users, and top users over 90 days show as structured properties on charts and dashboards.
DataHub also shows its own summary (Users, Top Users, Recent Views, and a popularity tier over 30
days), which a daily DataHub job builds from the usage records, so it lags by about a day.
DataHub ranks top users and counts Recent Views by `usageCount`, which chart usage records can't
have, so for charts the summary's Top Users are in arbitrary order and Recent Views stays empty,
while its Users count and popularity tier are still useful. The summary's Total Views is left
empty on purpose, since it needs running-total snapshot records.

Its recipe in Acryl adds a `credential` block whose
values and `${SECRET}` references are copied from the BigQuery source's recipe. It runs at 12:00
UTC, after the `bqetl_stmo` DAG loads `stmo_external` at 08:00 UTC (best-effort). The order
relative to the Redash source doesn't matter, since the two write different aspects.

Before a change that adds or renames a structured property starts running, define it in DataHub
by running `redash_structured_properties_recipe.dhub.yaml`. Otherwise DataHub rejects the
source's writes to it.

### Deepnote

The Deepnote source (`sync.datahub.deepnote_source.DeepnoteSource`) writes prod Streamlit apps
served from Deepnote as dashboards on the `Deepnote` platform.

It only ingests Streamlit apps that:

- are in the default Mozilla workspace. The workgroup workspaces (Ads, Marketing, Revenue) hold
  restricted data, so the source refuses an API key for any other workspace.
- have a `metadata.yaml` next to their entrypoint. The `dashboard` skill in the mozdata Claude
  plugin writes one. For a hand-made app, write one by hand.
- have `tier: prod` in it. A dashboard moved back to draft is soft-deleted on the next run.

Notebooks, drafts, and apps without that file are not ingested.

From `metadata.yaml` it reads these fields, laid out as in the skill's template. Only `tier` is
required:

```yaml
tier: prod
title: "Desktop DAU by country"     # else the project name
owner: "someone@mozilla.com"        # added as an owner
grounding:                          # linked to the Metric Hub glossary term
  source: metric_hub
  platform: firefox_desktop
  metric_name: daily_active_users_v2
query:
  sql: |                            # parsed for links to BigQuery tables
    SELECT ... FROM mozdata.telemetry.active_users_aggregates
```

`sensitivity`, `review_status`, `generated_by`, `drift_check`, and `promotion.promoted_at` are
shown as properties when present. Apps marked `sensitivity: restricted` are skipped. Dashboards are
keyed by project and entrypoint, so stopping and re-serving an app keeps the same dashboard.
Its recipe reads the `DEEPNOTE_API_KEY` secret in Acryl, an API key for the
`datahub-deepnote@mozilla.com` service account in the Mozilla workspace.

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
