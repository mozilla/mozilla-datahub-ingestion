import logging
import subprocess
import tempfile
from datetime import datetime

from pathlib import Path
import yaml
from jinja2 import Environment, FileSystemLoader

LOOKML_REPOSITORY_CONFIG = "lookml_repositories.yaml"
LOOKML_TEMPLATE = "lookml_recipe_template.dhub.yaml"
RECIPE_TEMPLATE_DIR = "recipe_templates"

log = logging.getLogger(__name__)


def _generate_lookml_recipes(recipes_path: Path) -> None:
    """
    Generate LookML recipes from configuration and repositories.

    LookML recipes have to be generated because they each sync LookML from one "project" (corresponds to one GitHub
    repository) and some are private.

    To generate LookML recipes we:
     1. Read and validate the repository configuration.
     2. Clone each LookML repository.
     3. Fill in the LookML recipe template.
    """

    with open(LOOKML_REPOSITORY_CONFIG) as f:
        lookml_repository_config = yaml.safe_load(f)

    if not lookml_repository_config or "repositories" not in lookml_repository_config:
        log.warning(
            f"No properly formatted lookml repository config file {LOOKML_REPOSITORY_CONFIG} found..."
        )
        return

    env = Environment(loader=FileSystemLoader(RECIPE_TEMPLATE_DIR))
    lookml_dhub_template = env.get_template(LOOKML_TEMPLATE)

    for lookml_project, lookml_repository_uri in lookml_repository_config[
        "repositories"
    ].items():
        # 1. Clone each LookML repository into `lookml_repositories/`
        subprocess.run(
            [
                "git",
                "clone",
                lookml_repository_uri,
                f"lookml_repositories/{lookml_project}",
            ]
        )

        # 2. Dynamically build the recipe (some are private)
        path_to_lookml_project = Path(f"./lookml_repositories/{lookml_project}")
        with open(recipes_path / f"{lookml_project}.dhub.yaml", "w") as f:
            f.write(
                lookml_dhub_template.render(
                    lookml_project_name=lookml_project,
                    path_to_lookml_folder=path_to_lookml_project.absolute(),
                )
            )


def _replace_sink_with_static_file(recipe_filename: str) -> dict:
    """
    For debugging purposes
    """

    static_filename = f"{recipe_filename}-{datetime.now()}.test.json"

    return {"type": "file", "config": {"filename": static_filename}}


def run_recipes_in_dir(dirname: str, dump_to_file: bool) -> None:
    """
    This function is a thin wrapper around the DataHub CLI ingest method with some additional functionality
    for debugging and generating recipes.

    We forward to the DataHub CLI instead of using DataHub as a library (via datahub.ingestion.run.pipeline) because:
       1. It has additional logging/reporting.
       2. It expands environment variables in recipe files.

    Forwarding/invoking to the datahub.cli Click functions was also considered, but that has custom configuration
    that I didn't figure out how to forward.
    """

    recipes_path = Path(dirname)

    if recipes_path.name == "lookml":
        _generate_lookml_recipes(recipes_path)

    recipe_files = list(recipes_path.glob("*.dhub.yaml"))
    if not recipe_files:
        log.warning(f"No files with format '.dhub.yaml' found in {dirname}")
        return

    log.info(f"Running recipes in {recipes_path}")
    for recipe_file in recipe_files:
        log.info(f"Running {recipe_file}.")
        with open(recipe_file) as f:
            config = yaml.safe_load(f)

        if dump_to_file:
            config["sink"] = _replace_sink_with_static_file(recipe_file.name)

            with tempfile.NamedTemporaryFile("w", suffix=".dhub.yaml") as new_recipe:
                yaml.dump(config, new_recipe)
                subprocess.call(["datahub", "ingest", "-c", new_recipe.name])

        else:
            subprocess.call(["datahub", "ingest", "-c", str(recipe_file)])
