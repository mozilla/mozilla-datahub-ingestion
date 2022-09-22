"""
A lightweight CLI that serves as the entrypoint for the datahub ingestion container.

Responsible for pushing metadata from our various services to a Datahub instance.
"""
import logging

import click

from sync.emitter import EMIT_FUNCTIONS, run_emitter
from sync.recipe_runner import run_recipes_in_dir


logging.basicConfig(
    level=logging.INFO,
    format="[%(asctime)s] %(levelname)-8s {%(name)s:%(lineno)d} - %(message)s",
)


@click.group()
def cli():
    pass


@cli.command()
@click.argument("directory", type=click.Path(exists=True))
@click.option(
    "--dump_to_file",
    is_flag=True,
    default=False,
    help="Replaces all recipe sinks with a JSON file sink (useful for debugging).",
)
def recipes(directory: str, dump_to_file: bool):
    """
    Run each recipe (.dhub.yaml file) in a directory.

    DIRECTORY: Path to the directory of recipes to run.
    """
    run_recipes_in_dir(directory, dump_to_file)


@cli.command()
@click.argument("name", type=click.Choice(EMIT_FUNCTIONS.keys()))
@click.option(
    "--dump_to_file",
    is_flag=True,
    default=False,
    help="Replaces emitter with a JSON file dump (useful for debugging).",
)
def emitter(name: str, dump_to_file: bool):
    """
    Runs a custom metadata emitter.

    NAME: The name of the chosen metadata emitter.
    """
    run_emitter(name, dump_to_file)
