"""
A lightweight CLI that serves as the entrypoint for the datahub ingestion container.

Responsible for pushing metadata from our various services to a Datahub instance.
"""
import logging

import click

from sync.datahub.emitter import EMIT_FUNCTIONS, run_emitter


logging.basicConfig(
    level=logging.INFO,
    format="[%(asctime)s] %(levelname)-8s {%(name)s:%(lineno)d} - %(message)s",
)


@click.group()
def datahub():
    """Commands for Datahub"""


@datahub.command()
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


@click.group()
def dataplex():
    """Commands for Dataplex"""


@dataplex.command()
def glean_pings():
    ...


@dataplex.command()
def lookml():
    ...


@dataplex.command()
def looker():
    ...


@click.group(commands={"datahub": datahub, "dataplex": dataplex})
def cli():
    ...
