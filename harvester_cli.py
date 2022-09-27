"""
Command line interface for the harvester
"""

import click

from harvester.pmh_interface import PMH_API
from harvester.mets import METS


@click.group()
@click.pass_context
@click.option(
    "--url",
    default="https://digi.kansalliskirjasto.fi/interfaces/OAI-PMH",
    help="URL of the OAI-PMH API to be used",
)
def cli(ctx, url):  # pylint: disable=unused-argument
    """
    Harvester for OAI-PMH data.
    """
    ctx.ensure_object(dict)

    api = PMH_API(url)
    ctx.obj["API"] = api


@cli.command
@click.pass_context
@click.argument("set_id")
def binding_ids(ctx, set_id):
    """
    Fetch all binding IDs in the given set.
    """
    # pylint does not understand that variables are inherited from the command
    # group
    # pylint: disable=undefined-variable
    ids = ctx.obj["API"].binding_ids(set_id)
    for id_ in ids:
        click.echo(id_)


@cli.command
@click.argument("mets_file_path")
@click.option("--encoding", default="utf-8")
def checksums(mets_file_path, encoding):
    """
    Output checksums for all files listed in the METS document.
    """
    mets = METS(mets_file_path, encoding)
    for checksum in mets.checksums():
        click.echo(checksum)


if __name__ == "__main__":
    cli()  # pylint: disable=no-value-for-parameter
