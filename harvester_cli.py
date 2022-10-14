"""
Command line interface for the harvester
"""

import click

from harvester.pmh_interface import PMH_API
from harvester.mets import METS


@click.group()
def cli():
    """
    Harvester for OAI-PMH data.
    """


@cli.command
@click.argument("set_id")
@click.option(
    "--url",
    default="https://digi.kansalliskirjasto.fi/interfaces/OAI-PMH",
    help="URL of the OAI-PMH API to be used",
)
def binding_ids(set_id, url):
    """
    Fetch all binding IDs in the given set.
    """
    # pylint does not understand that variables are inherited from the command
    # group
    # pylint: disable=undefined-variable
    api = PMH_API(url)
    ids = api.dc_identifiers(set_id)
    for id_ in ids:
        click.echo(id_)


@cli.command
@click.argument("mets_file_path")
@click.option("--encoding", default="utf-8")
def checksums(mets_file_path, encoding):
    """
    Output checksums for all files listed in the METS document.

    \b
    METS_FILE_PATH:
        Path to the METS file to be read
    """
    mets = METS(mets_file_path, encoding)
    for file in mets.files():
        click.echo(file.checksum)


@cli.command
@click.argument("mets_file_path")
@click.argument(
    "collection_dc_identifier",
)
@click.option("--encoding", default="utf-8")
def download_urls(mets_file_path, collection_dc_identifier, encoding):
    """
    Print download URLs for all files in METS

    \b
    METS_FILE_PATH:
        Path to the METS file to be read

    \b
    COLLECTION_DC_IDENTIFIER:
        Dublin Core identifier for the collection to
        which the binding described by this METS belongs. E.g.
        https://digi.kansalliskirjasto.fi/sanomalehti/binding/380082.
    """
    mets = METS(mets_file_path, collection_dc_identifier, encoding)
    for file in mets.files():
        try:
            click.echo(file.download_url)
        except NotImplementedError:
            click.echo(f"No download URL available for file {file.location_xlink}")


if __name__ == "__main__":
    cli()  # pylint: disable=no-value-for-parameter
