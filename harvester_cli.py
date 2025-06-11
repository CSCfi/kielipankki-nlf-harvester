"""
Command line interface for the harvester
"""

import click
from pathlib import Path

from harvester.pmh_interface import PMH_API
from harvester.mets import METS
from harvester import utils


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
@click.argument("set_id")
@click.option(
    "--from-date",
    default=None,
    help="Earliest time of deletion to consider, e.g. 2026-01-28",
)
@click.option(
    "--url",
    default="https://digi.kansalliskirjasto.fi/interfaces/OAI-PMH",
    help="URL of the OAI-PMH API to be used",
)
def deleted_binding_ids(set_id, from_date, url):
    """
    Fetch deleted binding IDs in the given set since a given date.
    """
    api = PMH_API(url)
    ids = api.deleted_dc_identifiers(set_id, from_date=from_date)
    for id_ in ids:
        click.echo(id_)


@cli.command
@click.argument("mets_file_path")
@click.argument(
    "collection_dc_identifier",
)
@click.option("--encoding", default="utf-8")
def list_download_urls(mets_file_path, collection_dc_identifier, encoding):
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
    mets = METS(
        collection_dc_identifier,
        mets_file=open(mets_file_path, "rb"),
        encoding=encoding,
    )
    for file in mets.files():
        try:
            click.echo(file.download_url)
        except NotImplementedError:
            click.echo(f"No download URL available for file {file.location_xlink}")


@cli.command
@click.argument("mets_file_path")
@click.argument(
    "collection_dc_identifier",
)
@click.option("--encoding", default="utf-8")
@click.option(
    "--base-path",
    default=None,
    help="The location under which the structure of downloaded files is created",
)
def download_files_from(mets_file_path, collection_dc_identifier, encoding, base_path):
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
    mets = METS(
        collection_dc_identifier,
        mets_file=open(mets_file_path, "rb"),
        encoding=encoding,
    )
    for file in mets.files():
        try:
            output_file_path = utils.file_download_location(
                file=file, base_path=base_path
            )
            output_file_path.parent.mkdir(parents=True, exist_ok=True)
            with open(output_file_path, "wb") as output_file:
                file.download(output_file=output_file)
        except NotImplementedError:
            click.echo(f"No download URL available for file {file.location_xlink}")


if __name__ == "__main__":
    cli()  # pylint: disable=no-value-for-parameter
