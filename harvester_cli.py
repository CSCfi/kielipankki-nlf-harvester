"""
Command line interface for the harvester
"""

import click
from pathlib import Path
import zipfile

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


@cli.command
@click.argument("zip_file_path")
@click.argument(
    "binding_id_list_file",
)
@click.option(
    "--guess_prefix",
    default=True,
    help="Use name of zip to guess which binding ids it covers",
)
def check_zip_integrity(zip_file_path, binding_id_list_file, guess_prefix):
    """
    For each ID in binding_id_list_file, check that there is a METS associated
    with it, and that the files referenced by the METS are present. Check that
    nothing else is present.
    """

    prefix = ""
    if guess_prefix:
        try:
            prefix = zip_file_path[
                zip_file_path.rindex("_") + 1 : zip_file_path.index(".zip")
            ]
            _ = int(prefix)
        except Exception:
            prefix = ""
    binding_id_set = {
        line.strip() for line in open(binding_id_list_file) if line.startswith(prefix)
    }
    seen_id_set = set()
    errors_found = 0
    binding_info_string = ""
    if prefix != "":
        binding_info_string = f" for bindings with prefix {prefix}"
    click.echo(
        f"Performing integrity check for {zip_file_path.split('/')[-1]}{binding_info_string}, expecting {len(binding_id_set)} bindings"
    )
    with zipfile.ZipFile(zip_file_path, "r") as zip_file:
        zip_file_namelist = set(zip_file.namelist())
        for zipinfo_obj in zip_file.infolist():
            if zipinfo_obj.is_dir():
                continue
            file_path = zipinfo_obj.filename
            try:
                binding_id = file_path.split("/")[-3]
            except IndexError:
                click.echo(f"  malformed path: {file_path}")
                errors_found += 1
                continue
            if file_path.endswith("_METS.xml"):
                # Existing METS files are considered "seen bindings", and we check that referenced files exist
                seen_id_set.add(binding_id)
                basepath = "/".join(file_path.split("/")[:-2])
                with zip_file.open(file_path, "r") as mets_file:
                    mets = METS(
                        f"https://digi.kansalliskirjasto.fi/sanomalehti/binding/{binding_id}",
                        mets_file=mets_file,
                        encoding="utf-8",
                    )
                    for contentfile in mets.files():
                        if contentfile.filename.endswith(".xml"):
                            if (
                                f"{basepath}/alto/{contentfile.filename}"
                                not in zip_file_namelist
                            ):
                                errors_found += 1
                                click.echo(
                                    f"  {binding_id}: missing ALTO: {contentfile.filename}"
                                )
                        else:
                            if (
                                f"{basepath}/access_img/pr-{contentfile.filename}"
                                not in zip_file_namelist
                            ):
                                errors_found += 1
                                click.echo(
                                    f"  {binding_id}: missing img: {contentfile.filename}"
                                )
            else:
                # Other files are normally handled in the METS check, but
                # there might also be garbage
                if binding_id not in binding_id_set:
                    f"  {binding_id}: id not in list, but zip contains {file_path}"
                    errors_found += 1
    for missing_binding_id in binding_id_set.difference(seen_id_set):
        click.echo(f"  {missing_binding_id}: missing METS")
        errors_found += 1
    for superfluous_binding_id in seen_id_set.difference(binding_id_set):
        click.echo(superfluous_binding_id + ": extra METS (wasn't in binding list)")
        errors_found += 1
    click.echo(f"Found {errors_found} errors")


# mets/
# alto/
# access_img/pr-

if __name__ == "__main__":
    cli()  # pylint: disable=no-value-for-parameter
