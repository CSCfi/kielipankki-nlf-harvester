"""
Tests for File and its subclasses.
"""

from pathlib import Path
from paramiko import SFTPClient
import pytest
import requests_mock

from harvester.file import File, ALTOFile
from harvester import utils


# Pylint does not understand fixture use
# pylint: disable=redefined-outer-name


def test_file_initialized_values():
    """
    Check that the values given when initializing are utilized correctly
    """
    test_file = File(
        "test_checksum", "test_algo", "test_location", "test_dc_identifier"
    )
    assert test_file.checksum == "test_checksum"
    assert test_file.algorithm == "test_algo"
    assert test_file.location_xlink == "test_location"
    assert test_file.binding_dc_identifier == "test_dc_identifier"


def test_alto_download_url(alto_file, alto_url, alto_filename):
    """
    Ensure that the download URL is formed using the filename and dc_identifier.
    """
    assert alto_file.download_url == f"{alto_url}/page-{alto_filename}"


def test_filename(alto_file, alto_filename):
    """
    Ensure that determining the file name based on the xpath works.
    """
    assert alto_file.filename == alto_filename


def test_erroneus_filename(alto_file_with_erroneous_name):
    """
    Ensure that an erroneous ALTO filename raises an error.
    """
    with pytest.raises(AttributeError, match=r".* alto.xml .*"):
        alto_file_with_erroneous_name.download_url()


def test_parsable_filename(alto_file_with_parsable_name):
    """
    Ensure that an ALTO file with a non-standard name gets parsed correctly.
    """
    assert (
        alto_file_with_parsable_name.download_url.rsplit("/", maxsplit=1)[-1]
        == "page-1.xml"
    )


def test_download_to_default_path(
    alto_file, cwd_in_tmp, mock_alto_download, alto_filename
):
    """
    Test downloading an ALTO file to the default location.
    """
    output_path = utils.file_download_location(file=alto_file)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    with open(output_path, "wb") as output_file:
        alto_file.download(output_file)

    expected_output_path = (
        Path(cwd_in_tmp) / "downloads" / "1234" / "alto" / alto_filename
    )
    assert expected_output_path.is_file()

    with open(expected_output_path, "r", encoding="utf-8") as alto:
        assert alto.read() == mock_alto_download


def test_download_to_custom_path(alto_file, mock_alto_download, tmpdir):
    """
    Ensure that downloaded files are saved into the specified location.
    """
    output_path = Path(tmpdir) / "some" / "sub" / "path" / "test_alto.xml"
    output_path.parent.mkdir(parents=True, exist_ok=True)

    with open(output_path, "wb") as output_file:
        alto_file.download(output_file)

    assert output_path.is_file()

    with open(output_path, "r", encoding="utf-8") as alto:
        assert alto.read() == mock_alto_download


def test_download_to_remote(alto_file, sftp_client, sftp_server, mock_alto_download):
    """
    Ensure that a valid file is written on the remote host.
    """

    sftp = sftp_client.open_sftp()
    output_path = Path(sftp_server.root) / "some" / "sub" / "path" / "test_alto.xml"

    utils.make_intermediate_dirs(
        sftp_client=sftp,
        remote_directory=output_path.parent,
    )

    with sftp.file(str(output_path), "wb") as file:
        alto_file.download(
            output_file=file,
            chunk_size=1024 * 1024,
        )

    with sftp.file(str(output_path), "r") as file:
        assert file.read().decode("utf-8") == mock_alto_download


def test_access_image_download_url(access_image, access_image_base_url):
    """
    Test that the most basic case of access image url parsing works
    """
    image = access_image(filename="pr-00001.tif")
    assert image.download_url == access_image_base_url + "/1"


def test_access_image_download_url_with_large_page_number(
    access_image, access_image_base_url
):
    """
    Test that URLs are parsed correctly when binding has a lot of pages
    """
    image = access_image(filename="pr-123456789.tif")
    assert image.download_url == access_image_base_url + "/123456789"


def test_access_image_download_url_with_zeros_in_page_number(
    access_image, access_image_base_url
):
    """
    Test that zeros in page number behave as expected:
      * leading zeros are ignored
      * other zeros (trailing and in the middle of the number) are included
    """
    image = access_image(filename="pr-012304560.tif")
    assert image.download_url == access_image_base_url + "/12304560"
