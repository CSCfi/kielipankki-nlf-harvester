"""
Tests for File and its subclasses.
"""

from pathlib import Path
from paramiko import SFTPClient
import pytest
import requests_mock

from harvester.file import File, ALTOFile


# Pylint does not understand fixture use
# pylint: disable=redefined-outer-name


@pytest.fixture
def alto_url():
    """
    Return the DC identifier for an ALTO test file
    """
    return "https://example.com/1234"


@pytest.fixture()
def alto_filename():
    """
    Return the filename for an ALTO test file
    """
    return "00002.xml"


@pytest.fixture
def alto_file(alto_url, alto_filename):
    """
    Return an ALTOFile for testing.
    """
    return ALTOFile(
        "test_checksum",
        "test_algo",
        f"file://./alto/{alto_filename}",
        alto_url,
    )


@pytest.fixture
def mock_alto_download(alto_url, alto_filename):
    """
    Fake a response for GETting an ALTO file from "NLF".

    We don't really need the proper contents of an ALTO file, so the response contains
    just dummy data.
    """
    alto_file_content = "<xml>test cöntent</xml>"
    with requests_mock.Mocker() as mocker:
        mocker.get(
            f"{alto_url}/page-{alto_filename}",
            content=alto_file_content.encode("utf-8"),
        )
        yield alto_file_content


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


def test_download_to_default_path(
    alto_file, cwd_in_tmp, mock_alto_download, alto_filename
):
    """
    Test downloading an ALTO file to the default location.
    """
    alto_file.download(write_operation=open)
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
    alto_file.download(
        write_operation=open,
        base_path=tmpdir,
        file_dir="some/sub/path",
        filename="test_alto.xml",
    )
    expected_output_path = Path(tmpdir) / "some" / "sub" / "path" / "test_alto.xml"
    assert expected_output_path.is_file()

    with open(expected_output_path, "r", encoding="utf-8") as alto:
        assert alto.read() == mock_alto_download


def test_download_to_remote_to_default_path(
    alto_file, sftp_client, mock_alto_download, mocker
):
    """
    Ensure that a valid file is written on the remote host.
    """
    mocker.patch("paramiko.sftp_client.SFTPClient.chdir")
    mocker.patch("paramiko.sftp_client.SFTPClient.mkdir")
    mocker.patch("paramiko.sftp_client.SFTPClient.file")

    sftp = sftp_client.open_sftp()

    alto_file.download(SFTPClient.file, sftp)

    SFTPClient.file.write.called_once_with(mock_alto_download)
