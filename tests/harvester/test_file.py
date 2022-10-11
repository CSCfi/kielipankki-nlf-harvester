"""
Tests for File and its subclasses.
"""

from harvester.file import File, ALTOFile


def test_file_initialized_values():
    """
    Check that the values given when initializing are utilized correctly
    """
    test_file = File("test_checksum", "test_algo", "test_location")
    assert test_file.checksum == "test_checksum"
    assert test_file.algorithm == "test_algo"
    assert test_file.location_xlink == "test_location"


def test_alto_download_url():
    """
    Ensure that the download URL is formed using the filename and dc_identifier.
    """
    alto_file = ALTOFile("test_checksum", "test_algo", "file://./alto/00002.xml")
    download_url = alto_file.download_url(dc_identifier="https://example.com/1234")

    assert download_url == "https://example.com/1234/page-00002.xml"
