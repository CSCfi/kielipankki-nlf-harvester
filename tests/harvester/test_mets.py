"""
Tests for the METSParser
"""

import pytest
from lxml import etree

from harvester.file import (
    File,
    ALTOFile,
    SkippedFile,
    AccessImageFile,
)
from harvester.mets import METS, METSLocationParseError


# Pylint does not understand fixtures
# pylint: disable=redefined-outer-name


@pytest.fixture
def simple_mets_object(simple_mets_path):
    """
    Return a METS object representing a simple, well-formed METS file.
    """
    return METS(
        "https://example.com/dc_identifier/1234", mets_file=open(simple_mets_path, "rb")
    )


@pytest.fixture
def mets_with_multiple_file_locations(simple_mets_path, tmp_path):
    """
    Return a path to METS file that has a file with two locations
    """
    # Due to security reasons related to executing C code, pylint does not have
    # an accurate view into the lxml library. This disables false alarms.
    # pylint: disable=c-extension-no-member

    with open(simple_mets_path, "r", encoding="utf-8") as mets_file:
        mets_tree = etree.parse(mets_file)
    files = mets_tree.xpath(
        "mets:fileSec/mets:fileGrp/mets:file",
        namespaces={
            "mets": "http://www.loc.gov/METS/",
        },
    )
    double_location_file = files[0]
    etree.SubElement(
        double_location_file,
        "FLocat",
        LOCTYPE="OTHER",
        href="content/not/important/here",
    )

    mets_output_path = tmp_path / "mets.xml"
    mets_tree.write(mets_output_path)
    return mets_output_path


def test_file_location_parsing(simple_mets_object):
    """
    Test file location parsing when there's one location for each file.
    """
    files = list(simple_mets_object.files())

    first_file = files[0]
    assert first_file.location_xlink == "file://./preservation_img/pr-00001.jp2"


def test_files_exception_on_two_locations_for_a_file(
    mets_with_multiple_file_locations,
):
    """
    Ensure that ambiguous location for a file is not ignored.

    This is important so that we don't try to use a wrong location later up in
    the pipeline (e.g. trying to use a URL to determine the location of a file
    in a zip package).
    """

    mets = METS(
        "dummy_dc_identifier", mets_file=open(mets_with_multiple_file_locations, "rb")
    )
    with pytest.raises(METSLocationParseError):
        for _ in mets.files():
            pass


def test_file_content_type_parsing(simple_mets_path, mets_dc_identifier):
    """
    Test content type parsing when there's one location for each file.
    """
    mets = METS(mets_dc_identifier, mets_file=open(simple_mets_path, "rb"))
    files = list(mets.files())

    first_file = files[0]
    assert isinstance(first_file, AccessImageFile)

    last_file = files[-1]
    assert isinstance(last_file, ALTOFile)


def test_alto_files(simple_mets_path, mets_dc_identifier):
    """
    Ensure that an accurate list of alto files is returned.
    """
    mets = METS(mets_dc_identifier, mets_file=open(simple_mets_path, "rb"))
    alto_files = list(mets.files_of_type(ALTOFile))
    assert len(alto_files) == 4
    assert all(isinstance(file, ALTOFile) for file in alto_files)


@pytest.fixture
def exotic_files_mets_object():
    """
    Return a METS object representing a "METS" file that has multiple file types.

    The said METS is not really a full, well-formed METS file, but a stripped down
    version instead: only the `File`s (and their `fileGrp`s and `fileSec`s) and their
    corresponding `amdSec`s are present. This is sufficient for testing file type
    parsing.

    The METS file contains the normal ALTO and access image files, but also "target" and
    "retained" images (one of each).
    """
    return METS(
        "https://example.com/dc_identifier/5678",
        mets_file=open("tests/data/exotic_files_METS.xml", "rb"),
    )


def test_access_image_files_found_when_unusual_images_present(exotic_files_mets_object):
    """
    Ensure that the normal image group members are found, even when there are other
    images too.
    """
    access_files = list(exotic_files_mets_object.files_of_type(AccessImageFile))
    assert len(access_files) == 4


def test_alto_files_found_when_unusual_images_present(exotic_files_mets_object):
    """
    Ensure that the normal image group members are found, even when there are other
    images too.
    """
    access_files = list(exotic_files_mets_object.files_of_type(ALTOFile))
    assert len(access_files) == 4


def test_access_image_page_numbers_work_when_unusual_images_present(
    exotic_files_mets_object,
):
    """
    Ensure that exotic images with low-end file names don't throw off page numbers.

    Even if there are target images or such whose file names suggest they come before
    the actual access images (or even in the middle of them), we should extract files
    with correct page numbers (e.g. 1-4, not 2, 3, 4 and 6 if files  pr-00001.jp2 and
    pr-00005.jp2 are not access images).
    """
    access_files = list(exotic_files_mets_object.files_of_type(AccessImageFile))
    for index, access_file in zip(range(1, 5), access_files):
        assert access_file.download_url.endswith(f"/{index}")


def test_alto_page_numbers_work_when_unusual_images_present(
    exotic_files_mets_object,
):
    """
    Ensure that we don't try to download ALTO counterparts of target images etc.
    """
    alto_files = list(exotic_files_mets_object.files_of_type(AccessImageFile))
    for index, alto_file in zip(range(1, 5), alto_files):
        assert alto_file.download_url.endswith(f"/{index}")
