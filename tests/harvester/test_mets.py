"""
Tests for the METSParser
"""

import pytest
from lxml import etree

from harvester.mets import METS, METSLocationParseError


@pytest.fixture
def simple_mets():
    """
    METS with one location for each file
    """
    return 'tests/data/379973_METS.xml'


# Pylint does not understand fixtures
# pylint: disable=redefined-outer-name


@pytest.fixture
def mets_with_multiple_file_locations(simple_mets, tmp_path):
    """
    Return a path to METS file that has a file with two locations
    """
    # Due to security reasons related to executing C code, pylint does not have
    # an accurate view into the lxml library. This disables false alarms.
    # pylint: disable=c-extension-no-member

    with open(simple_mets, 'r', encoding='utf-8') as mets_file:
        mets_tree = etree.parse(mets_file)
    files = mets_tree.xpath(
            'mets:fileSec/mets:fileGrp/mets:file',
            namespaces={
                'mets': 'http://www.loc.gov/METS/',
                }
            )
    double_location_file = files[0]
    etree.SubElement(
            double_location_file,
            'FLocat',
            LOCTYPE='OTHER',
            href='content/not/important/here',
            )

    mets_output_path = tmp_path / "mets.xml"
    mets_tree.write(mets_output_path)
    return mets_output_path


def test_checksums(simple_mets):
    """
    Test checksum parsing when there's one location for each file.
    """
    mets = METS(simple_mets)
    checksums = list(mets.checksums())
    assert checksums[0] == {
            'checksum': 'ab64aff5f8375ca213eeaee260edcefe',
            'algorithm': 'MD5',
            'location': 'file://./preservation_img/pr-00001.jp2'
            }
    assert checksums[-1] == {
            'checksum': 'a462f99b087161579104902c19d7746d',
            'algorithm': 'MD5',
            'location': 'file://./alto/00004.xml'
            }


def test_checksums_exception_on_two_locations_for_a_file(
        mets_with_multiple_file_locations):
    """
    Ensure that ambiguous location for a file is not ignored.

    This is important so that we don't try to use a wrong location later up in
    the pipeline (e.g. trying to use a URL to determine the location of a file
    in a zip package).
    """
    mets = METS(mets_with_multiple_file_locations)
    with pytest.raises(METSLocationParseError):
        for _ in mets.checksums():
            pass
