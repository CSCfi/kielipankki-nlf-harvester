"""
Tests for the METSParser
"""

import pytest

from harvester.mets_parser import METSParser


@pytest.fixture
def simple_mets():
    """
    Mets with one location for each file
    """
    return 'tests/data/379973_METS.xml'


# Pylint does not understand fixtures
# pylint: disable=redefined-outer-name

def test_checksums(simple_mets):
    """
    Test checksum parsing when there's one location for each file.
    """
    parser = METSParser(simple_mets)
    checksums = list(parser.checksums())
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
