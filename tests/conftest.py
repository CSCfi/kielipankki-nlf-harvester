"""
Test fixtures
"""

import pytest
import requests_mock
import builtins
from unittest.mock import mock_open, patch


@pytest.fixture(autouse=True)
def prevent_online_http_requests(monkeypatch):
    """
    Patch urlopen so that all non-patched requests raise an error.
    """
    def urlopen_error(self, method, url, *args, **kwargs):
        raise RuntimeError(
            f"Requests are not allowed in tests, but a test attempted a "
            f"{method} request to {self.scheme}://{self.host}{url}"
        )

    monkeypatch.setattr(
        "urllib3.connectionpool.HTTPConnectionPool.urlopen", urlopen_error
    )


@pytest.fixture
def oai_pmh_api_url():
    """
    The URL of the OAI-PMH API used in tests.
    """
    return 'https://digi.kansalliskirjasto.fi/interfaces/OAI-PMH'


@pytest.fixture
def two_page_set_id():
    """
    Return a set id for a data set with enough records for two pages.
    """
    return 'col-681'


def _text_from_file(filename):
    """
    Read the contents of the given file into a string.
    """
    with open(filename, encoding='utf-8') as infile:
        return infile.read()


# pylint does not understand fixtures
# pylint: disable=redefined-outer-name
@pytest.fixture
def two_page_pmh_response(oai_pmh_api_url, two_page_set_id):
    """
    Patch a GET request for a data set with two pages worth of records.

    :return: Information about the data set
    :rtype: dict
    """
    first_page = _text_from_file(f'tests/data/{two_page_set_id}-part1.xml')
    last_page = _text_from_file(f'tests/data/{two_page_set_id}-part2.xml')

    with requests_mock.Mocker() as mocker:
        first_page_url = (f'{oai_pmh_api_url}'
                          f'?metadataPrefix=oai_dc'
                          f'&set={two_page_set_id}'
                          f'&verb=ListRecords')
        mocker.get(first_page_url, text=first_page)

        last_page_url = (f'{oai_pmh_api_url}'
                         f'?verb=ListRecords'
                         f'&resumptionToken=59zS9njRIN')
        mocker.get(last_page_url, text=last_page)

        yield {
                'length': 106,
                'first_id': 'https://digi.kansalliskirjasto.fi/sanomalehti/binding/379973',
                'last_id': 'https://digi.kansalliskirjasto.fi/sanomalehti/binding/380082',
                }


@pytest.fixture
def mets_binding_id():
    """
    Return a binding ID for testing fetching METS files.
    """
    return "https://digi.kansalliskirjasto.fi/sanomalehti/binding/379973"


@pytest.fixture
def expected_mets_response(mets_binding_id, tmp_path):
    """
    Patch a GET request for fetching a METS file for a given binding id.
    """
    id = mets_binding_id.split('/')[-1]
    mets_content = _text_from_file(f'tests/data/{id}_METS.xml')

    with requests_mock.Mocker() as mocker:
        mets_url = f'https://digi.kansalliskirjasto.fi/sanomalehti/binding/{id}/mets.xml?full=true'
        mocker.get(mets_url, text=mets_content)
        yield mets_content

    m = mock_open()
    with patch('builtins.open', m):
        with open(f'{tmp_path}/{id}_METS.xml', 'w') as file:
            file.write(mets_content)
    
    m.assert_called_once_with(f'{tmp_path}/{id}_METS.xml', 'w')
    handle = m()
    handle.write.assert_called_once_with(mets_content)
