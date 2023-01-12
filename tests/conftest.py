"""
Test fixtures
"""

import os
import pytest
import requests
import requests_mock

from harvester.file import ALTOFile


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
    return "https://digi.kansalliskirjasto.fi/interfaces/OAI-PMH"


@pytest.fixture
def two_page_set_id():
    """
    Return a set id for a data set with enough records for two pages.
    """
    return "col-681"


def _text_from_file(filename):
    """
    Read the contents of the given file into a string.
    """
    with open(filename, encoding="utf-8") as infile:
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
    first_page = _text_from_file(f"tests/data/{two_page_set_id}-part1.xml")
    last_page = _text_from_file(f"tests/data/{two_page_set_id}-part2.xml")

    with requests_mock.Mocker() as mocker:
        first_page_url = (
            f"{oai_pmh_api_url}"
            f"?metadataPrefix=oai_dc"
            f"&set={two_page_set_id}"
            f"&verb=ListRecords"
        )
        mocker.get(first_page_url, text=first_page)

        last_page_url = (
            f"{oai_pmh_api_url}" f"?verb=ListRecords" f"&resumptionToken=59zS9njRIN"
        )
        mocker.get(last_page_url, text=last_page)

        yield {
            "length": 106,
            "first_id": "https://digi.kansalliskirjasto.fi/sanomalehti/binding/379973",
            "last_id": "https://digi.kansalliskirjasto.fi/sanomalehti/binding/380082",
        }


@pytest.fixture
def mets_dc_identifier():
    """
    Return a binding ID for testing fetching METS files.
    """
    return "https://digi.kansalliskirjasto.fi/sanomalehti/binding/379973"


@pytest.fixture
def simple_mets_path():
    """
    Path to METS with one location for each file
    """
    return "tests/data/379973_METS.xml"


@pytest.fixture
def expected_mets_response(mets_dc_identifier):
    """
    Patch a GET request for fetching a METS file for a given binding id.

    :return: Content of a METS file
    :rtype: str
    """
    binding_id = mets_dc_identifier.split("/")[-1]
    mets_content = _text_from_file(f"tests/data/{binding_id}_METS.xml")

    with requests_mock.Mocker() as mocker:
        mets_url = (
            f"https://digi.kansalliskirjasto.fi/sanomalehti/"
            f"binding/{binding_id}/mets.xml?full=true"
        )
        mocker.get(mets_url, text=mets_content)
        yield mets_content


@pytest.fixture
def cwd_in_tmp(tmp_path):
    """
    Change current working directory into a temporary directory.
    """
    original_cwd = os.getcwd()
    os.chdir(tmp_path)
    yield tmp_path
    os.chdir(original_cwd)


@pytest.fixture
def expected_set_list():
    """
    Patch API call that gets list of all collections available from NLF.
    """
    with open("tests/data/set_list.xml", "rb") as file:
        set_list_content = file.read()

    with requests_mock.Mocker() as mocker:
        url = "https://digi.kansalliskirjasto.fi/interfaces/OAI-PMH?verb=ListSets"
        mocker.get(url, content=set_list_content)
        yield set_list_content


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
def alto_file_with_erroneous_name(alto_url):
    """
    Return an ALTOFile with an erroneus filename for testing.
    """
    return ALTOFile(
        "test_checksum",
        "test_algo",
        "file://./alto/alto.xml",
        alto_url,
    )


@pytest.fixture
def alto_file_with_parsable_name(alto_url):
    """
    Return an ALTOfile with a non-standard, but parsable and working filename.
    """
    return ALTOFile(
        "test_checksum",
        "test_algo",
        "file://./alto/img0001-alto.xml",
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


def url_matcher(request):
    url = request.url
    mock_urls = [
        "https://digi.kansalliskirjasto.fi/sanomalehti/binding/379973/page-00001.xml",
        "https://digi.kansalliskirjasto.fi/sanomalehti/binding/379973/page-00002.xml",
        "https://digi.kansalliskirjasto.fi/sanomalehti/binding/379973/page-00003.xml",
        "https://digi.kansalliskirjasto.fi/sanomalehti/binding/379973/page-00004.xml",
    ]
    return url in mock_urls


@pytest.fixture
def mock_alto_download_for_test_mets():
    alto_file_content = "<xml>test cöntent</xml>"
    with requests_mock.Mocker() as mocker:
        mocker.get(
            requests_mock.ANY,
            additional_matcher=url_matcher,
            content=alto_file_content.encode("utf-8"),
        )
        yield alto_file_content
