"""
Tests for ensuring that the CLI stays functional in refactorings.

This means that the bare minimum is likely often enough: as long as the method and
parameter names are ok, things are likely fine.
"""

import re
import sys

import pytest
import requests_mock

from click.testing import CliRunner
from harvester_cli import cli


requires_37 = pytest.mark.skipif(
    sys.version_info < (3, 7),
    reason="CliRunner is buggy on click8.0 and newer ones require python3.7 or higher",
)


@requires_37
def test_binding_ids_with_url(oai_pmh_api_url, two_page_pmh_response, two_page_set_id):
    """
    Test that the CLI can fetch binding IDs from a specific URL
    """
    runner = CliRunner()

    # fmt: off
    result = runner.invoke(
        cli,
        [
            "binding-ids",
            "--url", oai_pmh_api_url,
            two_page_set_id,
        ],
        catch_exceptions=False,
    )
    # fmt: on

    # One extra line for line feed at the end
    assert len(result.output.split("\n")) == two_page_pmh_response["length"] + 1

    assert two_page_pmh_response["last_id"] in result.output
    assert two_page_pmh_response["first_id"] in result.output


@requires_37
def test_binding_ids_from_default_url(two_page_pmh_response, two_page_set_id):
    """
    Test that the CLI can fetch binding IDs from default API URL when not given
    """
    runner = CliRunner()

    result = runner.invoke(
        cli,
        [
            "binding-ids",
            two_page_set_id,
        ],
        catch_exceptions=False,
    )

    # One extra line for line feed at the end
    assert len(result.output.split("\n")) == two_page_pmh_response["length"] + 1

    assert two_page_pmh_response["last_id"] in result.output
    assert two_page_pmh_response["first_id"] in result.output


@requires_37
def test_checksums(simple_mets_path):
    """
    Check that at least one correct checksum is printed
    """
    runner = CliRunner()

    result = runner.invoke(
        cli,
        [
            "checksums",
            simple_mets_path,
        ],
        catch_exceptions=False,
    )
    assert "\n33cbc005ce7dac534bdcc424c8a082cd\n" in result.output


@requires_37
def test_list_download_urls(simple_mets_path):
    """
    Check that the CLI is able to call the `download_urls` function.

    At the time of writing this test, we do not have the functionality for identifying
    different types of files, so the output does not contain relevant information to
    check. It is advised to edit this test as support is added for new file types.
    """
    runner = CliRunner()

    result = runner.invoke(
        cli,
        ["list-download-urls", simple_mets_path, "https://example.com/1234"],
        catch_exceptions=False,
    )

    # four pages, alto and access image for each, plus a trailing newline
    assert len(result.output.split("\n")) == 8 + 1


@pytest.fixture
def dummy_response_to_all_downloads():
    """
    Return a test response to all file download requests to NLF.
    """
    download_urls = re.compile("https://example.com/1234/.*")
    with requests_mock.Mocker() as mocker:
        mocker.get(download_urls, content=b"test content")
        yield


@requires_37
@pytest.mark.usefixtures("dummy_response_to_all_downloads")
def test_download_files_from(simple_mets_path, tmpdir):
    """
    Check that `download()` is called for each file listed in METS.

    It is enough that no exceptions rise.
    """
    runner = CliRunner()

    runner.invoke(
        cli,
        [
            "download-files-from",
            simple_mets_path,
            "https://example.com/1234",
            "--base-path",
            tmpdir,
        ],
        catch_exceptions=False,
    )
