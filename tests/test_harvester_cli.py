"""
Tests for ensuring that the CLI stays functional in refactorings.

This means that the bare minimum is likely often enough: as long as the method and
parameter names are ok, things are likely fine.
"""

from click.testing import CliRunner
from harvester_cli import cli


def test_binding_ids_with_url(oai_pmh_api_url, two_page_pmh_response, two_page_set_id):
    """
    Test that the CLI can fetch binding IDs from a specific URL
    """
    runner = CliRunner()

    # fmt: off
    result = runner.invoke(
        cli,
        [
            "--url", oai_pmh_api_url,
            "binding-ids",
            two_page_set_id,
        ],
    )
    # fmt: on

    # One extra line for line feed at the end
    assert len(result.output.split("\n")) == two_page_pmh_response["length"] + 1

    assert two_page_pmh_response["last_id"] in result.output
    assert two_page_pmh_response["first_id"] in result.output


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
    )

    # One extra line for line feed at the end
    assert len(result.output.split("\n")) == two_page_pmh_response["length"] + 1

    assert two_page_pmh_response["last_id"] in result.output
    assert two_page_pmh_response["first_id"] in result.output


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
    )
    assert "\n33cbc005ce7dac534bdcc424c8a082cd\n" in result.output
