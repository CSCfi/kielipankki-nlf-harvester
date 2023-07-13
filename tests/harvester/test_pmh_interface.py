"""
Tests for PMH_API
"""

import builtins
from paramiko.sftp_client import SFTPClient

from harvester.pmh_interface import PMH_API
from harvester import utils


def _check_result(ids, expected_info):
    """
    Check that given list of ids is what we expect
    :param ids: The list of IDs to be checked
    :type ids: list
    :param expected_info: Description of the expected features of the result
                          list. Must contain keys ``length`` (expected length
                          of the result), ``first_id`` (the first item in the
                          list) and ``last_id`` (the last item in the list).
    :type expected_info: dict
    """
    assert len(ids) == expected_info["length"]
    assert ids[0] == expected_info["first_id"]
    assert ids[-1] == expected_info["last_id"]


def test_binding_ids_from_two_page_response(oai_pmh_api_url, two_page_pmh_response):
    """
    Ensure that binding IDs are extracted correctly from a two-page response.
    """
    api = PMH_API(oai_pmh_api_url)
    ids = list(api.dc_identifiers("col-681"))
    _check_result(ids, two_page_pmh_response)


def test_fetch_mets(
    oai_pmh_api_url, mets_dc_identifier, expected_mets_response, tmp_path
):
    """
    Ensure that a valid METS file is fetched and written to disk.
    """
    api = PMH_API(oai_pmh_api_url)
    output_file = tmp_path / "test_mets.xml"
    with open(output_file, "wb") as mets_file:
        response = api.download_mets(mets_dc_identifier, mets_file)

    assert response.decode("utf-8") == expected_mets_response


def test_set_ids(oai_pmh_api_url, expected_set_list):
    """
    Ensure that all collection IDs are returned correctly.
    """
    api = PMH_API(oai_pmh_api_url)
    set_ids = list(api.set_ids())
    assert len(set_ids) == 83
    assert set_ids[0] == "sanomalehti"
    assert set_ids[-1] == "col-101:col-161"
