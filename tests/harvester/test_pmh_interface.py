"""
Tests for PMH_API
"""

from harvester.pmh_interface import PMH_API
import pytest
import builtins


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
    assert len(ids) == expected_info['length']
    assert ids[0] == expected_info['first_id']
    assert ids[-1] == expected_info['last_id']


def test_binding_ids_from_two_page_response(oai_pmh_api_url,
                                            two_page_pmh_response):
    """
    Ensure that binding IDs are extracted correctly from a two-page response.
    """
    api = PMH_API(oai_pmh_api_url)
    ids = list(api.dc_identifiers('col-681'))
    _check_result(ids, two_page_pmh_response)


def test_binding_id_from_dc(oai_pmh_api_url,
                            mets_dc_identifier):
    """
    Test that DC identifiers are parsed to binding IDs correctly.
    """
    api = PMH_API(oai_pmh_api_url)
    assert api.binding_id_from_dc(mets_dc_identifier) == "379973"


def test_fetch_mets_with_filename(oai_pmh_api_url,
                                  mets_dc_identifier,
                                  expected_mets_response,
                                  tmp_path,
                                  mocker):
    """
    Ensure that a valid METS file is fetched and written to disk with file_name parameter.
    """
    api = PMH_API(oai_pmh_api_url)
    binding_id = api.binding_id_from_dc(mets_dc_identifier)
    mocker.patch('builtins.open')
    response = api.fetch_mets(mets_dc_identifier, tmp_path, f"{binding_id}_METS.xml")
    builtins.open.assert_called_once_with(str(tmp_path/f"{binding_id}_METS.xml"), 'w')
    assert response == expected_mets_response


def test_fetch_mets_without_filename(oai_pmh_api_url,
                                     mets_dc_identifier,
                                     expected_mets_response,
                                     tmp_path,
                                     mocker):
    """
    Ensure that a valid METS file is fetched and written to disk without file_name parameter.
    """
    api = PMH_API(oai_pmh_api_url)
    binding_id = api.binding_id_from_dc(mets_dc_identifier)
    mocker.patch('builtins.open')
    response = api.fetch_mets(mets_dc_identifier, tmp_path)
    builtins.open.assert_called_once_with(str(tmp_path/f"{binding_id}_METS.xml"), 'w')
    assert response == expected_mets_response


def test_fetch_all_mets_for_set(oai_pmh_api_url,
                                two_page_set_id,
                                tmp_path,
                                mocker):
    """
    Ensure that fetch_mets is called the correct number of times during a fetch_all_mets_for_set call.
    """
    api = PMH_API(oai_pmh_api_url)
    mocker.patch('harvester.pmh_interface.PMH_API.fetch_mets')
    mocker.patch('harvester.pmh_interface.PMH_API.dc_identifiers', return_value=range(106))
    api.fetch_all_mets_for_set(two_page_set_id, tmp_path)
    assert api.fetch_mets.call_count == 106
