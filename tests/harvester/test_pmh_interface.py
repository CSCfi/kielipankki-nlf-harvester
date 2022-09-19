"""
Tests for PMH_API
"""

from harvester.pmh_interface import PMH_API
import pytest



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
    ids = list(api.binding_ids('col-681'))
    _check_result(ids, two_page_pmh_response)


def test_fetch_mets_with_filename(oai_pmh_api_url,
                                  mets_binding_id,
                                  expected_mets_response,
                                  tmp_path):
    """
    Ensure that a valid METS file is fetched with file_name parameter.
    """
    id = mets_binding_id.split('/')[-1]
    api = PMH_API(oai_pmh_api_url)
    response = api.fetch_mets(mets_binding_id, tmp_path, f"{id}_METS.xml")
    assert response == expected_mets_response


def test_fetch_mets_without_filename(oai_pmh_api_url,
                                     mets_binding_id,
                                     expected_mets_response,
                                     tmp_path):
    """
    Ensure that a valid METS file is fetched without file_name parameter.
    """
    api = PMH_API(oai_pmh_api_url)
    response = api.fetch_mets(mets_binding_id, tmp_path)
    assert response == expected_mets_response
