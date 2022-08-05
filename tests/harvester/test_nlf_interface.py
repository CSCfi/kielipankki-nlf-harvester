"""
Tests for PMH_API
"""

import pytest

from harvester.nlf_interface import PMH_API


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
