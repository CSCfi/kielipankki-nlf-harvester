"""
Tests for PMH_API
"""

import pytest

from harvester.nlf_interface import PMH_API


@pytest.mark.usefixtures('two_page_pmh_response')
def test_binding_ids():
    api = PMH_API('https://digi.kansalliskirjasto.fi/interfaces/OAI-PMH')
    ids = list(api.binding_ids('col-681'))
    assert len(ids) == 106
    assert ids[0] == '379973'
    assert ids[-1] == '380082'
