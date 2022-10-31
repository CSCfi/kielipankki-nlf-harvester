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


def test_download_mets_with_filename(
    oai_pmh_api_url, mets_dc_identifier, expected_mets_response, tmp_path, mocker
):
    """
    Ensure that a valid METS file is fetched and written to disk with file_name.
    """
    api = PMH_API(oai_pmh_api_url)
    binding_id = utils.binding_id_from_dc(mets_dc_identifier)
    mocker.patch("builtins.open")
    response = api.download_mets(
        api.download_mets_to_local,
        dc_identifier=mets_dc_identifier,
        folder_path=tmp_path,
        file_name=f"{binding_id}_METS.xml",
    )
    builtins.open.assert_called_once_with(tmp_path / f"{binding_id}_METS.xml", "w")
    assert response == expected_mets_response


def test_download_mets_without_filename(
    oai_pmh_api_url, mets_dc_identifier, expected_mets_response, tmp_path, mocker
):
    """
    Ensure that a valid METS file is fetched and written to disk without file_name.
    """
    api = PMH_API(oai_pmh_api_url)
    binding_id = utils.binding_id_from_dc(mets_dc_identifier)
    mocker.patch("builtins.open")
    response = api.download_mets(
        api.download_mets_to_local,
        dc_identifier=mets_dc_identifier,
        folder_path=tmp_path,
    )
    builtins.open.assert_called_once_with(tmp_path / f"{binding_id}_METS.xml", "w")
    assert response == expected_mets_response


def test_download_mets_with_default_path(
    oai_pmh_api_url, mets_dc_identifier, expected_mets_response, mocker, cwd_in_tmp
):
    """
    Ensure that a valid METS file is fetched and written to disk with default path.
    """
    api = PMH_API(oai_pmh_api_url)
    binding_id = utils.binding_id_from_dc(mets_dc_identifier)
    mocker.patch("builtins.open")
    response = api.download_mets(
        api.download_mets_to_local, dc_identifier=mets_dc_identifier
    )
    builtins.open.assert_called_once_with(
        cwd_in_tmp / "downloads/mets" / f"{binding_id}_METS.xml", "w"
    )
    assert response == expected_mets_response


def test_fetch_all_mets_for_set(oai_pmh_api_url, two_page_set_id, tmp_path, mocker):
    """
    Test fetching all METS files in a collection

    This is done by checking that fetch_mets is called the correct number of times
    during a fetch_all_mets_for_set call.
    """
    api = PMH_API(oai_pmh_api_url)
    mocker.patch("harvester.pmh_interface.PMH_API.download_mets_to_local")
    mocker.patch(
        "harvester.pmh_interface.PMH_API.dc_identifiers", return_value=range(106)
    )
    api.download_all_mets_for_set(
        api.download_mets_to_local, set_id=two_page_set_id, folder_path=tmp_path
    )

    # pylint does not know about the extra functions from mocker
    # pylint: disable=no-member
    assert api.download_mets_to_local.call_count == 106


def test_download_mets_to_remote_with_filename(
    oai_pmh_api_url,
    mets_dc_identifier,
    expected_mets_response,
    mocker,
    tmp_path,
    sftp_client,
):
    """
    Test that METS files are saved on a remote host with filename parameter provided.
    """
    api = PMH_API(oai_pmh_api_url)
    binding_id = utils.binding_id_from_dc(mets_dc_identifier)
    mocker.patch("paramiko.sftp_client.SFTPClient.file")
    mocker.patch("paramiko.sftp_client.SFTPClient.chdir")
    mocker.patch("paramiko.sftp_client.SFTPClient.mkdir")
    sftp = sftp_client.open_sftp()
    tmp_path_string = str(tmp_path)
    response = api.download_mets(
        api.download_mets_to_remote,
        dc_identifier=mets_dc_identifier,
        folder_path=tmp_path_string,
        sftp_client=sftp,
        file_name=f"{binding_id}_METS.xml",
    )
    SFTPClient.file.assert_called_once_with(
        f"{tmp_path_string}/{binding_id}_METS.xml", "w"
    )
    assert response == expected_mets_response


def test_download_mets_to_remote_without_filename(
    oai_pmh_api_url,
    mets_dc_identifier,
    expected_mets_response,
    mocker,
    tmp_path,
    sftp_client,
):
    """
    Test that METS files are saved on a remote host without providing filename parameter.
    """
    api = PMH_API(oai_pmh_api_url)
    binding_id = utils.binding_id_from_dc(mets_dc_identifier)
    mocker.patch("paramiko.sftp_client.SFTPClient.file")
    mocker.patch("paramiko.sftp_client.SFTPClient.chdir")
    mocker.patch("paramiko.sftp_client.SFTPClient.mkdir")
    sftp = sftp_client.open_sftp()
    tmp_path_string = str(tmp_path)
    response = api.download_mets_to_remote(mets_dc_identifier, tmp_path_string, sftp)
    SFTPClient.file.assert_called_once_with(
        f"{tmp_path_string}/{binding_id}_METS.xml", "w"
    )
    assert response == expected_mets_response
