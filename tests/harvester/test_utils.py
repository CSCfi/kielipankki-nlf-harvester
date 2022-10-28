"""
Tests for utility functions.
"""

from paramiko.sftp_client import SFTPClient

from harvester import utils


def test_binding_id_from_dc(mets_dc_identifier):
    """
    Test that DC identifiers are parsed to binding IDs correctly.
    """
    assert utils.binding_id_from_dc(mets_dc_identifier) == "379973"


def test_make_intermediate_dirs(sftp_client, mocker):
    """
    Test that intermediate directories on a remote host are created as expected.
    TODO Testing that the function works as expected after OSError.
    """
    mocker.patch("paramiko.sftp_client.SFTPClient.chdir")
    mocker.patch("paramiko.sftp_client.SFTPClient.mkdir")
    sftp = sftp_client.open_sftp()

    utils.make_intermediate_dirs(sftp, "/")
    SFTPClient.chdir.assert_called_once()

    assert utils.make_intermediate_dirs(sftp, "") == None
