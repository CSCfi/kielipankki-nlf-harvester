"""
Tests for utility functions.
"""

from pathlib import Path

from harvester import utils


def test_binding_id_from_dc(mets_dc_identifier):
    """
    Test that DC identifiers are parsed to binding IDs correctly.
    """
    assert utils.binding_id_from_dc(mets_dc_identifier) == "379973"


def test_make_intermediate_dirs(sftp_server, sftp_client):
    """
    Test that intermediate directories on a remote host are created as expected.
    """
    sftp = sftp_client.open_sftp()
    root_path = Path(sftp_server.root)
    utils.make_intermediate_dirs(sftp, f"{root_path}/does/not/exist")
    assert sftp.listdir(f"{root_path}/does/not") == ["exist"]
