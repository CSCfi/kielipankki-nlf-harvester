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


def test_calculate_batch_size():
    """
    Test that batch sizes are calculated correctly.
    """
    assert utils.calculate_batch_size(1) == 1
    assert utils.calculate_batch_size(100) == 10
    assert utils.calculate_batch_size(10000) == 100
    assert utils.calculate_batch_size(100000) == 250
    assert utils.calculate_batch_size(1000000) == 500


def test_split_into_download_batches():
    """
    Test that a collection is split to batches correctly.
    """
    bindings = list(range(50))
    batches = utils.split_into_download_batches(bindings)
    assert batches == [
        [0, 1, 2, 3, 4, 5, 6, 7, 8, 9],
        [10, 11, 12, 13, 14, 15, 16, 17, 18, 19],
        [20, 21, 22, 23, 24, 25, 26, 27, 28, 29],
        [30, 31, 32, 33, 34, 35, 36, 37, 38, 39],
        [40, 41, 42, 43, 44, 45, 46, 47, 48, 49],
    ]


def test_binding_download_location():
    """
    Test that subdirectory structure is created correctly
    """
    binding_id = "123456"
    dir_structure = utils.binding_download_location(binding_id, "col-123")
    assert dir_structure == "col-123/1/12/123/1234/12345/123456/123456"

    dir_structure_set_depth = utils.binding_download_location(
        binding_id, "col-123", 3
    )
    assert dir_structure_set_depth == "col-123/1/12/123/123456"
