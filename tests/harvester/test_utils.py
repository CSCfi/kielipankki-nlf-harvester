"""
Tests for utility functions.
"""

from pathlib import Path
from datetime import date
import pytest
import os

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
    new_path = root_path / "does" / "not" / "exist"
    utils.make_intermediate_dirs(sftp, new_path)

    try:
        sftp.chdir(str(new_path))
    except IOError:
        assert False, "Path {new_path} was not created"


def test_calculate_batch_size():
    """
    Test that batch sizes are calculated correctly.
    """
    assert utils.calculate_batch_size(1) == 1
    assert utils.calculate_batch_size(100) == 10
    assert utils.calculate_batch_size(10000) == 30
    assert utils.calculate_batch_size(100000) == 100


def test_split_into_download_batches():
    """
    Test that a collection is split to batches correctly.
    """
    bindings = list(range(50))
    batches = utils.split_into_download_batches(bindings)
    assert batches == [
        ([0, 1, 2, 3, 4, 5, 6, 7, 8, 9], 0),
        ([10, 11, 12, 13, 14, 15, 16, 17, 18, 19], 1),
        ([20, 21, 22, 23, 24, 25, 26, 27, 28, 29], 2),
        ([30, 31, 32, 33, 34, 35, 36, 37, 38, 39], 3),
        ([40, 41, 42, 43, 44, 45, 46, 47, 48, 49], 4),
    ]


def test_binding_download_location():
    """
    Test that subdirectory structure is created correctly
    """
    binding_id = "123456"
    dir_structure = utils.binding_download_location(binding_id)
    assert dir_structure == "1/12/123/1234/12345/123456/123456"

    dir_structure_set_depth = utils.binding_download_location(binding_id, 3)
    assert dir_structure_set_depth == "1/12/123/123456"


@pytest.mark.parametrize(
    "prefixes",
    [
        [str(i) for i in range(1, 10)],
        [str(i) for i in list(range(1, 4)) + list(range(40, 50)) + list(range(5, 9))],
    ],
)
def test_assign_bindings_to_subsets(prefixes):
    """
    Test assigning bindings to subsets

    We verify that there is the same number of subsets as there are prefixes, and that
    each binding in a subset has binding id that starts with the corresponding prefix.
    This is done for a simple set of prefixes (1, 2, ..., 9) and a more complex one with
    varying prefix length (1, 2, 3, 40, 51, ..., 49, 5, ...9).
    """
    with open("tests/data/test_col_bindings", "r") as f:
        bindings = f.read().splitlines()

    subsets = utils.assign_bindings_to_subsets(bindings, prefixes)

    assert len(subsets.keys()) == len(prefixes)

    for prefix, bindings in subsets.items():
        for binding in bindings:
            assert utils.binding_id_from_dc(binding).startswith(prefix)


def test_subset_for_binding(mets_dc_identifier):
    """
    Test that the correct subset prefix is found for a binding and if
    no prefix is found, an error is raised.
    """
    subset_split = {"36": [], "37": []}
    prefix = utils.subset_for_binding(mets_dc_identifier, subset_split)
    assert prefix == "37"

    subset_split = {"": []}
    prefix = utils.subset_for_binding(mets_dc_identifier, subset_split)
    assert prefix == ""

    subset_split = {"12": [], "13": []}
    with pytest.raises(ValueError):
        prefix = utils.subset_for_binding(mets_dc_identifier, subset_split)


def test_assign_update_bindings_to_subsets():
    """
    Test that incoming bindings are assigned to the existing subsets correctly.
    """
    with open("tests/data/binding_list.txt", "r") as f:
        bindings = f.read().splitlines()

    subset_split = utils.assign_update_bindings_to_subsets(
        bindings, "tests/data/subset_split.json"
    )

    assert subset_split["20"] == [bindings[0]]
    assert subset_split["21"] == [bindings[1]]
    assert "22" not in subset_split
    assert subset_split["3"] == [bindings[2]]
    assert subset_split["9"] == [bindings[3]]

    small_subset_split = utils.assign_update_bindings_to_subsets(
        bindings, "tests/data/small_subset_split.json"
    )

    assert len(small_subset_split) == 1
    assert small_subset_split[""] == bindings


def test_read_bindings(tmpdir):
    """
    Test that a list of bindings is read from file correctly.
    """
    with open("tests/data/binding_list.txt", "r") as f1:
        bindings = f1.read().splitlines()
        os.mkdir(Path(tmpdir) / "col-000")
        with open(Path(tmpdir) / "col-000" / f"binding_ids_{date.today()}", "w") as f2:
            for binding in bindings:
                f2.write(binding + "\n")

    assert utils.read_bindings(tmpdir, "col-000") == bindings

    with pytest.raises(FileNotFoundError):
        utils.read_bindings(tmpdir, "col-001")


def test_save_subset_split(tmpdir):
    """
    Test that saving the subset split to file actually creates a file.
    """
    with open("tests/data/binding_list.txt", "r") as f:
        bindings = f.read().splitlines()

    subset_split = utils.assign_update_bindings_to_subsets(
        bindings, "tests/data/subset_split.json"
    )
    utils.save_subset_split(subset_split, tmpdir, "col-000")
    output_path = Path(tmpdir) / Path("col-000_subsets.json")
    assert output_path.is_file()
    assert utils.save_subset_split(subset_split, tmpdir, "col-000") == None
