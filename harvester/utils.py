"""
General utility functions for working with the OAI-PMH API, metadata and files.
"""

import os
from pathlib import Path


def binding_id_from_dc(dc_identifier):
    """
    Parse binding ID from dc_identifier URL.

    :param dc_identifier: DC identifier of a record
    :type dc_identifier: str
    """
    return dc_identifier.split("/")[-1]


def make_intermediate_dirs(sftp_client, remote_directory):
    """
    Create all the intermediate directories in a remote host and ensure that
    the download path is created correctly.

    SFTPClient.mkdir raises an IOError if an already existing folder is attempted
    to be created. The error is caught and does not require further action.

    :param sftp_client: A Paramiko SFTP client.
    :param remote_directory: Absolute Path of the directory containing the file
    """
    for i in range(1, len(remote_directory.split("/")) + 1):
        remote_dir = "/".join(remote_directory.split("/")[:i])
        try:
            sftp_client.mkdir(remote_dir)
        except IOError:
            continue


def file_download_location(file, base_path=None, file_dir=None, filename=None):
    """
    The output location can be specified with the components ``base_path``,
    ``file_dir`` and ``filename``. If not given, the output location is as
    follows:

     ./downloads/[binding ID]/[type directory]/[filename from location_xlink]
     ^^^^^^^^^^^ ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^ ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
      base_path          file_dir                         filename

    Return :class:`pathlib.Path`
    """
    if not base_path:
        base_path = file._default_base_path()
    if not file_dir:
        file_dir = file._default_file_dir()
    if not filename:
        filename = file._default_filename()

    return Path(base_path) / Path(file_dir) / Path(filename)


def mets_download_location(dc_identifier, base_path=None, file_dir=None, filename=None):
    """
    The output location can be specified with the components ``base_path``,
    ``file_dir`` and ``filename``. If not given, the output location is as
    follows:

     ./downloads/[binding ID]/[type directory]/[binding ID]_METS.xml
     ^^^^^^^^^^^ ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^ ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
      base_path          file_dir                         filename

    Return :class:`pathlib.Path`
    """
    if not base_path:
        base_path = Path(os.getcwd()) / "downloads"
    if not file_dir:
        file_dir = f"{binding_id_from_dc(dc_identifier)}/mets"
    if not filename:
        filename = f"{binding_id_from_dc(dc_identifier)}_METS.xml"

    return Path(base_path) / Path(file_dir) / Path(filename)


def binding_download_location(binding_id, set_id, depth=None):
    """
    Construct and return a subdirectory structure of given depth
    for a binding. Default depth is the length of the binding ID.

    :param binding_id: Binding id
    :type binding_id: str
    :param set_id: Collection ID to which the binding belongs to
    :type set_id: str
    :param depth: Depth of the subdirectory structure (defaults to length of binding ID)
    :type depth: int
    """
    if not depth:
        depth = len(binding_id)
    sub_dirs = [f"{binding_id[:i]}" for i in range(1, depth + 1)]
    sub_dirs.append(binding_id)
    binding_path = os.path.join(set_id, *sub_dirs)
    return binding_path


def calculate_batch_size(col_size):
    """
    Return a suitable download batch size for a collection.
    """
    if col_size < 500:
        return min(col_size, 10)
    if col_size < 50000:
        return 100
    if col_size < 500000:
        return 250
    else:
        return 500


def split_into_batches(bindings):
    """
    Split a collection into download batches.
    """
    col_size = len(bindings)
    batch_size = calculate_batch_size(col_size)
    batches = [bindings[i : i + batch_size] for i in range(0, col_size, batch_size)]
    return batches


def divide_bindings_to_images(set_id):
    """
    Return (name for image, list of bindings) for each batch of image content
    """
    with open(f"binding_ids/{set_id}/binding_ids", "r") as f:
        bindings = f.read().splitlines()

    img_contents = []

    if len(bindings) < 10000:
        img_contents.append((set_id, len(bindings)))
        return img_contents

    for i in range(10):
        img_name = f"{set_id}_{i}"
        img_bindings = [b for b in bindings if binding_id_from_dc(b).startswith(str(i))]
        if len(img_bindings) > 100000:
            for j in range(10):
                new_img_name = f"{set_id}_{i}{j}"
                new_img_bindings = [
                    b
                    for b in img_bindings
                    if binding_id_from_dc(b).startswith(f"{i}{j}")
                ]
                if new_img_bindings:
                    img_contents.append((new_img_name, len(new_img_bindings)))
        else:
            if img_bindings:
                img_contents.append((img_name, len(img_bindings)))

    return img_contents
