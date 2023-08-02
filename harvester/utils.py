"""
General utility functions for working with the OAI-PMH API, metadata and files.
"""

import os
import json
from pathlib import Path
from datetime import datetime


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
    remote_directory_parts = remote_directory.parts
    for i in range(1, len(remote_directory_parts) + 1):
        remote_dir = Path(*remote_directory_parts[:i])
        try:
            sftp_client.mkdir(str(remote_dir))
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


def mets_file_name(dc_identifier):
    """
    Return file name for the METS file corresponding to given DC identifier.

    The resulting file names are in [binding_id]_METS.xml format, e.g. 123456_METS.xml.
    """
    return f"{binding_id_from_dc(dc_identifier)}_METS.xml"


def binding_download_location(binding_id, depth=None):
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
    return os.path.join(*sub_dirs)


def calculate_batch_size(col_size):
    """
    Return a suitable download batch size for a collection.
    """
    if col_size < 500:
        return min(col_size, 10)
    if col_size < 50000:
        return 100
    if col_size < 500000:
        return 500
    else:
        return 2000


def split_into_download_batches(bindings):
    """
    Split a collection into download batches.
    Return a list of tuples, containing the batch itself and the batch index.
    """
    col_size = len(bindings)
    batch_size = calculate_batch_size(col_size)
    batches = [bindings[i : i + batch_size] for i in range(0, col_size, batch_size)]
    return list(zip(batches, range(len(batches))))


def bindings_with_prefix(bindings, prefix):
    """
    Find DC identifiers of which binding ID start with a given prefix.
    """
    return [
        binding
        for binding in bindings
        if binding_id_from_dc(binding).startswith(prefix)
    ]


def assign_bindings_to_images(bindings, max_bindings_per_image, shared_prefix=""):
    """
    Split a list of bindings into images, each image containing no more than
    max_bindings_per_image bindings.
    """
    if len(bindings) <= max_bindings_per_image:
        return {shared_prefix: bindings}
    images = {}
    for i in range(10):
        prefix = shared_prefix + str(i)
        prefixed_bindings = bindings_with_prefix(bindings, prefix)
        images.update(
            assign_bindings_to_images(prefixed_bindings, max_bindings_per_image, prefix)
        )
    return images


def image_for_binding(dc_identifier, image_split):
    """
    Find the image of which prefix matches the prefix of the given binding ID.
    """
    binding_id = binding_id_from_dc(dc_identifier)
    for prefix in image_split:
        if binding_id.startswith(prefix):
            return prefix
    raise ValueError(f"No prefix found for binding {binding_id}")


def assign_update_bindings_to_images(bindings, image_split_file):
    """
    Assign an incoming list of new bindings into existing disk images
    based on their binding ID.
    """
    with open(image_split_file, "r") as json_file:
        image_split = json.load(json_file)
    for dc_identifier in bindings:
        image = image_for_binding(dc_identifier, image_split)
        image_split[image].append(dc_identifier)
    return image_split


def read_bindings(binding_list_dir, set_id):
    """
    Read and return a list of bindings from the latest binding ID file.
    """
    try:
        binding_id_files = [
            datetime.strptime(fname.split("_")[-1], "%Y-%m-%d").date()
            for fname in os.listdir(binding_list_dir / set_id)
        ]
    except FileNotFoundError:
        raise FileNotFoundError(f"No binding ID file found for set {set_id}")
    latest = max(binding_id_files)
    with open(binding_list_dir / set_id / f"binding_ids_{str(latest)}", "r") as f:
        bindings = f.read().splitlines()
    return bindings


def save_image_split(image_split, image_split_dir, set_id):
    if os.path.exists(image_split_dir / f"{set_id}_images.json"):
        return
    if not os.path.exists(image_split_dir):
        os.makedirs(image_split_dir)
    with open(image_split_dir / f"{set_id}_images.json", "w") as json_file:
        image_split = {k: [] for k in image_split}
        json.dump(image_split, json_file)


def remote_file_exists(sftp_client, path):
    """
    Check if a non-empty file already exists in the given remote path.

    :param sftp_client: Client for accessing the remote machine via SFTP
    :type sftp_client: :class:`paramiko.sftp_client.SFTPClient`
    :param path: Path whose existence is to be checked
    :type path: :class:`pathlib.Path`
    :return: True if a non-empty file exists, otherwise False
    """
    try:
        file_size = sftp_client.stat(str(path)).st_size
    except OSError:
        return False
    else:
        if file_size > 0:
            return True
    return False
