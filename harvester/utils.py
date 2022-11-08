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


def make_intermediate_dirs(sftp_client, remote_directory) -> None:
    """
    Create all the intermediate directories in a remote host and ensure that
    the download path is created correctly.

    :param sftp_client: A Paramiko SFTP client.
    :param remote_directory: Absolute Path of the directory containing the file
    :return:
    """
    if remote_directory == "/":
        sftp_client.chdir("/")
        return
    if remote_directory == "":
        return
    try:
        sftp_client.chdir(remote_directory)
    except IOError:
        dirname, basename = os.path.split(remote_directory.rstrip("/"))
        make_intermediate_dirs(sftp_client, dirname)
        sftp_client.mkdir(basename)
        sftp_client.chdir(basename)
        return


def construct_file_download_location(
    file, base_path=None, file_dir=None, filename=None
):
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
