"""
General utility functions for working with the OAI-PMH API, metadata and files.
"""

import os


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
