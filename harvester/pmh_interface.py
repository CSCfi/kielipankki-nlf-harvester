"""
Fetch data from an OAI-PMH API of the National Library of Finland
"""

from sickle import Sickle
from pathlib import Path
import requests
import os

from harvester import utils


class PMH_API:
    """
    Interface for fetching data from an OAI-PMH API
    """

    # The name of the class follows the recommendation of PEP-8 to capitalize
    # all letters of an abbreviation. PMHAPI would be hard to read though, so
    # the underscore was added for clarity pylint: disable=invalid-name

    def __init__(self, url):
        """
        :param url: URL of the OAI-PMH API used
        """
        self._sickle = Sickle(url)

    def dc_identifiers(self, set_id):
        """
        Iterate over all DC identifiers in the given set.

        :param set_id: Set (also known as collection) identifier
        """
        records = self._sickle.ListRecords(metadataPrefix="oai_dc", set=set_id)
        for record in records:
            yield record.metadata["identifier"][0]

    def fetch_mets(self, dc_identifier):
        """
        Fetch METS as an XML document given a binding ID and save to disk.

        :param dc_identifier: DC identifier of a record
        :param folder_path: Path to folder to which the METS file will be stored
        :param file_name: Name of the file to which the METS will be stored (optional
            parameter)
        """

        mets_url = f"{dc_identifier}/mets.xml?full=true"
        xml_response = requests.get(mets_url, timeout=5)
        xml_response.raise_for_status()

        return xml_response.text

    def download_mets(
        self,
        download_function,
        dc_identifier,
        sftp_client=None,
        folder_path=None,
        file_name=None,
    ):
        """
        Save fetched METS file to either remote of local file system.

        :param download_function: Function with which to download the METS
        :type download_function: function
        :param dc_identifier: DC identifier of the METS
        :type dc_identifier: str
        :param sftp_client: SFTPClient to connect to the remote host (required for remote download)
        :type sftp_client: paramiko.SFTPClient, optional
        :param folder_path: Path of the folder to which METS is downloaded
        :type folder_path: str, optional
        :param file_name: Name of the file
        :type file_name: str, optional

        """
        return download_function(
            dc_identifier=dc_identifier,
            sftp_client=sftp_client,
            folder_path=folder_path,
            file_name=file_name,
        )

    def download_mets_to_local(
        self, dc_identifier, folder_path=None, file_name=None, **kwargs
    ):
        """
        Save fetched METS file to disk.
        """
        mets_content = self.fetch_mets(dc_identifier)

        if not folder_path:
            folder_path = self._default_mets_path()

        if not file_name:
            file_name = f"{utils.binding_id_from_dc(dc_identifier)}_METS.xml"

        folder_path = Path(folder_path)
        folder_path.mkdir(parents=True, exist_ok=True)

        with open(folder_path / file_name, "w") as file:
            file.write(mets_content)

        return mets_content

    def download_mets_to_remote(
        self, dc_identifier, folder_path, sftp_client, file_name=None
    ):
        """
        Save fetched METS file to remote disk via SFTP client.
        """
        mets_content = self.fetch_mets(dc_identifier)

        if not file_name:
            file_name = f"{utils.binding_id_from_dc(dc_identifier)}_METS.xml"

        utils.make_intermediate_dirs(
            sftp_client=sftp_client, remote_directory=folder_path
        )
        with sftp_client.file(f"{folder_path}/{file_name}", "w") as remote_file:
            remote_file.write(mets_content)

        return mets_content

    def _default_mets_path(self):
        """
        Return folder path to store METS file in.
        """
        return Path(os.getcwd()) / "downloads/mets"

    def download_all_mets_for_set(self, download_function, set_id, folder_path):
        """
        Fetch and save all METS files for a given set.

        :param set_id: Set (also known as collection) identifier
        """
        dc_iterator = self.dc_identifiers(set_id)

        for identifier in dc_iterator:
            self.download_mets(
                download_function, dc_identifier=identifier, folder_path=folder_path
            )
