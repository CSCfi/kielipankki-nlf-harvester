"""
Fetch data from an OAI-PMH API of the National Library of Finland
"""

from sickle import Sickle
import requests


class PMH_API():
    """
    Interface for fetching data from an OAI-PMH API
    """

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
        records = self._sickle.ListRecords(metadataPrefix='oai_dc', set=set_id)
        for record in records:
            yield record.metadata['identifier'][0]

    
    def binding_id_from_dc(self, dc_identifier):
        """
        Parse binding ID from dc_identifier URL.

        :param dc_identifier: DC identifier of a record
        """
        return dc_identifier.split("/")[-1]
    

    def fetch_mets(self, dc_identifier, folder_path, file_name=None):
        """
        Fetch METS as an XML document given a binding ID and save to disk.

        :param dc_identifier: DC identifier of a record
        :param folder_path: Path to folder to which the METS file will be stored
        :param file_name: Name of the file to which the METS will be stored (optional parameter)
        """

        mets_url = f"{dc_identifier}/mets.xml?full=true"
        xml_response = requests.get(mets_url)

        if not file_name:
            path = f'{folder_path}/{self.binding_id_from_dc(dc_identifier)}_METS.xml'
        else:
            path = f'{folder_path}/{file_name}'

        with open(path, "w") as file:
            file.write(xml_response.text)

        return xml_response.text


    def fetch_all_mets_for_set(self, set_id, folder_path):
        """
        Fetch and save all METS files for a given set.

        :param set_id: Set (also known as collection) identifier
        """
        dc_iterator = self.dc_identifiers(set_id)

        for identifier in dc_iterator:
            self.fetch_mets(identifier, folder_path)
