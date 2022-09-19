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

    def binding_ids(self, set_id):
        """
        Iterate over all binding IDs in the given set.

        :param set_id: Set (also known as collection) identifier
        """
        records = self._sickle.ListRecords(metadataPrefix='oai_dc', set=set_id)
        for record in records:
            #yield record.header.identifier.split(':')[-1]
            yield record.metadata['identifier'][0]

    def fetch_mets(self, binding_id, folder_path, file_name=None):
        """
        Fetch METS as an XML document given a binding ID and save to disk.

        :param binding_id: Binding identifier
        :param folder_path: Path to folder to which the METS file will be stored
        :param file_name: Name of the file to which the METS will be stored (optional parameter)
        """

        mets_url = f"{binding_id}/mets.xml?full=true"
        xml_response = requests.get(mets_url)

        if not file_name:
            path = f'{folder_path}/{binding_id.split("/")[-1]}_METS.xml'
        else:
            path = f'{folder_path}/{file_name}'

        open(path, "w").write(xml_response.text)

        return xml_response.text


    def fetch_all_mets_for_set(self, set_id, folder_path):
        """
        Fetch and save all METS files for a given set.

        :param set_id: Set (also known as collection) identifier
        """
        binding_id_iterator = self.binding_ids(set_id)

        for binding_id in binding_id_iterator:
            self.fetch_mets(binding_id, folder_path)

