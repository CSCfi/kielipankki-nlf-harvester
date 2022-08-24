"""
Fetch data from an OAI-PMH API of the National Library of Finland
"""

from sickle import Sickle


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
            yield record.header.identifier.split(':')[-1]
