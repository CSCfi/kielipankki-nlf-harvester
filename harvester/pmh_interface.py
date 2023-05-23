"""
Fetch data from an OAI-PMH API of the National Library of Finland
"""

from sickle import Sickle
import requests


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

    def set_ids(self):
        """
        List IDs for all sets available from the API.
        """
        sets = [set_tree.setSpec for set_tree in self._sickle.ListSets()]
        for set_id in sets:
            yield set_id

    def dc_identifiers(self, set_id, from_date=None):
        """
        Iterate over all DC identifiers in the given set.

        :param set_id: Set (also known as collection) identifier
        """
        request_params = {"metadataPrefix": "oai_dc", "set": set_id, "from": from_date}
        binding_ids = self._sickle.ListIdentifiers(**request_params)
        for binding_id in binding_ids:
            yield f"https://digi.kansalliskirjasto.fi/sanomalehti/binding/{binding_id.identifier.rsplit(':')[-1]}"

    def download_mets(self, dc_identifier, output_mets_file):
        """
        Download file from NLF to either remote or local directory.

        :param dc_identifier: DC identifier of a record
        :type dc_identifier: str
        :param output_file: Path where the file will be downloaded
        :type output_file: str or `pathlib.Path`

        """
        mets_url = f"{dc_identifier}/mets.xml?full=true"

        with requests.get(mets_url, timeout=5) as source:
            source.raise_for_status()
            response = source.content
            output_mets_file.write(response)

        return response
