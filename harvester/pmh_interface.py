"""
Fetch data from an OAI-PMH API of the National Library of Finland
"""

from datetime import datetime, timezone
from dateutil import parser

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

    def deleted_dc_identifiers(self, set_id, from_date=None):
        """
        Iterate over the DC identifiers of bindings in a set marked as deleted.

        The filtering is done manually from all deleted bindings instead of e.g. doing
        ListIdentifiers since a specific date and filtering based on status="deleted",
        because filtering based on date does not seem to work reliably for deleted
        records.

        :param set_id: Set (also known as collection) identifier
        :param from_timestamp: Earliest time of deletion to consider: earlier deletions
                               are ignored.
        """
        request_params = {"metadataPrefix": "qdc_finna", "set": "deleted"}
        namespace = {"oai": "http://www.openarchives.org/OAI/2.0/"}
        if from_date:
            threshold_time = datetime.strptime(from_date, "%Y-%m-%d").replace(
                tzinfo=timezone.utc
            )
        else:
            threshold_time = None

        for binding in self._sickle.ListRecords(**request_params):
            identifier = binding.xml.xpath(
                "./oai:header/oai:identifier/text()",
                namespaces=namespace,
            )[0]
            sets = binding.xml.xpath(
                "./oai:header/oai:setSpec/text()", namespaces=namespace
            )

            if set_id not in sets:
                continue

            if threshold_time:
                deletion_datestamp = binding.xml.xpath(
                    "./oai:header/oai:datestamp/text()",
                    namespaces=namespace,
                )[0]

                deletion_time = parser.isoparse(deletion_datestamp)
                if deletion_time < threshold_time:
                    continue

            yield identifier

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
