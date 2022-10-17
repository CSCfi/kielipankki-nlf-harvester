"""
Tools for reading and interpreting METS files.
"""

from lxml import etree

from harvester.file import File


# Due to security reasons related to executing C code, pylint does not have an
# accurate view into the lxml library. This disables false alarms.
# pylint: disable=c-extension-no-member


class METS:
    """
    An interface for handling METS files.
    """

    # This is expected to change when the software develops
    # pylint: disable=too-few-public-methods

    def __init__(self, mets_path, binding_dc_identifier, encoding="utf-8"):
        """
        Create a new METS file object.

        :param mets_path: Path to the METS file.
        :param encoding: Text encoding of the METS file. Defaults to utf-8.
        """
        self.mets_path = mets_path
        self.encoding = encoding
        self.binding_dc_identifier = binding_dc_identifier
        self._files = None
        self.binding_id = (
            str(mets_path).split("/")[-1].split("_")[0]
        )  # Not necessarily reliable!
        self.dc_identifier = (
            f"https://digi.kansalliskirjasto.fi/sanomalehti/binding/{self.binding_id}"
        )

    def _file_location(self, file_element):
        """
        Return the href to location of the given file element from METS.

        The file must have exactly one location.

        :param file_element: <file> element from METS
        :type file_element: class:`lxml.etree._Element`
        :raises METSLocationParseError: Raised if the location cannot be
                determined, e.g. too many locations.
        """

    def _ensure_files(self):
        """
        Make sure that self.files is populated
        """
        if self._files:
            return

        with open(self.mets_path, "r", encoding=self.encoding) as mets_file:
            mets_tree = etree.parse(mets_file)
        files = mets_tree.xpath(
            "mets:fileSec/mets:fileGrp/mets:file",
            namespaces={"mets": "http://www.loc.gov/METS/"},
        )

        self._files = []
        for file_element in files:
            self._files.append(
                File.file_from_element(
                    file_element,
                    self.binding_dc_identifier,
                )
            )

    def files(self):
        """
        Iterate over all files listed in METS.

        :return: All files listed in the METS document
        :rtype: Iterator[:class:`~harvester.file.File`]
        """
        self._ensure_files()
        for file in self._files:
            yield file

    def alto_files(self):
        """
        Iterate over all alto files listed in METS.

        :return: All alto files listed in METS document
        :rtype: :rtype: Iterator[:class:`~harvester.file.ALTOFile`]
        """

        alto_files = [file for file in self.files() if file.file_type == "ALTOFile"]
        for file in alto_files:
            yield file

    def download_alto_files(self, base_path=None, file_dir=None):
        """
        Download all alto files listed in METS.
        """

        for i, file in enumerate(self.alto_files()):
            file.download(
                self.dc_identifier,
                base_path,
                file_dir,
                f"{self.binding_id}_alto_{i+1}.xml",
            )
