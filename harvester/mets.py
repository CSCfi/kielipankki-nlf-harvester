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

    def files_of_type(self, filetype):
        """
        Iterate over all files of a given type listed in METS

        :param filetype: Desired filetype of the File objects
        :type filetype: str
        :return: All files of given type listed in METS
        :rtype: Iterator[:class:`~harvester.file.File`]
        """
        files = [file for file in self.files() if file.filetype == filetype]
        for file in files:
            yield file

    def download_files_of_type(
        self, filetype, base_path=None, file_dir=None, file_name=None
    ):
        """
        Download all files of given filetype listed in METS.
        """

        files = self.files_of_type(filetype)

        for file in files:
            file.download(base_path, file_dir, file_name)

    def download_alto_files(self, base_path=None, file_dir=None, file_name=None):
        """
        Download all alto files listed in METS.
        """

        self.download_files_of_type("ALTOFile", base_path, file_dir, file_name)
