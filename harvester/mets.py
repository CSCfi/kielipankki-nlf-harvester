"""
Tools for reading and interpreting METS files.
"""

from lxml import etree


# Due to security reasons related to executing C code, pylint does not have an
# accurate view into the lxml library. This disables false alarms.
# pylint: disable=c-extension-no-member


class METS:
    """
    An interface for handling METS files.
    """
    # This is expected to change when the software develops
    # pylint: disable=too-few-public-methods

    def __init__(self, mets_path, encoding="utf-8"):
        """
        Create a new METS file object.

        :param mets_path: Path to the METS file.
        :param encoding: Text encoding of the METS file. Defaults to utf-8.
        """
        self.mets_path = mets_path
        self.encoding = encoding

    def _file_location(self, file_element):
        """
        Return the href to location of the given file element from METS.

        The file must have exactly one location.

        :param file_element: <file> element from METS
        :type file_element: class:`lxml.etree._Element`
        :raises METSLocationParseError: Raised if the location cannot be
                determined, e.g. too many locations.
        """
        children = file_element.getchildren()
        if len(children) != 1:
            raise METSLocationParseError(
                    "Expected 1 location, found {len(children)}"
                    )
        return children[0].attrib['{http://www.w3.org/TR/xlink}href']

    def checksums(self):
        """
        Iterate over all checksums listed in METS.

        :return: All files listed in the METS document, together with their
            checksums and the algorithms used when calculating them.
        :rtype: Iterator[dict]
        """
        with open(self.mets_path, 'r', encoding=self.encoding) as mets_file:
            mets_tree = etree.parse(mets_file)
        files = mets_tree.xpath(
                'mets:fileSec/mets:fileGrp/mets:file',
                namespaces={'mets': 'http://www.loc.gov/METS/'}
                )
        for file_element in files:

            yield {
                    'checksum': file_element.attrib['CHECKSUM'],
                    'algorithm': file_element.attrib['CHECKSUMTYPE'],
                    'location': self._file_location(file_element),
                    }


class METSLocationParseError(ValueError):
    """
    Exception raised when location of a file cannot be determined.
    """
