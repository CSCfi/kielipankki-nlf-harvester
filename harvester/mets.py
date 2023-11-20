"""
Tools for reading and interpreting METS files.
"""

from lxml import etree

from harvester.file import AccessImageFile, ALTOFile


# Due to security reasons related to executing C code, pylint does not have an
# accurate view into the lxml library. This disables false alarms.
# pylint: disable=c-extension-no-member


class METS:
    """
    An interface for handling METS files.
    """

    # This is expected to change when the software develops
    # pylint: disable=too-few-public-methods

    def __init__(self, binding_dc_identifier, mets_file, encoding="utf-8"):
        """
        Create a new METS file object.

        :param binding_dc_identifier: DC identifier of binding
        :param mets_file: Open file object of the METS file.
        :param encoding: Text encoding of the METS file. Defaults to utf-8.
        """
        self.binding_dc_identifier = binding_dc_identifier
        self.mets_file = mets_file
        self.encoding = encoding
        self.parser = etree.XMLParser(encoding=self.encoding)
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

        mets_tree = etree.fromstring(self.mets_file.read(), parser=self.parser)
        self.mets_file.close()
        files = mets_tree.xpath(
            "mets:fileSec/mets:fileGrp/mets:file",
            namespaces={"mets": "http://www.loc.gov/METS/"},
        )

        self._files = []

        self._add_access_image_files(file_elements=files)
        self._add_alto_files_for_images()

    def _add_access_image_files(self, file_elements):
        """
        Add an AccessImageFile to self._files for every access image in file_elements

        There doesn't seem to be a reliable way of matching the elements to the
        digi.kansalliskirjasto.fi download URLs (at least the numeric part of file name
        nor the SEQ numbers do not work), so creating the right number of files whose
        page numbers go from 1 to n is prioritized. In all reviewed cases, this also
        seems to be the right order, but this cannot be guaranteed. Fortunately all
        files in a fileGrp have also always had the same file type extension (which is
        the only part utilized from location_xlink).
        """
        page_number = 1
        for file_element in file_elements:
            children = file_element.getchildren()
            if len(children) != 1:
                raise METSLocationParseError(
                    "Expected 1 location, found {len(children)}"
                )
            location = file_element.xpath("./*/@*[local-name()='href']")[0]
            parent = file_element.getparent()

            filegrp_use = parent.attrib["USE"]
            filegrp_id = parent.attrib["ID"]

            if filegrp_use in ["Images", "reference"] and filegrp_id in [
                "IMGGRP",
                "ACIMGGRP",
            ]:
                self._files.append(
                    AccessImageFile(
                        binding_dc_identifier=self.binding_dc_identifier,
                        page_number=page_number,
                        location_xlink=location,
                    )
                )
                page_number += 1

    def _add_alto_files_for_images(self):
        """
        Add an ALTOFile to self._files for every access image already present

        We add ALTOs based on access image files instead of the entries in the METS file
        because METS also lists ALTOs for target images etc even though they are not
        available for downloading via digi.kansalliskirjasto.fi.
        """
        alto_files = []
        for file_ in self._files:
            if not isinstance(file_, AccessImageFile):
                continue

            alto_files.append(
                ALTOFile(
                    binding_dc_identifier=file_.binding_dc_identifier,
                    page_number=file_.page_number,
                )
            )
        self._files = self._files + alto_files

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
        files = [file for file in self.files() if isinstance(file, filetype)]
        for file in files:
            yield file


class METSFileEmptyError(Exception):
    """
    Exception raised when an empty METS file is downloaded.
    """


class METSLocationParseError(ValueError):
    """
    Exception raised when location of a file cannot be determined.
    """
