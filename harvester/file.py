"""
Representation of a single file (e.g. an XML file with OCR results for a
page from a newspaper, or a jpeg showing the scanned page).
"""

import os
from pathlib import Path
import urllib.request


class File:
    """
    A shared base class for files originating from NLF.
    """

    def __init__(self, checksum, algorithm, location_xlink):
        """
        Create a new file

        :param checksum: Checksum for the file
        :type checksum: String
        :param algorithm: Algorithm used when calculating the checksum, e.g. MD5
        :type algorithm: String
        :param location_xlink: Location of the file as given in METS.
        :type location_xlink: String
        :param content_type: Type of information represented by this file.
        :type content_type: :class:`~harvester.file.ContentType`
        """
        self.checksum = checksum
        self.algorithm = algorithm
        self.location_xlink = location_xlink

    @classmethod
    def file_from_element(cls, file_element):
        """
        Return new subclass object representing the given file element.

        :param file_element: A ``file`` element from METS. The
            information for the parent and child elements must be
            accessible too.
        :type file_element: :class:`lxml.etree._Element`
        """
        children = file_element.getchildren()
        if len(children) != 1:
            raise METSLocationParseError("Expected 1 location, found {len(children)}")
        location = children[0].attrib["{http://www.w3.org/TR/xlink}href"]

        parent = file_element.getparent()

        if (
            parent.attrib["USE"] in ["alto", "Text"]
            and parent.attrib["ID"] == "ALTOGRP"
        ):
            file_cls = ALTOFile
        else:
            file_cls = UnknownTypeFile

        return file_cls(
            checksum=file_element.attrib["CHECKSUM"],
            algorithm=file_element.attrib["CHECKSUMTYPE"],
            location_xlink=location,
        )

    def download_url(self, dc_identifier):
        """
        The URL from which this file can be downloaded from NLF.

        :param dc_identifier: Dublin Core identifier for the binding to which this file
            belongs. These identifiers are of form
            https://digi.kansalliskirjasto.fi/sanomalehti/binding/[BINDING ID] and thus
            the base of the download URL.
        :type dc_identifier: String
        """
        raise NotImplementedError(
            "download_url must be defined separately for each subclass of File"
        )

    def _default_base_path(self):
        """
        TODO
        """
        return Path(os.getcwd())

    def _default_file_dir(self, dc_identifier):
        """
        TODO
        """
        raise NotImplementedError("todo")  # TODO better msg, implement in subclasses

    def _default_file_name(self):
        """
        TODO
        """
        raise NotImplementedError("todo")  # TODO better msg, implement in subclasses

    def _construct_download_location(self, base_path, file_dir, file_name):
        """
        Return :class:`pathlib.Path`
        """
        if not base_path:
            base_path = self._default_base_path
        if not file_dir:
            file_dir = self._default_file_dir
        if not file_name:
            file_name = self._default_file_name

        return Path(base_path) / Path(file_dir) / Path(file_name)

    def download(self, dc_identifier, base_path=None, file_dir=None, file_name=None):
        """
        Download the file from NLF.

        :param dc_identifier: Dublin Core identifier for the binding to which this file
            belongs. These identifiers are of form
            https://digi.kansalliskirjasto.fi/sanomalehti/binding/[BINDING ID] and thus
            the base of the download URL.
        :type dc_identifier: String
        :param base_path:
        :type base_path:
        :param file_dir:
        :type file_dir:
        :param file_name:
        :type file_name:
        """
        output = self._construct_download_location(base_path, file_dir, file_name)
        with urllib.request.urlopen(self.download_url(dc_identifier)) as source, open(
            output, "wb"
        ) as output_file:
            output_file.write(source.read())


class UnknownTypeFile(File):
    """
    Temporary class for files whose type is not known.

    To be deleted when we figure out the file type detection.
    """

    def __init__(self, checksum, algorithm, location_xlink):
        super().__init__(checksum, algorithm, location_xlink)
        self.file_type = "UnknownTypeFile"


class ALTOFile(File):
    """
    An XML file with contents of a page described using the ALTO schema.
    """

    def __init__(self, checksum, algorithm, location_xlink):
        super().__init__(checksum, algorithm, location_xlink)
        self.file_type = "ALTOFile"

    def download_url(self, dc_identifier):
        """
        The URL from which this file can be downloaded from NLF.

        :param dc_identifier: Dublin Core identifier for the binding to which this file
            belongs. These identifiers are of form
            https://digi.kansalliskirjasto.fi/sanomalehti/binding/[BINDING ID] and thus
            the base of the download URL. Note that this functionality relies on the
            identifier not having a trailing slash (as seems to be the case in NLF
            data).
        :type dc_identifier: String
        """
        href_filename = self.location_xlink.rsplit("/", maxsplit=1)[-1]
        return f"{dc_identifier}/page-{href_filename}"


class METSLocationParseError(ValueError):
    """
    Exception raised when location of a file cannot be determined.
    """
