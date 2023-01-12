"""
Representation of a single file (e.g. an XML file with OCR results for a
page from a newspaper, or a jpeg showing the scanned page).
"""

import os
from pathlib import Path
import requests
import re

from harvester import utils


class File:
    """
    A shared base class for files originating from NLF.
    """

    def __init__(self, checksum, algorithm, location_xlink, binding_dc_identifier):
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
        self.binding_dc_identifier = binding_dc_identifier

    @property
    def filename(self):
        """
        Name of the file as reported in ``self.location_xlink``

        :return: File name part of ``self.location_xlink``
        :rtype: str
        """
        path_without_scheme = self.location_xlink.split("://")[-1]
        return Path(path_without_scheme).name

    @classmethod
    def file_from_element(cls, file_element, binding_dc_identifier):
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
        location = file_element.xpath("./*/@*[local-name()='href']")[0]
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
            binding_dc_identifier=binding_dc_identifier,
        )

    @property
    def download_url(self):
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
        The default root directory for the file structure for downloaded bindings.

        :return: Path to the root of the file structure created for downloaded files.
            Defaults to the current directory.
        :rtype: :class:`pathlib.Path`
        """
        return Path(os.getcwd()) / "downloads"

    def _default_file_dir(self):
        """
        The default subdirectory (structure) for the downloaded files of this type.

        :return: The relative Path for the directory in which the downloaded files of
            this type are placed under the base path.
        :rtype: :class:`pathlib.Path`
        """
        raise NotImplementedError(
            "Default file directory must be set on a per filetype basis"
        )

    def _default_filename(self):
        """
        The default file name for downloaded files.

        :return: File name as a Path object
        :rtype: :class:`pathlib.Path`
        """
        return self.filename

    def _ensure_dir(self, dir_):
        """
        Make sure that the output directory exists
        """

    def download(
        self,
        output_file,
        chunk_size=None,
    ):
        """
        Download file from NLF to either remote or local directory.

        :param output_file: Path where the file will be downloaded
        :type output_file: str or `pathlib.Path`
        :param chunk_size: Chunk size for streaming the request content
        :type chunk_size: int

        """
        with requests.get(self.download_url, timeout=5, stream=True) as source:
            source.raise_for_status()
            for chunk in source.iter_content(chunk_size=chunk_size):
                output_file.write(chunk)


class UnknownTypeFile(File):
    """
    Temporary class for files whose type is not known.

    To be deleted when we figure out the file type detection.
    """


class ALTOFile(File):
    """
    An XML file with contents of a page described using the ALTO schema.
    """

    @property
    def download_url(self):
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
        if not re.match(r"^0+([1-9]+0*)+.xml$", href_filename):
            try:
                href_filename = (
                    f"{re.search(r'([1-9]+0*)+', href_filename).group(0)}.xml"
                )
            except AttributeError:
                raise AttributeError(
                    f"ALTO filename {href_filename} does not follow the accepted convention."
                )
        return f"{self.binding_dc_identifier}/page-{href_filename}"

    def _default_file_dir(self):
        """
        The default subdirectory (structure) for the downloaded files of this type.

        :return: The relative Path for the directory in which the downloaded files of
            this type are placed under the base path.
        :rtype: :class:`pathlib.Path`
        """
        return Path(utils.binding_id_from_dc(self.binding_dc_identifier)) / "alto"


class METSLocationParseError(ValueError):
    """
    Exception raised when location of a file cannot be determined.
    """
