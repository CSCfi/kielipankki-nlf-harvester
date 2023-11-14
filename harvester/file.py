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

    def __init__(self, binding_dc_identifier, page_number):
        """
        Create a new file

        :param binding_dc_identifier: DC identifier of the binding to which this file
                                      belongs
        :type binding_dc_identifier: String
        :param page_number: The page number for the page which this file represents in
                            binding.
        :type page_number: int
        """
        self.binding_dc_identifier = binding_dc_identifier
        self.page_number = page_number

    @property
    def file_extension(self):
        """
        Return the file type extension for this file.

        :return: File type suffix, including the leading dot (".xml")
        :rtype: str
        """
        raise NotImplementedError(
            "file_extension must be defined separately for each subclass of File"
        )

    @property
    def filename(self):
        """
        Name of the file constructed from page number and file type appropriate suffix.

        The page number is padded with leading zeroes so that it is always five digits
        long (e.g. 00002.jp2).

        :return: File name (e.g. "00002.jp2")
        :rtype: str
        """
        return f"{self.page_number:05}{self.file_extension}"

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


class SkippedFile(File):
    """
    A class for files that are listed in METS but do not need download logic.

    These can be e.g. versions of scanned pages with rulers etc present in the image,
    that are not normally available for users.
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
        return f"{self.binding_dc_identifier}/page-{self.page_number}.xml"

    def _default_file_dir(self):
        """
        The default subdirectory (structure) for the downloaded files of this type.

        :return: The relative Path for the directory in which the downloaded files of
            this type are placed under the base path.
        :rtype: :class:`pathlib.Path`
        """
        return Path(utils.binding_id_from_dc(self.binding_dc_identifier)) / "alto"

    @property
    def file_extension(self):
        return ".xml"


class AccessImageFile(File):
    """
    A jpg file containing a scanned page/sheet from a binding
    """

    def __init__(self, binding_dc_identifier, page_number, location_xlink):
        """
        Create a new file

        :param location_xlink: Location of the file as given in METS.
        :type location_xlink: String
        :param binding_dc_identifier: DC identifier of the binding to which this file
                                      belongs
        :type binding_dc_identifier: String
        :param page_number: The page number for the page which this file represents in
                            binding.
        :type page_number: int
        """
        self.location_xlink = location_xlink
        super().__init__(binding_dc_identifier, page_number)

    @property
    def download_url(self):
        """
        The URL from which this file can be downloaded from NLF.

        These are in consist of the full DC identifier (e.g.
        "https://digi.kansalliskirjasto.fi/sanomalehti/binding/1426186"), directory
        "image" and the page number (e.g. "1"), no extension. The resulting URL will be
        something like
        "https://digi.kansalliskirjasto.fi/sanomalehti/binding/1426186/image/1"
        """
        filename_root = Path(self.filename).stem
        page_number = re.search(r"[1-9]\d*", filename_root).group(0)
        return f"{self.binding_dc_identifier}/image/{page_number}"

    @property
    def file_extension(self):
        """
        Return the file type extension for this file.

        Image files could have different suffixes and we have the location_xlink readily
        available for access images, so we can parse them dynamically.

        :return: File type suffix, including the leading dot (".xml")
        :rtype: str
        """
        return "".join(Path(self.location_xlink).suffixes)


class UnknownFileException(ValueError):
    """
    Exception raised when the file corresponding to a File element cannot be determined
    """
