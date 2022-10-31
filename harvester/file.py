"""
Representation of a single file (e.g. an XML file with OCR results for a
page from a newspaper, or a jpeg showing the scanned page).
"""

import os
from pathlib import Path
import requests

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

    def _construct_download_location(self, base_path, file_dir, filename):
        """
        Return :class:`pathlib.Path`
        """
        if not base_path:
            base_path = self._default_base_path()
        if not file_dir:
            file_dir = self._default_file_dir()
        if not filename:
            filename = self._default_filename()

        return Path(base_path) / Path(file_dir) / Path(filename)

    def _ensure_dir(self, dir_):
        """
        Make sure that the output directory exists
        """

    def download(
        self,
        download_function,
        sftp_client=None,
        chunk_size=1024 * 1024,
        base_path=None,
        file_dir=None,
        filename=None,
    ):
        """
        Download file from NLF to either remote or local directory.

        The output location can be specified with the components ``base_path``,
        ``file_dir`` and ``filename``. If not given, the output location is as
        follows::

         ./downloads/[binding ID]/[type directory]/[filename from location_xlink]
         ^^^^^^^^^^^ ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^ ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
          base_path          file_dir                         filename

        :param download_function: Download function to download the file with
        :type download_function: function
        :param sftp_client: SFTPClient to connect to the remote host (required for remote download)
        :type sftp_client: paramiko.SFTPClient, optional
        :param base_path: The root directory for the file structure for
            downloaded bindings.
        :type base_path: str, optional
        :param file_dir: Output directory for the files, relative to ``base_path``
        :type file_dir: str, optional
        :param filename: Output file name.
        :type filename: str, optional

        """
        download_function(
            sftp_client=sftp_client,
            chunk_size=chunk_size,
            base_path=base_path,
            file_dir=file_dir,
            filename=filename,
        )

    def download_to_local(self, base_path=None, file_dir=None, filename=None, **kwargs):
        """
        Download the file from NLF to a local directory.
        """
        output = self._construct_download_location(base_path, file_dir, filename)
        output.parent.mkdir(parents=True, exist_ok=True)

        with requests.get(self.download_url, timeout=5) as source, open(
            output, "wb"
        ) as output_file:
            source.raise_for_status()
            output_file.write(source.content)

    def download_to_remote(
        self,
        sftp_client,
        chunk_size=1024 * 1024,
        base_path=None,
        file_dir=None,
        filename=None,
    ):
        """
        Download the file from NLF to a remote server.
        """
        output = str(self._construct_download_location(base_path, file_dir, filename))
        utils.make_intermediate_dirs(
            sftp_client=sftp_client, remote_directory=output.rsplit("/", maxsplit=1)[0]
        )

        with requests.get(self.download_url, timeout=5, stream=True) as source:
            source.raise_for_status()
            with sftp_client.file(output, "wb") as remote_file:
                for chunk in source.iter_content(chunk_size=chunk_size):
                    remote_file.write(chunk)


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
