"""
Representation of a single file (e.g. an XML file with OCR results for a
page from a newspaper, or a jpeg showing the scanned page).
"""

from enum import Enum


class File:
    """
    A file originating from NLF.
    """  # TODO

    def __init__(self, checksum, algorithm, location_xlink, content_type):
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
        self.content_type = content_type


class ContentType(Enum):
    """
    The supported types of :class:`~harvester.file.File`.

    These do not represent the file type as such, e.g. "image/jpeg", but the type of the
    content instead.
    """

    ALTO_XML = "alto"
    ACCESS_IMAGE = "access_img"
