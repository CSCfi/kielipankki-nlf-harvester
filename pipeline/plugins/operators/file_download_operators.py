"""
Airflow operators for downloading individual files to a remote location
"""

import re
from requests.exceptions import RequestException

from airflow.models import BaseOperator

from harvester.mets import METS, METSFileEmptyError
from harvester.file import ALTOFile
from harvester import utils


class SaveFilesSFTPOperator(BaseOperator):
    """
    Save file to a remote filesystem using SSH connection.

    :param sftp_client: SFTPClient
    :param ssh_client: SSHClient
    :param dc_identifier: DC identifier of binding
    :param output_directory: Directory in which the files are saved
    :param ignore_files_set: Set of paths not to download
    """

    def __init__(
        self,
        sftp_client,
        ssh_client,
        dc_identifier,
        output_directory,
        ignore_files_set=set(),
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.sftp_client = sftp_client
        self.ssh_client = ssh_client
        self.dc_identifier = dc_identifier
        self.output_directory = output_directory
        self.ignore_files_set = ignore_files_set

    def ensure_output_location(self):
        """
        Make sure that the output directory exists

        Creates all intermediate directories too, if necessary.
        """
        utils.make_intermediate_dirs(
            sftp_client=self.sftp_client,
            remote_directory=self.output_directory,
        )

    def tmp_path(self, output_file):
        """
        Return the path to a temporary file corresponding to output_file.

        The temporary path is formed by appending ``.tmp`` to the final output path.

        :output_file: Path representing the final output location
        :type output_file: :class:`pathlib.Path`
        :return: Path representing the corresponding temporary file
        :rtype: :class:`pathlib.Path`
        """
        return output_file.with_suffix(output_file.suffix + ".tmp")

    def move_file_to_final_location(self, tmp_output_file, output_file):
        """
        Move file from temporary to final location.

        :return: Exit status from bash command
        """
        _, stdout, _ = self.ssh_client.exec_command(
            f"mv {tmp_output_file} {output_file}"
        )

        return stdout.channel.recv_exit_status()

    def delete_temporary_file(self, tmp_file):
        """
        Delete file if its name ends with ".tmp", else do nothing.

        :return: Exit status from bash command, or 0 if did nothing
        """
        if not str(tmp_file).endswith(".tmp"):
            return 0
        _, stdout, _ = self.ssh_client.exec_command(f"rm -f {tmp_file}")

        return stdout.channel.recv_exit_status()

    def execute(self, context):
        raise NotImplementedError(
            "execute() must be defined separately for each file type."
        )


class SaveMetsSFTPOperator(SaveFilesSFTPOperator):
    """
    Save a METS file remote a filesystem using SSH connection.

    :param api: API from which to download the file
    """

    def __init__(self, api, **kwargs):
        super().__init__(**kwargs)
        self.api = api

    @property
    def output_file(self):
        """
        Absolute path of the downloaded METS file

        :return: Absolute path of the downloaded METS
        :rtype: :class:`pathlib.Path`
        """
        return self.output_directory / utils.mets_file_name(self.dc_identifier)

    def execute(self, context):

        file_name_in_image = re.sub("^.+batch_[^/]", "", str(self.output_file))
        if file_name_in_image in self.ignore_files_set:
            return

        tmp_output_file = self.tmp_path(self.output_file)

        self.ensure_output_location()

        with self.sftp_client.file(str(tmp_output_file), "w") as file:
            try:
                self.api.download_mets(
                    dc_identifier=self.dc_identifier, output_mets_file=file
                )
            except RequestException as e:
                self.delete_temporary_file(tmp_output_file)
                if e.response is not None:
                    self.log.error(
                        f"METS download {self.dc_identifier} failed with "
                        f"{e.response.status_code}, will retry and/or continue with "
                        f"others"
                    )
                raise e
            except OSError as e:
                raise OSError(
                    f"Writing METS {self.dc_identifier} to file failed with error "
                    f"number {e.errno}"
                )

        if not utils.remote_file_exists(self.sftp_client, tmp_output_file):
            raise METSFileEmptyError(f"METS file {self.dc_identifier} is empty.")

        exit_status = self.move_file_to_final_location(
            tmp_output_file, self.output_file
        )

        if exit_status != 0:
            raise OSError(
                f"Moving METS file {self.dc_identifier} from tmp to destination failed"
            )


class SavePageFilesSFTPOperator(SaveFilesSFTPOperator):
    """
    A base class for downloading different representations of pages in a binding.
    """

    def __init__(self, mets_path, **kwargs):
        """
        :param mets_path: Path to the METS file for the binding
        """
        super().__init__(**kwargs)
        self.mets_path = mets_path

    @property
    def file_type(self):
        """
        String representation of the file type to be downloaded.
        """
        raise NotImplementedError("File type string must be defined in subclasses")

    def files(self, mets):
        """
        Iterable containing all of the files that are to be downloaded.

        :param mets: :class:`harvester.mets.METS` for the binding
        :returns: Iterable containing the files represented as
                  :class:`harvester.file.File`.
        """
        raise NotImplementedError("Files list must be implemented in subclasses")

    def _report_errors(
        self, failed_404_count, failed_401_count, skipped_already_done, total_alto_files
    ):
        """
        Log information about reasons some files were not successfully downloaded
        """
        if failed_404_count > 0:
            self.log.error(
                f"When downloading {self.file_type} files for binding "
                f"{self.dc_identifier}, {failed_404_count}/{total_alto_files} files "
                f"failed with a 404"
            )
        if failed_401_count > 0:
            self.log.error(
                f"When downloading {self.file_type} files for binding "
                f"{self.dc_identifier}, {failed_401_count}/{total_alto_files} files "
                f"failed with a 401"
            )
        if skipped_already_done > 0:
            self.log.info(
                f"When downloading {self.file_type} files for binding "
                f"{self.dc_identifier}, {skipped_already_done}/{total_alto_files} "
                f"were skipped as already downloaded"
            )

    def execute(self, context):
        """
        Download all files of specific type listed in METS.
        """
        mets = METS(self.dc_identifier, self.sftp_client.file(str(self.mets_path), "r"))
        downloaded_files = self.files(mets)

        self.ensure_output_location()

        total_files = 0
        failed_404_count = 0
        failed_401_count = 0
        skipped_already_done = 0
        mark_failed = False

        for file_ in downloaded_files:
            total_files += 1
            output_file = self.output_directory / file_.filename

            if utils.remote_file_exists(self.sftp_client, output_file):
                skipped_already_done += 1
                continue

            tmp_output_file = self.tmp_path(output_file)

            with self.sftp_client.file(str(tmp_output_file), "wb") as file:
                try:
                    file_.download(
                        output_file=file,
                        chunk_size=10 * 1024 * 1024,
                    )
                except RequestException as e:
                    self.delete_temporary_file(tmp_output_file)
                    mark_failed = True
                    if e.response is None:
                        # There is no response if e is eg. a ReadTimeout
                        continue
                    if e.response.status_code == 404:
                        failed_404_count += 1
                    elif e.response.status_code == 401:
                        failed_401_count += 1
                    else:
                        self.log.error(
                            "%s file download with URL %s failed: %s",
                            self.file_type,
                            file_.download_url,
                            e.response,
                        )
                        raise e
                    continue

            if self.move_file_to_final_location(tmp_output_file, output_file) != 0:
                self.log.error(
                    "Moving %s file %s from tmp to destination failed",
                    self.file_type,
                    file_.download_url,
                )

            self._report_errors(
                failed_404_count,
                failed_401_count,
                skipped_already_done,
                total_files,
            )

        if mark_failed:
            raise DownloadBatchError


class SaveAltosSFTPOperator(SavePageFilesSFTPOperator):
    """
    Save ALTO files for one binding on remote filesystem using SSH connection.
    """

    def files(self, mets):
        return mets.files_of_type(ALTOFile)

    @property
    def file_type(self):
        return "ALTO"


class DownloadBatchError(Exception):
    """
    Error raised when an error occurs during the downloading and storing of a download batch
    """
