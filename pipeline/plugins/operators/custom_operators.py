import os
import re
from requests.exceptions import RequestException

from airflow.models import BaseOperator
from airflow.providers.ssh.hooks.ssh import SSHHook
from airflow.models import Connection
from airflow import settings

from harvester.mets import METS, METSFileEmptyError
from harvester.file import ALTOFile
from harvester import utils


class CreateConnectionOperator(BaseOperator):
    """
    Create any type of Airflow connection.

    :param conn_id: Connection ID
    :param conn_type: Type of connection
    :param host: Host URL
    :param schema: Schema (e.g. HTTPS)
    """

    def __init__(self, conn_id, conn_type, host, schema, **kwargs):
        super().__init__(**kwargs)
        self.conn_id = conn_id
        self.conn_type = conn_type
        self.host = host
        self.schema = schema

    def execute(self, context):
        session = settings.Session()
        conn_ids = [conn.conn_id for conn in session.query(Connection).all()]
        if self.conn_id not in conn_ids:
            self.log.info(
                "Creating a new %s connection with ID %s", self.conn_type, self.conn_id
            )
            conn = Connection(
                conn_id=self.conn_id,
                conn_type=self.conn_type,
                host=self.host,
                schema=self.schema,
            )
            session.add(conn)
            session.commit()


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
        ignore_files_set = set(),
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.sftp_client = sftp_client
        self.ssh_client = ssh_client
        self.dc_identifier = dc_identifier
        self.output_directory = output_directory
        self.ignore_files_set = ignore_files_set,

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
        _, stdout, _ = self.ssh_client.exec_command(
            f"rm -f {tmp_file}"
        )

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

        file_name_in_image = re.sub('^.+batch_[^/]', '/', str(self.output_file))
        if file_name_in_image in self.ignore_files:
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
                raise RequestException(
                    f"METS download {self.dc_identifier} failed: {e.response}"
                )
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


class SaveAltosSFTPOperator(SaveFilesSFTPOperator):
    """
    Save ALTO files for one binding on remote filesystem using SSH connection.

    :param mets_path: Path to the METS file of the binding
    """

    def __init__(self, mets_path, **kwargs):
        super().__init__(**kwargs)
        self.mets_path = mets_path

    def execute(self, context):
        mets = METS(self.dc_identifier, self.sftp_client.file(str(self.mets_path), "r"))
        alto_files = mets.files_of_type(ALTOFile)

        self.ensure_output_location()

        for alto_file in alto_files:
            output_file = self.output_directory / alto_file.filename

            file_name_in_image = re.sub('^.+batch_[^/]', '/', str(self.output_file))
            if file_name_in_image in self.ignore_files:
                continue

            tmp_output_file = self.tmp_path(output_file)

            with self.sftp_client.file(str(tmp_output_file), "wb") as file:
                try:
                    alto_file.download(
                        output_file=file,
                        chunk_size=10 * 1024 * 1024,
                    )
                except RequestException as e:
                    self.delete_temporary_file(tmp_output_file)
                    self.log.error(
                        "ALTO download with URL %s failed: %s",
                        alto_file.download_url,
                        e.response,
                    )
                    continue

            exit_status = self.move_file_to_final_location(tmp_output_file, output_file)

            if exit_status != 0:
                self.log.error(
                    "Moving ALTO file %s from tmp to destination failed",
                    alto_file.download_url,
                )


class DownloadBindingBatchOperator(BaseOperator):
    """
    Download a batch of bindings.

    :param batch: a list of DC identifiers
    :param ssh_conn_id: SSH connection id
    :param target_directory: Root of the image directory hierarchy (containing batches)
    :param api: OAI-PMH api
    """

    def __init__(
        self,
        batch,
        ssh_conn_id,
        target_directory,
        api,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.batch = batch
        self.ssh_conn_id = ssh_conn_id
        self.target_directory = target_directory
        self.api = api

    def execute(self, context):
        ssh_hook = SSHHook(ssh_conn_id=self.ssh_conn_id)
        with ssh_hook.get_conn() as ssh_client:
            sftp_client = ssh_client.open_sftp()

            for dc_identifier in self.batch:
                binding_id = utils.binding_id_from_dc(dc_identifier)
                tmp_binding_path = (
                    self.target_directory / utils.binding_download_location(binding_id)
                )
                mets_directory = tmp_binding_path / "mets"

                mets_operator = SaveMetsSFTPOperator(
                    task_id=f"save_mets_{binding_id}",
                    api=self.api,
                    sftp_client=sftp_client,
                    ssh_client=ssh_client,
                    dc_identifier=dc_identifier,
                    output_directory=mets_directory,
                )

                mets_operator.execute(context={})

                SaveAltosSFTPOperator(
                    task_id=f"save_altos_{binding_id}",
                    mets_path=mets_operator.output_file,
                    sftp_client=sftp_client,
                    ssh_client=ssh_client,
                    dc_identifier=dc_identifier,
                    output_directory=tmp_binding_path / "alto",
                ).execute(context={})

class StowBindingBatchOperator(BaseOperator):
    """
    Download a batch of bindings, typically to a faster smaller drive, make a
    .tar file typically in a slower bigger drive, and delete the downloaded
    files.

    :param batch_with_index: tuple of (list of DC identifiers, batch index)
    :param ssh_conn_id: SSH connection id
    :param tmp_download_directory: Root of the fast temporary directory (containing batch directories)
    :param tar_directory: Root of the directory for tar files
    :param api: OAI-PMH api
    """

    def __init__(
        self,
        batch_with_index,
        ssh_conn_id,
        tmp_download_directory,
        tar_directory,
        api,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.batch_with_index = batch_with_index
        self.ssh_conn_id = ssh_conn_id
        self.tmp_download_directory = tmp_download_directory
        self.tar_directory = tar_directory
        self.api = api

    def create_tar_archive(self, target_file, source_dir):
        """
        Create tar file target_file out of contents of source_dir.

        :return: Exit status from tar
        """
        _, stdout, _ = self.ssh_client.exec_command(
            f"tar --create --file {target_file} --directory={source_dir} ."
        )

        return stdout.channel.recv_exit_status()

    def rmtree(self, dir_path):
        """
        Recursively delete directory.

        :return: Exit status from tar
        """
        _, stdout, _ = self.ssh_client.exec_command(
            f"rm -rf {dir_path}"
        )

        return stdout.channel.recv_exit_status()

    def get_ignore_files_set(self, sftp_client):
        """
        Return a set of paths that we don't need to download.
        """
        retval = set()
        ignore_files_filename = self.tmp_download_dir/"existing_files.txt"
        if utils.remote_file_exists(sftp_client,
                                    ignore_files_filename):
            with sftp_client.open(ignore_files_filename) as ignore_files_fobj:
                ignore_files_contents = str(ignore_files_fobj.read(), encoding='utf-8')
                retval = set(ignore_files_contents.split('\n'))
        return retval

    def execute(self, context):
        ssh_hook = SSHHook(ssh_conn_id=self.ssh_conn_id)
        with ssh_hook.get_conn() as ssh_client:
            sftp_client = ssh_client.open_sftp()
            ignore_files_set = self.get_ignore_files_set(sftp_client)
            batch, batch_num = batch_with_index
            for dc_identifier in batch:
                binding_id = utils.binding_id_from_dc(dc_identifier)
                tmp_binding_path = (
                    self.tmp_download_directory / f"batch_{batch_num}" / utils.binding_download_location(binding_id)
                )

                SaveMetsSFTPOperator(
                    task_id=f"save_mets_{binding_id}",
                    api=self.api,
                    sftp_client=sftp_client,
                    ssh_client=ssh_client,
                    dc_identifier=dc_identifier,
                    output_directory=tmp_binding_path / "mets",
                    ignore_files_set=ignore_files_set,
                ).execute(context={})

                SaveAltosSFTPOperator(
                    task_id=f"save_altos_{binding_id}",
                    mets_path=mets_operator.output_file,
                    sftp_client=sftp_client,
                    ssh_client=ssh_client,
                    dc_identifier=dc_identifier,
                    output_directory=tmp_binding_path / "alto",
                    ignore_files_set=ignore_files_set,
                ).execute(context={})

                if create_tar_archive(
                        f"{tar_directory}/{batch_num}.tar",
                        f"{tmp_binding_path}") != 0:
                    self.log.error(
                        f"Failed to create tar file for batch {batch_num} from tmp to destination failed")

                if rmtree(f"{tmp_binding_path}") != 0:
                    self.log.error(
                        f"Failed to clean up downloads for {batch_num}")

class PrepareDownloadLocationOperator(BaseOperator):
    """
    Prepare download location for data that goes into one disk image.

    This consists of:
    - creating the destination directory if it does not exist
    - extracting the contents of the previous corresponding image if one
      is found in the given ``image_output_dir``

    :param ssh_conn_id: SSH connection id
    :param file_download_dir: Path of the output directory
    :type file_download_dir: :class:`pathlib.Path`
    :param old_image_path: Path of the corresponding image created
                           during the previous download.
    :type old_image_path: :class:`pathlib.Path`
    :param env_dict: Environment variable dictionary, in particular
                     for $PATH, to pass to exec_command().

    """

    def __init__(
        self,
        ssh_conn_id,
        file_download_dir,
        old_image_path,
        env_dict = {}
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.ssh_conn_id = ssh_conn_id
        self.old_image_path = old_image_path
        self.file_download_dir = file_download_dir
        self.env_dict = {}

    def extract_image(self, ssh_client):
        """
        Extract contents of a disk image in given path.
        """
        ssh_client.exec_command(
            f"unsquashfs -d {self.file_download_dir} {self.old_image_path}"

    def create_file_listing_from_image(self, ssh_client):
        """
        Extract file listing of a disk image in given path to file_download_dir.
        """
        ssh_client.exec_command(
            f'unsquashfs -d "" -lc {self.old_image_path} > {self.file_download_dir}/existing_files.txt',
            environment = self.env_dict
        )

    def create_image_folder(self, sftp_client, image_dir_path):
        """
        Create folder to store image contents in.
        """
        utils.make_intermediate_dirs(
            sftp_client=sftp_client,
            remote_directory=image_dir_path,
        )

    def execute(self, context):
        ssh_hook = SSHHook(ssh_conn_id=self.ssh_conn_id)
        with ssh_hook.get_conn() as ssh_client:
            sftp_client = ssh_client.open_sftp()

            self.create_image_folder(sftp_client, self.file_download_dir)

            if utils.remote_file_exists(sftp_client, self.old_image_path):
                self.extract_image(
                    ssh_client,
                )


class CreateImageOperator(BaseOperator):
    """
    Create a disk image of the given source data.

    Before a new image is created, the source data is checked for temporary files (i.e.
    ones with extension ".tmp"). If temporary files are found, the source data is deemed
    erroneous and an exception is raised.

    The image creation is a three-step process:
    1. Create a temporary image
    2. Remove the old image from the intended final location (if present)
    3. Move the newly-created image to the final location

    :param ssh_conn_id: SSH connection id
    :param data_source: Path to the directory that contains the data that is to be
                        stored in the newly-created image.
    :type data_source: :class:`pathlib.Path`
    :param image_path: Path to which the newly-created image is written.
    :type image_path: :class:`pathlib.Path`
    """

    def __init__(
        self,
        ssh_conn_id,
        data_source,
        image_path,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.ssh_conn_id = ssh_conn_id
        self.data_source = data_source
        self.image_path = image_path

    def ssh_execute_and_raise(self, ssh_client, command):
        """
        Run the given command and raisie ImageCreationError on non-zero return value.
        """
        _, stdout, stderr = ssh_client.exec_command(command)

        self.log.debug(stdout)

        exit_code = stdout.channel.recv_exit_status()
        if exit_code != 0:
            error_message = "\n".join(stderr.readlines())
            raise ImageCreationError(
                f"Command {command} failed (exit code {exit_code}). Stderr output:\n"
                f"{error_message}"
            )

    def temporary_files_present(self, sftp_client, directory):
        """
        Report whether temporary files are present in the given directory.

        Files with suffix ".tmp" are considered to be temporary. Files in subdirectories
        of arbitrary depth are also inspected.

        NB: this operation requires listing all files within the directory, so it is not
        suitable for use on Lustre with large directories or directory trees.

        :returns: True if temporary file(s) were found, otherwise False
        """
        for item in sftp_client.listdir(str(directory)):
            if item.endswith(".tmp"):
                return True

        return False

    def execute(self, context):
        tmp_image_path = self.image_path.with_suffix(".sqfs.tmp")

        ssh_hook = SSHHook(ssh_conn_id=self.ssh_conn_id)
        with ssh_hook.get_conn() as ssh_client:
            with ssh_client.open_sftp() as sftp_client:

                if self.temporary_files_present(sftp_client, self.data_source):
                    raise ImageCreationError(
                        "Temporary files found in image data source directory, "
                        "halting image creation"
                    )

                self.log.info("Creating temporary image in %s on Puhti", tmp_image_path)
                self.ssh_execute_and_raise(
                    ssh_client,
                    f"mksquashfs {self.data_source} {tmp_image_path} -mem 2G",
                )

                self.log.info(
                    "Attempting deletion of old image %s on Puhti", self.image_path
                )
                try:
                    sftp_client.remove(str(self.image_path))
                except FileNotFoundError:
                    self.log.info("Old image not present, no removal needed")
                else:
                    self.log.info("Old image removed")

                self.log.info(
                    "Moving temporary image %s to final location %s on Puhti",
                    tmp_image_path,
                    self.image_path,
                )
                sftp_client.posix_rename(str(tmp_image_path), str(self.image_path))

                self.log.info(
                    "Removing the temporary source directory tree %s",
                    self.data_source,
                )
                self.ssh_execute_and_raise(ssh_client, f"rm -r {self.data_source}")


class ImageCreationError(Exception):
    """
    Error raised when an error occurs during the disk image creation/overwrite process
    """
