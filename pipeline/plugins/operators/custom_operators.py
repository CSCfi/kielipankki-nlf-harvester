from requests.exceptions import RequestException

from airflow.models import BaseOperator
from airflow.providers.ssh.hooks.ssh import SSHHook
from airflow.models import Connection
from airflow import settings

# pylint does not understand that custom operators are not third party code
# pylint: disable=wrong-import-order

from harvester import utils
from operators.file_download_operators import (
    SaveMetsSFTPOperator,
    SaveAltosSFTPOperator,
    DownloadBatchError,
)


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
        self.mark_failed = False

    def create_tar_archive(self, ssh_client, target_file, source_dir):
        """
        Create tar file target_file out of contents of source_dir.

        :return: Exit status from tar
        """
        _, stdout, _ = ssh_client.exec_command(
            f"tar --create --file {target_file} --directory={source_dir} ."
        )

        return stdout.channel.recv_exit_status()

    def rmtree(self, ssh_client, dir_path):
        """
        Recursively delete directory.

        :return: Exit status from rm
        """
        _, stdout, _ = ssh_client.exec_command(f"rm -rf {dir_path}")

        return stdout.channel.recv_exit_status()

    def get_ignore_files_set(self, sftp_client):
        """
        Return a set of paths that we don't need to download.
        """
        retval = set()
        ignore_files_filename = self.tmp_download_directory / "existing_files.txt"
        if utils.remote_file_exists(sftp_client, ignore_files_filename):
            with sftp_client.open(str(ignore_files_filename)) as ignore_files_fobj:
                ignore_files_contents = str(ignore_files_fobj.read(), encoding="utf-8")
                retval = set(ignore_files_contents.split("\n"))
        return retval

    def temporary_files_present(self, ssh_client, directory):
        """
        Report whether temporary files are present in the given directory.

        Files with suffix ".tmp" are considered to be temporary. Files in subdirectories
        of arbitrary depth are also inspected.

        NB: this operation requires listing all files within the directory, so it is not
        suitable for use on Lustre with large directories or directory trees.

        :returns: True if temporary file(s) were found, otherwise False
        """
        _, stdout, _ = ssh_client.exec_command(
            f'find {directory} -name "*.tmp" | grep .'
        )
        # grep will have exit code 0 only if it found some matches
        return stdout.channel.recv_exit_status() == 0

    def execute_save_files_operator(self, operator, context, retries=3):
        """
        Execute the given operator, log errors and set `mark_failed` if necessary

        The operator can be set to be retried a number of times before marking the task
        as failed.

        :operator: An operator to be executed, must inherit `SaveFilesSFTPOperator`
        :context: Standard Airflow context object.
        :retries: The number of failed tries allowed before marking this task as failed
        :returns: True if the operation succeeded, otherwise False
        """
        try:
            operator.execute(context=context)
        except Exception as e:
            if (not issubclass(type(e), RequestException)) and type(
                e
            ) != DownloadBatchError:
                self.log.error(
                    f"Unexpected exception when downloading {operator.file_type} in "
                    f"{operator.dc_identifier}, continuing anyway: {e}"
                )
            if context["task_instance"].try_number < 3:
                # If we're not on our third try, we'll fail this batch before
                # tar creation. If we *are* on our third task, create tar
                # anyway, succeed in the task, and log failures.
                self.mark_failed = True
            else:
                self.log.error(
                    f"Downloading {operator.file_type} in {operator.dc_identifier} "
                    f"still failing, moving on with image creation"
                )
            return False
        return True

    def execute(self, context):
        ssh_hook = SSHHook(ssh_conn_id=self.ssh_conn_id)
        with ssh_hook.get_conn() as ssh_client:
            sftp_client = ssh_client.open_sftp()
            batch, batch_num = self.batch_with_index
            batch_root = self.tmp_download_directory / f"batch_{batch_num}"
            for dc_identifier in batch:
                binding_id = utils.binding_id_from_dc(dc_identifier)
                tmp_binding_path = batch_root / utils.binding_download_location(
                    binding_id
                )

                mets_operator = SaveMetsSFTPOperator(
                    task_id=f"save_mets_{binding_id}",
                    api=self.api,
                    sftp_client=sftp_client,
                    ssh_client=ssh_client,
                    dc_identifier=dc_identifier,
                    output_directory=tmp_binding_path / "mets",
                    ignore_files_set={},
                )

                mets_downloaded = self.execute_save_files_operator(
                    mets_operator, context
                )

                if not mets_downloaded:
                    continue

                alto_operator = SaveAltosSFTPOperator(
                    task_id=f"save_altos_{binding_id}",
                    mets_path=mets_operator.output_file,
                    sftp_client=sftp_client,
                    ssh_client=ssh_client,
                    dc_identifier=dc_identifier,
                    output_directory=tmp_binding_path / "alto",
                    ignore_files_set={},
                )
                self.execute_save_files_operator(alto_operator, context)

                if self.temporary_files_present(ssh_client, tmp_binding_path):
                    raise DownloadBatchError(
                        "Temporary files found in download batch, "
                        "halting archive creation"
                    )

            if self.mark_failed:
                # Now, after all the downloads are done, we fail if necessary
                # rather than continue with other stuff
                raise DownloadBatchError

            if (
                self.create_tar_archive(
                    ssh_client, f"{self.tar_directory}/{batch_num}.tar", f"{batch_root}"
                )
                != 0
            ):
                self.log.error(
                    f"Failed to create tar file for batch {batch_num} from tmp to destination failed"
                )

            if self.rmtree(ssh_client, f"{batch_root}") != 0:
                self.log.error(f"Failed to clean up downloads for {batch_num}")


class PrepareDownloadLocationOperator(BaseOperator):
    """
    Prepare download location for data that goes into one disk image.

    This consists of:
    - creating the destination directories if they do not exist
    - listing the contents of the previous corresponding image into a text
      file if one is found in the given ``image_output_dir``

    :param ssh_conn_id: SSH connection id
    :param old_image_path: Path of the corresponding image created
                           during the previous download.
    :type old_image_path: :class:`pathlib.Path`
    :param ensure_dirs: Directories needed for the download.
    :type ensure_dirs: :type: iterable of :class:`pathlib.Path`
    :param extra_bin_dir: Path for binaries on remote machine
    :type extra_bin_dir: :class:`pathlib.Path`

    """

    def __init__(
        self,
        ssh_conn_id,
        old_image_path,
        ensure_dirs,
        extra_bin_dir,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.ssh_conn_id = ssh_conn_id
        self.old_image_path = old_image_path
        self.ensure_dirs = ensure_dirs
        self.extra_bin_dir = extra_bin_dir

    def extract_image(self, ssh_client):
        """
        Extract contents of a disk image in given path.
        """
        ssh_client.exec_command(
            f"{self.extra_bin_dir}/unsquashfs -d {self.file_download_dir} {self.old_image_path}"
        )

    def create_file_listing_from_image(self, ssh_client):
        """
        Extract file listing of a disk image in given path to file_download_dir.
        """
        ssh_client.exec_command(
            f'{self.extra_bin_dir}/unsquashfs -d "" -lc {self.old_image_path} > {self.file_download_dir}/existing_files.txt'
        )

    def create_directory(self, sftp_client, path):
        """
        Create directory (and parents) to store files in.
        """
        utils.make_intermediate_dirs(
            sftp_client=sftp_client,
            remote_directory=path,
        )

    def execute(self, context):
        ssh_hook = SSHHook(ssh_conn_id=self.ssh_conn_id)
        with ssh_hook.get_conn() as ssh_client:
            sftp_client = ssh_client.open_sftp()
            for dirpath in self.ensure_dirs:
                self.create_target_folder(sftp_client, dirpath)


class CreateImageOperator(BaseOperator):
    """
    Create a disk image of the given source data.

    The image is first created as a temporary file with the suffix .tmp, and
    if the creation is successful, it is moved into the final location,
    overwriting what was possibly already there.

    :param ssh_conn_id: SSH connection id
    :param data_source: Path to the directory that contains the .tar files
    :type data_source: :class:`pathlib.Path`
    :param image_path: Path to which the newly-created image is written.
    :type image_path: :class:`pathlib.Path`
    :param image_path: Path for binaries on remote machine
    :type image_path: :class:`pathlib.Path`
    """

    def __init__(
        self,
        ssh_conn_id,
        data_source,
        image_path,
        extra_bin_dir,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.ssh_conn_id = ssh_conn_id
        self.data_source = data_source
        self.image_path = image_path
        self.extra_bin_dir = extra_bin_dir

    def ssh_execute_and_raise(self, ssh_client, command):
        """
        Run the given command and raise ImageCreationError on non-zero return value.
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

    def execute(self, context):
        tmp_image_path = self.image_path.with_suffix(".sqfs.tmp")

        ssh_hook = SSHHook(ssh_conn_id=self.ssh_conn_id)
        with ssh_hook.get_conn() as ssh_client:
            with ssh_client.open_sftp() as sftp_client:
                old_image_to_tar_cmd = ":"  # no-op
                if utils.remote_file_exists(sftp_client, self.image_path):
                    old_image_to_tar = (
                        f"{self.extra_bin_dir}/sqfs2tar {self.image_path}"
                    )
                    self.log.info(
                        "Will use old image %s as a tar source for new image on Puhti",
                        self.image_path,
                    )
                mksquashfs_cmd = f"{self.extra_bin_dir}/sqfstar -ignore-zeros -mem 2G {tmp_image_path}"
                self.log.info("Creating temporary image in %s on Puhti", tmp_image_path)
                self.ssh_execute_and_raise(
                    ssh_client,
                    f"{old_image_to_tar_cmd} | cat - {self.data_source}/*.tar | {mksquashfs_cmd}",
                )

                self.log.info(
                    "Moving temporary image %s to final location %s on Puhti",
                    tmp_image_path,
                    self.image_path,
                )
                sftp_client.posix_rename(str(tmp_image_path), str(self.image_path))

                self.log.info(
                    "Removing the source .tar tree %s",
                    self.data_source,
                )
                self.ssh_execute_and_raise(ssh_client, f"rm -r {self.data_source}")


class ImageCreationError(Exception):
    """
    Error raised when an error occurs during the disk image creation/overwrite process
    """
