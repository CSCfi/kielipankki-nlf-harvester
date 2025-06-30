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
    SaveAccessImagesSFTPOperator,
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
    .zip file typically in a slower bigger drive, and delete the downloaded
    files.

    :param batch_with_index: tuple of (list of DC identifiers, batch index)
    :param ssh_conn_id: SSH connection id
    :param tmp_download_directory: Root of the fast temporary directory (containing batch directories)
    :param intermediate_zip_directory: Root of the directory for intermediate zip files
    :param api: OAI-PMH api
    """

    def __init__(
        self,
        batch_with_index,
        ssh_conn_id,
        tmp_download_directory,
        intermediate_zip_directory,
        api,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.batch_with_index = batch_with_index
        self.ssh_conn_id = ssh_conn_id
        self.tmp_download_directory = tmp_download_directory
        self.zip_directory = intermediate_zip_directory
        self.api = api
        self.mark_failed = False

    def create_zip_archive(self, ssh_client, target_file, source_dir):
        """
        Create zip file target_file out of contents of source_dir.

        :return: Exit status from zip
        """
        _, stdout, _ = ssh_client.exec_command(
            f"cd {source_dir}; zip --suffixes .jpg:.jpeg:.jp2 --recurse-paths {target_file} ./"
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
                # zip creation. If we *are* on our third task, create zip
                # anyway, succeed in the task, and log failures.
                self.mark_failed = True
            else:
                self.log.error(
                    f"Downloading {operator.file_type} in {operator.dc_identifier} "
                    f"still failing, moving on with target creation"
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

                access_image_operator = SaveAccessImagesSFTPOperator(
                    task_id=f"save_acces_images_{binding_id}",
                    mets_path=mets_operator.output_file,
                    sftp_client=sftp_client,
                    ssh_client=ssh_client,
                    dc_identifier=dc_identifier,
                    output_directory=tmp_binding_path / "access_img",
                    ignore_files_set={},
                )
                self.execute_save_files_operator(access_image_operator, context)

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
                self.create_zip_archive(
                    ssh_client, f"{self.zip_directory}/{batch_num}.zip", f"{batch_root}"
                )
                != 0
            ):
                self.log.error(
                    f"Failed to create zip file for batch {batch_num} from tmp to destination failed"
                )

            if self.rmtree(ssh_client, f"{batch_root}") != 0:
                self.log.error(f"Failed to clean up downloads for {batch_num}")


class PrepareDownloadLocationOperator(BaseOperator):
    """
    Prepare download location for data that goes into one target.

    This consists of:
    - creating the destination directories if they do not exist
    - copying the old target to the target directory if we are updating

    :param ssh_conn_id: SSH connection id
    :param old_target_path: Path of the corresponding target created
                            during the previous download, if updating.
    :type old_target_path: :class:`pathlib.Path`
    :param new_target_path: Path for new target, if updating
    :type new_target_path: :class:`pathlib.Path`
    :param ensure_dirs: Directories needed for the download.
    :type ensure_dirs: :type: iterable of :class:`pathlib.Path`

    """

    def __init__(
        self,
        ssh_conn_id,
        old_target_path,
        new_target_path,
        ensure_dirs,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.ssh_conn_id = ssh_conn_id
        self.old_target_path = old_target_path
        self.new_target_path = new_target_path
        self.ensure_dirs = ensure_dirs

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
                self.create_directory(sftp_client, dirpath)
            if self.old_target_path and self.new_target_path:
                assert not utils.remote_file_exists(sftp_client, self.new_target_path)
                target_copy_command = (
                    f"cp {self.old_target_path} {self.new_target_path}"
                )
                utils.ssh_execute(ssh_client, target_copy_command)


class PuhtiSshOperator(BaseOperator):
    """
    Base class for operators that need to use Puhti like users do: load modules etc.
    """

    def __init__(self, ssh_conn_id, **kwargs):
        super().__init__(**kwargs)
        self.ssh_conn_id = ssh_conn_id

    def execute(self, context):
        raise NotImplementedError("Must be implemented in subclasses")

    def ssh_execute_and_raise(self, ssh_client, command):
        """
        Run the given command and raise ShellCommandError on non-zero return value.
        """
        _, stdout, stderr = ssh_client.exec_command(command)

        self.log.debug(stdout)

        exit_code = stdout.channel.recv_exit_status()
        if exit_code != 0:
            raise ShellCommandError(
                exit_code=exit_code,
                command=command,
                stdout=stdout.read().decode("utf-8"),
                stderr=stderr.read().decode("utf-8"),
            )

    def run_in_login_shell(self, ssh_client, payload_command, modules=""):
        """
        Run a command on Puhti login shell as a human user would.

        :param payload_command: Command to be run
        :type payload_command: str
        :param modules: Modules to be loaded, separated by space, e.g. "libzip allas"
        :type modules: str

        """
        if modules:
            modules_command = f"module load {modules} && "
        else:
            modules_command = ""

        full_command = (
            "/bin/bash -lc "
            '"'
            ". /appl/profile/zz-csc-env.sh && "
            f"{modules_command}"
            f"{payload_command}"
            '"'
        )

        self.ssh_execute_and_raise(ssh_client, full_command)


class CreateTargetOperator(PuhtiSshOperator):
    """
    Merge intermediate zips into a single distribution target.

    The target file is first created/updated in target_path. If everything
    is successful, it can be processed further and finally moved to the final location
    in the publish_to_users task.

    :param ssh_conn_id: SSH connection id
    :param data_source: Path to the directory that contains the intermediate .zip files
    :type data_source: :class:`pathlib.Path`
    :param target_path: Path to which the newly-created target is written.
    :type target_path: :class:`pathlib.Path`
    """

    def __init__(
        self,
        ssh_conn_id,
        data_source,
        target_path,
        **kwargs,
    ):
        super().__init__(ssh_conn_id=ssh_conn_id, **kwargs)
        self.data_source = data_source
        self.target_path = target_path

    def execute(self, context):
        ssh_hook = SSHHook(ssh_conn_id=self.ssh_conn_id)
        with ssh_hook.get_conn() as ssh_client:
            self.log.info(
                "Merging intermediate zips into %s on Puhti", self.target_path
            )

            # zipmerge -k will skip compression for files that were not compressed in the source zips
            self.run_in_login_shell(
                ssh_client,
                f'zipmerge -k {self.target_path} {self.data_source/"*"}',
                modules="libzip",
            )

            self.log.info(
                "Removing the intermediate .zip directory %s",
                self.data_source,
            )
            self.ssh_execute_and_raise(ssh_client, f"rm -r {self.data_source}")


class RemoveDeletedBindingsOperator(PuhtiSshOperator):
    """
    Operatof for deleting files from our data set when they have been deleted at NLF.

    This does not affect old versions of the data set, but the deleted files will not be
    included in new zips any more.

    It is not considered an error if some or all of the to-be-deleted bindings are not
    found in the zip. This can happen e.g. when a binding is added and then removed
    between our consecutive updates.
    """

    def __init__(
        self,
        ssh_conn_id,
        zip_path,
        deleted_bindings_list,
        **kwargs,
    ):
        super().__init__(ssh_conn_id=ssh_conn_id, **kwargs)
        self.zip_path = zip_path
        self.deleted_binding_identifiers = [
            utils.binding_id_from_dc(dc_identifier)
            for dc_identifier in deleted_bindings_list
        ]

    def execute(self, context):
        if not self.deleted_binding_identifiers:
            self.log.info(f"No bindings to delete from {self.zip_path}")
            return

        ssh_hook = SSHHook(ssh_conn_id=self.ssh_conn_id)

        self.log.info(f"::group::Removing the following bindings from {self.zip_path}")
        for deleted_binding in self.deleted_binding_identifiers:
            self.log.info(deleted_binding)
        self.log.info("::endgroup::")

        deleted_binding_globs = [
            utils.binding_download_location(binding_id) + "/*"
            for binding_id in self.deleted_binding_identifiers
        ]
        self.log.info("::group::These correspond to the following subdirectories:")
        for deleted_binding_directory in deleted_binding_globs:
            self.log.info(deleted_binding_directory)
        self.log.info("::endgroup::")

        with ssh_hook.get_conn() as ssh_client:
            try:
                self.run_in_login_shell(
                    ssh_client,
                    f'zip -d {self.zip_path} {" ".join(deleted_binding_globs)}',
                    modules="libzip",
                )
            except ShellCommandError as e:
                if e.exit_code == 12:
                    self.log.info(
                        f"None of the listed bindings were found in the zip: {e}"
                    )

        self.log.info("Deletion complete")


class ShellCommandError(Exception):
    """
    Error raised when running a command via SSH fails
    """

    def __init__(self, exit_code, command, stdout, stderr):
        self.exit_code = exit_code
        self.command = command
        self.stdout = stdout
        self.stderr = stderr

        message = (
            f"Command {self.command} failed (exit code {self.exit_code}).\n"
            f"Stdout:\n{self.stdout}\n"
            f"Stderr:\n{self.stderr}"
        )

        super().__init__(message)
