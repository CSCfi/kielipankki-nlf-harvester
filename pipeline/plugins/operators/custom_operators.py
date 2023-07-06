import os

from airflow.models import BaseOperator
from airflow.hooks.base import BaseHook
from airflow.providers.ssh.hooks.ssh import SSHHook
from airflow.models import Connection
from airflow import settings

from harvester.mets import METS, METSFileEmptyError
from harvester.file import ALTOFile
from harvester.pmh_interface import PMH_API
from harvester import utils

from requests.exceptions import RequestException


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
                f"Creating a new {self.conn_type} connection with ID {self.conn_id}"
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
    :param tmpdir: Absolute path for a temporary directory on the remote server
    :param dc_identifier: DC identifier of binding
    :param binding_path: Base path for download location
    :param file_dir: Directory where file will be saved
    """

    def __init__(
        self,
        sftp_client,
        ssh_client,
        tmpdir,
        dc_identifier,
        binding_path,
        file_dir,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.sftp_client = sftp_client
        self.ssh_client = ssh_client
        self.dc_identifier = dc_identifier
        self.binding_path = binding_path
        self.file_dir = file_dir
        self.tmpdir = tmpdir

    def ensure_tmp_output_location(self):
        """
        Make sure that all intermediate directories exist for temporary storage
        """
        utils.make_intermediate_dirs(
            sftp_client=self.sftp_client, remote_directory=self.tmpdir / self.file_dir
        )

    def ensure_final_output_location(self):
        """
        Make sure that all intermediate directories exist for final storage
        """
        utils.make_intermediate_dirs(
            sftp_client=self.sftp_client,
            remote_directory=self.binding_path / self.file_dir,
        )

    def move_file_to_final_location(self, temp_output_file, output_file):
        """
        Move file from temporary to final location.

        :return: Exit status from bash command
        """
        _, stdout, _ = self.ssh_client.exec_command(
            f"mv {temp_output_file} {output_file}"
        )

        return stdout.channel.recv_exit_status()

    def execute(self, context):
        raise NotImplementedError(
            "execute() must be defined separately for each file type."
        )

    def file_exists(self, path):
        """
        Check if a non-empty file already exists in the given path.

        :return: True if a non-empty file exists, otherwise False
        """
        try:
            file_size = self.sftp_client.stat(str(path)).st_size
        except OSError:
            return False
        else:
            if file_size > 0:
                return True
        return False


class SaveMetsSFTPOperator(SaveFilesSFTPOperator):
    """
    Save a METS file remote a filesystem using SSH connection.

    :param api: API from which to download the file
    """

    def __init__(self, api, **kwargs):
        super().__init__(**kwargs)
        self.api = api

    def execute(self, context):
        output_file = str(
            utils.mets_download_location(
                dc_identifier=self.dc_identifier,
                base_path=self.binding_path,
                file_dir=self.file_dir,
            )
        )

        if self.file_exists(output_file):
            return

        self.ensure_tmp_output_location()

        temp_output_file = utils.mets_download_location(
            dc_identifier=self.dc_identifier,
            base_path=self.tmpdir,
            file_dir=self.file_dir,
            filename=f"{utils.binding_id_from_dc(self.dc_identifier)}_METS.xml.temp",
        )

        with self.sftp_client.file(str(temp_output_file), "w") as file:
            try:
                self.api.download_mets(
                    dc_identifier=self.dc_identifier, output_mets_file=file
                )
            except RequestException as e:
                raise RequestException(
                    f"METS download {self.dc_identifier} failed: {e.response}"
                )
            except OSError as e:
                raise OSError(
                    f"Writing METS {self.dc_identifier} to file failed with error number {e.errno}"
                )

        self.ensure_final_output_location()

        if not self.file_exists(temp_output_file):
            raise METSFileEmptyError(f"METS file {self.dc_identifier} is empty.")

        exit_status = self.move_file_to_final_location(temp_output_file, output_file)

        if exit_status != 0:
            raise OSError(
                f"Moving METS file {self.dc_identifier} from temp to destination failed"
            )


class SaveAltosSFTPOperator(SaveFilesSFTPOperator):
    """
    Save ALTO files for one binding on remote filesystem using SSH connection.

    :param mets_path: Path to where the METS file of the binding is stored
    """

    def __init__(self, mets_path, **kwargs):
        super().__init__(**kwargs)
        self.mets_path = mets_path

    def execute(self, context):
        path = os.path.join(
            self.mets_path, f"{utils.binding_id_from_dc(self.dc_identifier)}_METS.xml"
        )

        mets = METS(self.dc_identifier, self.sftp_client.file(path, "r"))
        alto_files = mets.files_of_type(ALTOFile)

        self.ensure_final_output_location()
        self.ensure_tmp_output_location()

        for alto_file in alto_files:
            output_file = str(
                utils.file_download_location(
                    file=alto_file,
                    base_path=self.binding_path,
                    file_dir=self.file_dir,
                )
            )

            if self.file_exists(output_file):
                continue

            temp_output_file = utils.file_download_location(
                file=alto_file, base_path=self.tmpdir, file_dir=self.file_dir
            )

            with self.sftp_client.file(str(temp_output_file), "wb") as file:
                try:
                    alto_file.download(
                        output_file=file,
                        chunk_size=10 * 1024 * 1024,
                    )
                except RequestException as e:
                    self.log.error(
                        f"ALTO download with URL {alto_file.download_url} failed: {e.response}"
                    )
                    continue

            exit_status = self.move_file_to_final_location(
                temp_output_file, output_file
            )

            if exit_status != 0:
                self.log.error(
                    f"Moving ALTO file {alto_file.download_url} from temp to destination failed"
                )


class DownloadBindingBatchOperator(BaseOperator):
    """
    Download a batch of bindings.

    :param batch: a list of DC identifiers
    :param ssh_conn_id: SSH connection id
    :param base_path: Base path for images
    :param image_base_name: Name for disk image
    :param tmpdir: Absolute path for a temporary directory on the remote server
    :param api: OAI-PMH api
    """

    def __init__(
        self,
        batch,
        ssh_conn_id,
        base_path,
        image_base_name,
        tmpdir,
        api,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.batch = batch
        self.ssh_conn_id = ssh_conn_id
        self.base_path = base_path
        self.image_base_name = image_base_name
        self.tmpdir = tmpdir
        self.api = api

    def execute(self, context):
        ssh_hook = SSHHook(ssh_conn_id=self.ssh_conn_id)
        with ssh_hook.get_conn() as ssh_client:
            sftp_client = ssh_client.open_sftp()

            for dc_identifier in self.batch:
                binding_id = utils.binding_id_from_dc(dc_identifier)
                binding_path = os.path.join(
                    self.base_path,
                    self.image_base_name,
                    utils.binding_download_location(binding_id),
                )
                tmp_binding_path = os.path.join(
                    self.tmpdir,
                    utils.binding_download_location(binding_id),
                )

                SaveMetsSFTPOperator(
                    task_id=f"save_mets_{binding_id}",
                    api=self.api,
                    sftp_client=sftp_client,
                    ssh_client=ssh_client,
                    tmpdir=tmp_binding_path,
                    dc_identifier=dc_identifier,
                    binding_path=binding_path,
                    file_dir="mets",
                ).execute(context={})

                SaveAltosSFTPOperator(
                    task_id=f"save_altos_{binding_id}",
                    mets_path=f"{binding_path}/mets",
                    sftp_client=sftp_client,
                    ssh_client=ssh_client,
                    tmpdir=tmp_binding_path,
                    dc_identifier=dc_identifier,
                    binding_path=binding_path,
                    file_dir="alto",
                ).execute(context={})

                ssh_client.exec_command(f"rm -r {tmp_binding_path}")


class PrepareDownloadLocationOperator(BaseOperator):
    """
    Prepare download location for a disk image.

    :param ssh_conn_id: SSH connection id
    :param base_path: Base path for images
    :param image_base_name: Name for disk image
    """

    def __init__(
        self,
        ssh_conn_id,
        base_path,
        image_base_name,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.ssh_conn_id = ssh_conn_id
        self.base_path = base_path
        self.image_base_name = image_base_name

    def ensure_image_location(self, sftp_client):
        """
        Ensure that image location exists.
        """
        utils.make_intermediate_dirs(
            sftp_client=sftp_client,
            remote_directory=self.base_path,
        )

    def extract_image(self, ssh_client, sftp_client, image_dir_path):
        """
        Extract contents of a disk image in given path.
        """
        sftp_client.chdir(self.base_path)
        ssh_client.exec_command(f"unsquashfs -d {image_dir_path} {image_dir_path}.sqfs")

    def create_image_folder(self, sftp_client, image_dir_path):
        """
        Create folder to store image contents in.
        """
        utils.make_intermediate_dirs(
            sftp_client=sftp_client,
            remote_directory=image_dir_path,
        )

    def execute(self, context):
        image_dir_path = os.path.join(self.base_path, self.image_base_name)

        ssh_hook = SSHHook(ssh_conn_id=self.ssh_conn_id)
        with ssh_hook.get_conn() as ssh_client:
            sftp_client = ssh_client.open_sftp()

            self.ensure_image_location(sftp_client)

            if f"{self.image_base_name}.sqfs" in sftp_client.listdir(self.base_path):
                self.extract_image(ssh_client, sftp_client, image_dir_path)

            else:
                self.create_image_folder(sftp_client, image_dir_path)


class CreateImageOperator(BaseOperator):
    """
    Prepare download location for a disk image.

    :param ssh_conn_id: SSH connection id
    :param base_path: Base path for images
    :param image_base_name: Name for disk image
    """

    def __init__(
        self,
        ssh_conn_id,
        base_path,
        image_base_name,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.ssh_conn_id = ssh_conn_id
        self.base_path = base_path
        self.image_base_name = image_base_name

    def execute(self, context):
        self.log.info(f"Creating image {self.image_base_name} on Puhti")
        image_dir_path = os.path.join(self.base_path, self.image_base_name)

        ssh_hook = SSHHook(ssh_conn_id=self.ssh_conn_id)
        with ssh_hook.get_conn() as ssh_client:
            ssh_client.exec_command(f"rm {image_dir_path}.sqfs")
            _, stdout, stderr = ssh_client.exec_command(
                f"mksquashfs {image_dir_path} {image_dir_path}.sqfs"
            )
            if stdout.channel.recv_exit_status() != 0:
                raise Exception(
                    f"Creation of image {image_dir_path}.sqfs failed: {stderr.read().decode('utf-8')}"
                )
            ssh_client.exec_command(f"rm -r {image_dir_path}")
