import os

from airflow.models import BaseOperator
from airflow.hooks.base import BaseHook
from airflow.models import Connection
from airflow import settings

from harvester.mets import METS
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


class SaveMetsOperator(BaseOperator):
    """
    Save METS file for one binding on local filesystem.

    :param http_conn_id: Connection ID of API
    :param dc_identifier: DC identifier of binding
    :param base_path: Base path for download location
    """

    def __init__(self, http_conn_id, dc_identifier, base_path, **kwargs):
        super().__init__(**kwargs)
        self.http_conn_id = http_conn_id
        self.dc_identifier = dc_identifier
        self.base_path = base_path

    def execute(self, context):
        http_conn = BaseHook.get_connection(self.http_conn_id)
        api = PMH_API(url=http_conn.host)
        output_file = str(
            utils.construct_mets_download_location(
                dc_identifier=self.dc_identifier,
                base_path=self.base_path,
                file_dir="mets",
            )
        )
        mets_path = self.base_path / "mets"
        mets_path.mkdir(parents=True, exist_ok=True)
        with open(output_file, "wb") as file:
            api.download_mets(dc_identifier=self.dc_identifier, output_mets_file=file)


class SaveAltosOperator(BaseOperator):
    """
    Save ALTO files for one binding on local filesystem.

    :param dc_identifier: DC identifier of binding
    :param base_path: Base path for download location
    """

    def __init__(self, dc_identifier, base_path, mets_path, **kwargs):
        super().__init__(**kwargs)
        self.dc_identifier = dc_identifier
        self.base_path = base_path
        self.mets_path = mets_path

    def execute(self, context):
        path = os.path.join(
            self.mets_path, f"{utils.binding_id_from_dc(self.dc_identifier)}_METS.xml"
        )
        mets = METS(self.dc_identifier, open(path, "rb"))
        alto_files = mets.files_of_type(ALTOFile)
        for alto_file in alto_files:
            output_file = utils.construct_file_download_location(
                file=alto_file, base_path=self.base_path
            )
            output_file.parent.mkdir(parents=True, exist_ok=True)
            with open(output_file, "wb") as file:
                alto_file.download(output_file=file)


class SaveMetsSFTPOperator(BaseOperator):
    """
    Save METS file for one binding on remote filesystem using SSH connection.

    :param http_conn_id: Connection ID of API
    :param sftp_client: SFTPClient
    :param dc_identifier: DC identifier of binding
    :param base_path: Base path for download location
    """

    def __init__(
        self,
        api,
        sftp_client,
        ssh_client,
        tmpdir,
        dc_identifier,
        base_path,
        file_dir,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.api = api
        self.sftp_client = sftp_client
        self.ssh_client = ssh_client
        self.dc_identifier = dc_identifier
        self.base_path = base_path
        self.file_dir = file_dir
        self.tmpdir = tmpdir

    def execute(self, context):
        utils.make_intermediate_dirs(
            sftp_client=self.sftp_client,
            remote_directory=f"{self.tmpdir}/{self.file_dir}",
        )

        temp_output_file = str(
            utils.construct_mets_download_location(
                dc_identifier=self.dc_identifier,
                base_path=self.tmpdir,
                file_dir=self.file_dir,
                filename=f"{utils.binding_id_from_dc(self.dc_identifier)}_METS.xml.temp",
            )
        )

        with self.sftp_client.file(temp_output_file, "w") as file:
            try:
                self.api.download_mets(
                    dc_identifier=self.dc_identifier, output_mets_file=file
                )
            except RequestException as e:
                raise RequestException(
                    f"METS download {self.dc_identifier} failed: {e.response}"
                )
            else:
                output_file = str(
                    utils.construct_mets_download_location(
                        dc_identifier=self.dc_identifier,
                        base_path=self.base_path,
                        file_dir=self.file_dir,
                    )
                )
                utils.make_intermediate_dirs(
                    sftp_client=self.sftp_client,
                    remote_directory=f"{self.base_path}/{self.file_dir}",
                )

                self.ssh_client.exec_command(f"mv {temp_output_file} {output_file}")


class SaveAltosSFTPOperator(BaseOperator):
    """
    Save ALTO files for one binding on remote filesystem using SSH connection.

    :param http_conn_id: Connection ID of API
    :param ssh_conn_id: SSH connection ID
    :param base_path: Base path for download location
    :param dc_identifier: DC identifier of binding
    """

    def __init__(
        self,
        sftp_client,
        ssh_client,
        tmpdir,
        base_path,
        file_dir,
        mets_path,
        dc_identifier,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.sftp_client = sftp_client
        self.ssh_client = ssh_client
        self.tmpdir = tmpdir
        self.base_path = base_path
        self.file_dir = file_dir
        self.mets_path = mets_path
        self.dc_identifier = dc_identifier

    def execute(self, context):
        path = os.path.join(
            self.mets_path, f"{utils.binding_id_from_dc(self.dc_identifier)}_METS.xml"
        )

        mets = METS(self.dc_identifier, self.sftp_client.file(path, "r"))
        alto_files = mets.files_of_type(ALTOFile)

        utils.make_intermediate_dirs(
            sftp_client=self.sftp_client,
            remote_directory=f"{self.base_path}/{self.file_dir}",
        )

        utils.make_intermediate_dirs(
            sftp_client=self.sftp_client,
            remote_directory=f"{self.tmpdir}/{self.file_dir}",
        )

        for alto_file in alto_files:
            temp_output_file = str(
                utils.construct_file_download_location(
                    file=alto_file, base_path=self.tmpdir, file_dir=self.file_dir
                )
            )

            with self.sftp_client.file(temp_output_file, "wb") as file:
                try:
                    alto_file.download(
                        output_file=file,
                        chunk_size=10 * 1024 * 1024,
                    )
                except RequestException as e:
                    raise RequestException(
                        f"ALTO download with URL {alto_file.download_url} failed: {e.response}"
                    )
                else:
                    output_file = str(
                        utils.construct_file_download_location(
                            file=alto_file,
                            base_path=self.base_path,
                            file_dir=self.file_dir,
                        )
                    )
                    self.ssh_client.exec_command(f"mv {temp_output_file} {output_file}")
