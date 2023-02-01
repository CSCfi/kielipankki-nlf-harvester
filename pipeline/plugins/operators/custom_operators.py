import os
from more_itertools import peekable

from airflow.models import BaseOperator
from airflow.hooks.base import BaseHook
from airflow.providers.ssh.hooks.ssh import SSHHook
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

    def __init__(self, api, sftp_client, dc_identifier, base_path, file_dir, **kwargs):
        super().__init__(**kwargs)
        self.api = api
        self.sftp_client = sftp_client
        self.dc_identifier = dc_identifier
        self.base_path = base_path
        self.file_dir = file_dir

    def execute(self, context):
        output_file = str(
            utils.construct_mets_download_location(
                dc_identifier=self.dc_identifier,
                base_path=self.base_path,
                file_dir=self.file_dir,
            )
        )

        with self.sftp_client.file(output_file, "w") as file:
            try:
                self.api.download_mets(
                    dc_identifier=self.dc_identifier, output_mets_file=file
                )
            except RequestException as e:
                print(
                    f"Download of METS file {self.dc_identifier} failed with code {e.response.status_code}"
                )
                # Delete empty file and binding folders if download fails
                self.sftp_client.remove(output_file)
                self.sftp_client.rmdir("/".join(output_file.split("/")[:-1]))
                self.sftp_client.rmdir("/".join(output_file.split("/")[:-2]))


class SaveMetsForSetSFTPOperator(BaseOperator):
    """
    Save all METS files for one set on remote filesystem using SSH connection.

    :param http_conn_id: Connection ID of API
    :param ssh_conn_id: SSH connection ID
    :param base_path: Base path for download location
    :param set_id: Set ID
    """

    def __init__(self, http_conn_id, ssh_conn_id, base_path, set_id, **kwargs):
        super().__init__(**kwargs)
        self.http_conn_id = http_conn_id
        self.ssh_conn_id = ssh_conn_id
        self.base_path = base_path
        self.set_id = set_id

    def execute(self, context):
        http_conn = BaseHook.get_connection(self.http_conn_id)
        api = PMH_API(url=http_conn.host)

        ssh_hook = SSHHook(ssh_conn_id=self.ssh_conn_id)
        with ssh_hook.get_conn() as ssh_client:
            sftp_client = ssh_client.open_sftp()

            for dc_identifier in api.dc_identifiers(set_id=self.set_id):
                utils.make_intermediate_dirs(
                    sftp_client=sftp_client,
                    remote_directory=f"{self.base_path}/{self.set_id.replace(':', '_')}/{utils.binding_id_from_dc(dc_identifier)}/mets",
                )
                SaveMetsSFTPOperator(
                    task_id=f"save_mets_{utils.binding_id_from_dc(dc_identifier)}",
                    api=api,
                    sftp_client=sftp_client,
                    dc_identifier=dc_identifier,
                    base_path=f"{self.base_path}",
                    file_dir=f"{self.set_id.replace(':', '_')}/{utils.binding_id_from_dc(dc_identifier)}/mets",
                ).execute(context={})


class SaveMetsForAllSetsSFTPOperator(BaseOperator):
    """
    Save all METS files for all sets on remote filesystem using SSH connection.

    :param http_conn_id: Connection ID of API
    :param ssh_conn_id: SSH connection ID
    :param base_path: Base path for download location
    """

    def __init__(self, http_conn_id, ssh_conn_id, base_path, **kwargs):
        super().__init__(**kwargs)
        self.http_conn_id = http_conn_id
        self.ssh_conn_id = ssh_conn_id
        self.base_path = base_path

    def execute(self, context):
        http_conn = BaseHook.get_connection(self.http_conn_id)
        api = PMH_API(url=http_conn.host)

        ssh_hook = SSHHook(ssh_conn_id=self.ssh_conn_id)
        with ssh_hook.get_conn() as ssh_client:
            sftp_client = ssh_client.open_sftp()
            utils.make_intermediate_dirs(
                sftp_client=sftp_client,
                remote_directory=f"{self.base_path}/mets",
            )
            for set_id in [
                s
                for s in list(api.set_ids())
                if not s.startswith(("col-501", "col-82", "col-25", "col-101"))
                and not s.endswith("rajatut")
            ]:
                print(f"Downloading METS for set {set_id}")
                SaveMetsForSetSFTPOperator(
                    task_id=f"save_mets_for_{set_id.replace(':', '_')}",
                    http_conn_id=self.http_conn_id,
                    ssh_conn_id=self.ssh_conn_id,
                    base_path=self.base_path,
                    set_id=set_id,
                ).execute(context={})


class SaveAltosForMetsSFTPOperator(BaseOperator):
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
        base_path,
        file_dir,
        mets_path,
        dc_identifier,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.sftp_client = sftp_client
        self.base_path = base_path
        self.file_dir = file_dir
        self.mets_path = mets_path
        self.dc_identifier = dc_identifier

    def execute(self, context):
        path = os.path.join(
            self.mets_path, f"{utils.binding_id_from_dc(self.dc_identifier)}_METS.xml"
        )
        mets = METS(self.dc_identifier, self.sftp_client.file(path, "r"))
        alto_files = peekable(mets.files_of_type(ALTOFile))

        first_alto = alto_files.peek()
        first_alto_path = str(
            utils.construct_file_download_location(
                file=first_alto, base_path=self.base_path, file_dir=self.file_dir
            )
        )
        utils.make_intermediate_dirs(
            sftp_client=self.sftp_client,
            remote_directory=first_alto_path.rsplit("/", maxsplit=1)[0],
        )
        for alto_file in alto_files:
            output_file = str(
                utils.construct_file_download_location(
                    file=alto_file, base_path=self.base_path, file_dir=self.file_dir
                )
            )
            with self.sftp_client.file(output_file, "wb") as file:
                try:
                    alto_file.download(
                        output_file=file,
                        chunk_size=10 * 1024 * 1024,
                    )
                except RequestException as e:
                    print(
                        f"File download failed with URL {alto_file.download_url} with code {e.response.status_code}"
                    )
                    self.sftp_client.remove(output_file)
                    continue


class SaveAltosForSetSFTPOperator(BaseOperator):
    """
    Save all ALTO files for one set on remote filesystem using SSH connection.

    :param http_conn_id: Connection ID of API
    :param ssh_conn_id: SSH connection ID
    :param base_path: Base path for download location
    :param set_id: Set ID
    """

    def __init__(self, http_conn_id, ssh_conn_id, base_path, set_id, **kwargs):
        super().__init__(**kwargs)
        self.http_conn_id = http_conn_id
        self.ssh_conn_id = ssh_conn_id
        self.base_path = base_path
        self.set_id = set_id

    def execute(self, context):
        http_conn = BaseHook.get_connection(self.http_conn_id)
        api = PMH_API(url=http_conn.host)

        ssh_hook = SSHHook(ssh_conn_id=self.ssh_conn_id)
        with ssh_hook.get_conn() as ssh_client:
            sftp_client = ssh_client.open_sftp()

            for dc_identifier in api.dc_identifiers(set_id=self.set_id):
                SaveAltosForMetsSFTPOperator(
                    task_id=f"save_altos_for_{utils.binding_id_from_dc(dc_identifier)}",
                    sftp_client=sftp_client,
                    base_path=self.base_path,
                    file_dir=f"{self.set_id.replace(':', '_')}/{utils.binding_id_from_dc(dc_identifier)}/alto",
                    mets_path=f"{self.base_path}/{self.set_id.replace(':', '_')}/{utils.binding_id_from_dc(dc_identifier)}/mets",
                    dc_identifier=dc_identifier,
                ).execute(context={})


class SaveAltosForAllSetsSFTPOperator(BaseOperator):
    """
    Save all ALTO files for all sets on remote filesystem using SSH connection.

    :param http_conn_id: Connection ID of API
    :param ssh_conn_id: SSH connection ID
    :param base_path: Base path for download location
    """

    def __init__(self, http_conn_id, ssh_conn_id, base_path, **kwargs):
        super().__init__(**kwargs)
        self.http_conn_id = http_conn_id
        self.ssh_conn_id = ssh_conn_id
        self.base_path = base_path

    def execute(self, context):
        http_conn = BaseHook.get_connection(self.http_conn_id)
        api = PMH_API(url=http_conn.host)

        for set_id in [
            s
            for s in list(api.set_ids())
            if not s.startswith(("col-501", "col-82", "col-25", "col-101"))
            and not s.endswith("rajatut")
        ]:
            print(f"Downloading ALTOs for set {set_id}")
            SaveAltosForSetSFTPOperator(
                task_id=f"save_altos_for_{set_id.replace(':', '_')}",
                http_conn_id=self.http_conn_id,
                ssh_conn_id=self.ssh_conn_id,
                base_path=self.base_path,
                set_id=set_id,
            ).execute(context={})
