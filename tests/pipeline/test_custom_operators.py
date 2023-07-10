import pytest
import os
from pathlib import Path
from airflow import settings
from airflow.models import Connection
from requests.exceptions import RequestException

from harvester import utils
from harvester.mets import METSFileEmptyError
from pipeline.plugins.operators.custom_operators import (
    CreateConnectionOperator,
    SaveMetsSFTPOperator,
    SaveAltosSFTPOperator,
)
from harvester.pmh_interface import PMH_API


def test_create_connection_operator():
    """
    Check that CreateConnectionOperator actually creates an Airflow connection
    """
    create_nlf_connection = CreateConnectionOperator(
        task_id="create_nlf_connection",
        conn_id="nlf_http_conn",
        conn_type="HTTP",
        host="https://digi.kansalliskirjasto.fi/interfaces/OAI-PMH",
        schema="HTTPS",
    )

    create_nlf_connection.execute(context={})

    session = settings.Session()
    conn_ids = [conn.conn_id for conn in session.query(Connection).all()]
    assert "nlf_http_conn" in conn_ids


@pytest.mark.usefixtures("expected_mets_response")
def test_existing_mets_not_downloaded_again(
    oai_pmh_api_url,
    mets_dc_identifier,
    sftp_server,
    ssh_server,
):
    """
    Test that existing METS files are not redownloaded.
    """
    api = PMH_API(oai_pmh_api_url)
    temp_path = Path(sftp_server.root) / "tmp"
    mets_dir = temp_path / "mets"

    with ssh_server.client("user") as ssh_client:
        sftp = ssh_client.open_sftp()

        utils.make_intermediate_dirs(
            sftp_client=sftp,
            remote_directory=mets_dir,
        )

        with sftp.file(str(mets_dir / "379973_METS.xml"), "w") as sftp_file:
            sftp_file.write("test")

        sftp_mets_operator = SaveMetsSFTPOperator(
            task_id="test_save_mets_remote",
            api=api,
            sftp_client=sftp,
            ssh_client=ssh_client,
            tmpdir=temp_path,
            dc_identifier=mets_dc_identifier,
            file_dir="mets",
        )

        sftp_mets_operator.execute(context={})

        # Ensure that the METS file was not overwritten:
        with sftp.file(str(mets_dir / "379973_METS.xml"), "r") as sftp_file:
            assert sftp_file.read().decode("utf-8") == "test"

        # Check that the METS file was not downloaded to another location within
        # output_path either
        files_in_output_path = sum(len(files) for _, _, files in os.walk(temp_path))
        assert files_in_output_path == 1


def test_existing_tempfile_does_not_prevent_download(
    oai_pmh_api_url,
    mets_dc_identifier,
    sftp_server,
    ssh_server,
    expected_mets_response,
):
    """
    Ensure that pre-existing temporary file does not prevent proper download.

    This is checked by manually creating the temporary file for the operator before
    executing it, and checking that the proper file has been created afterwards.
    """
    api = PMH_API(oai_pmh_api_url)
    temp_path = Path(sftp_server.root) / "tmp"
    mets_dir = temp_path / "mets"

    with ssh_server.client("user") as ssh_client:
        sftp = ssh_client.open_sftp()

        utils.make_intermediate_dirs(
            sftp_client=sftp,
            remote_directory=mets_dir,
        )

        sftp_mets_operator = SaveMetsSFTPOperator(
            task_id="test_save_mets_remote",
            api=api,
            sftp_client=sftp,
            ssh_client=ssh_client,
            tmpdir=temp_path,
            dc_identifier=mets_dc_identifier,
            file_dir="mets",
        )

        tmpfile_path = sftp_mets_operator.tempfile_path(sftp_mets_operator.output_file)
        with sftp.file(str(tmpfile_path), "w") as pre_existing_tmpfile:
            pre_existing_tmpfile.write("this should not prevent proper download")

        sftp_mets_operator.execute(context={})

        with sftp.file(str(sftp_mets_operator.output_file), "r") as file:
            assert file.read().decode("utf-8") == expected_mets_response


def test_save_mets_sftp_operator(
    oai_pmh_api_url,
    mets_dc_identifier,
    sftp_server,
    ssh_server,
    expected_mets_response,
):
    """
    Check that executing SaveMetsSFTPOperator saves a METS file to the remote server
    """

    api = PMH_API(oai_pmh_api_url)
    temp_path = Path(sftp_server.root) / "tmp" / "sub" / "path"

    with ssh_server.client("user") as ssh_client:
        sftp = ssh_client.open_sftp()

        sftp_mets_operator = SaveMetsSFTPOperator(
            task_id="test_save_mets_remote",
            api=api,
            sftp_client=sftp,
            ssh_client=ssh_client,
            tmpdir=temp_path,
            dc_identifier=mets_dc_identifier,
            file_dir="file_dir",
        )

        sftp_mets_operator.execute(context={})

        with sftp.file(str(temp_path / "file_dir" / "379973_METS.xml"), "r") as file:
            assert file.read().decode("utf-8") == expected_mets_response


@pytest.mark.usefixtures("empty_mets_response")
def test_empty_mets(
    oai_pmh_api_url,
    empty_mets_dc_identifier,
    sftp_server,
    ssh_server,
):
    """
    Ensure that an empty METS file raises an exception.
    """

    api = PMH_API(oai_pmh_api_url)
    temp_path = Path(sftp_server.root) / "tmp" / "sub" / "path"

    with ssh_server.client("user") as ssh_client:
        sftp = ssh_client.open_sftp()

        sftp_mets_operator = SaveMetsSFTPOperator(
            task_id="test_save_mets_remote",
            api=api,
            sftp_client=sftp,
            ssh_client=ssh_client,
            tmpdir=temp_path,
            dc_identifier=empty_mets_dc_identifier,
            file_dir="file_dir",
        )

        with pytest.raises(METSFileEmptyError):
            sftp_mets_operator.execute(context={})


@pytest.mark.usefixtures("failed_mets_response")
def test_failed_mets_request(
    oai_pmh_api_url,
    failed_mets_dc_identifier,
    sftp_server,
    ssh_server,
):
    """
    Ensure that an empty METS file raises an exception.
    """

    api = PMH_API(oai_pmh_api_url)
    temp_path = Path(sftp_server.root) / "tmp" / "sub" / "path"

    with ssh_server.client("user") as ssh_client:
        sftp = ssh_client.open_sftp()

        sftp_mets_operator = SaveMetsSFTPOperator(
            task_id="test_save_mets_remote",
            api=api,
            sftp_client=sftp,
            ssh_client=ssh_client,
            tmpdir=temp_path,
            dc_identifier=failed_mets_dc_identifier,
            file_dir="file_dir",
        )

        with pytest.raises(RequestException):
            sftp_mets_operator.execute(context={})


def test_save_altos_sftp_operator(
    mets_dc_identifier,
    sftp_server,
    ssh_server,
    mock_alto_download_for_test_mets,
    simple_mets_path,
):
    """
    Check that executing SaveAltosForMetsSFTPOperator correctly saves ALTO files
    to a remote server.
    """
    temp_path = Path(sftp_server.root) / "tmp"
    mets_dir = temp_path / "sub_dir" / "mets"
    alto_dir = temp_path / "sub_dir" / "alto"
    mets_file = mets_dir / "379973_METS.xml"

    with ssh_server.client("user") as ssh_client:
        sftp = ssh_client.open_sftp()

        utils.make_intermediate_dirs(
            sftp_client=sftp,
            remote_directory=mets_dir,
        )

        with sftp.file(str(mets_file), "w") as sftp_file:
            with open(simple_mets_path, encoding="utf-8") as local_file:
                sftp_file.write(local_file.read())

        operator = SaveAltosSFTPOperator(
            task_id="test_save_altos_remote",
            sftp_client=sftp,
            ssh_client=ssh_client,
            tmpdir=temp_path,
            file_dir=alto_dir,
            mets_path=mets_dir,
            dc_identifier=mets_dc_identifier,
        )

        operator.execute(context={})

        alto_locations = [
            alto_dir / file
            for file in ["00001.xml", "00002.xml", "00003.xml", "00004.xml"]
        ]
        for alto_location in alto_locations:
            with sftp.file(str(alto_location), "r") as alto:
                assert alto.read().decode("utf-8") == mock_alto_download_for_test_mets


@pytest.mark.usefixtures("mock_alto_download_for_test_mets")
def test_existing_altos_not_downloaded_again(
    mets_dc_identifier,
    sftp_server,
    ssh_server,
    simple_mets_path,
):
    """
    Test that existing ALTO files are not redownloaded.
    """
    output_path = Path(sftp_server.root) / "dir"
    mets_dir = output_path / "sub_dir" / "mets"
    alto_dir = output_path / "sub_dir" / "alto"
    mets_file = mets_dir / "379973_METS.xml"
    temp_path = Path(sftp_server.root) / "tmp"

    with ssh_server.client("user") as ssh_client:
        sftp = ssh_client.open_sftp()

        utils.make_intermediate_dirs(
            sftp_client=sftp,
            remote_directory=mets_dir,
        )

        with sftp.file(str(mets_file), "w") as sftp_file:
            with open(simple_mets_path, encoding="utf-8") as local_file:
                sftp_file.write(local_file.read())

        alto_locations = [
            alto_dir / filename
            for filename in ["00001.xml", "00002.xml", "00003.xml", "00004.xml"]
        ]

        utils.make_intermediate_dirs(
            sftp_client=sftp,
            remote_directory=alto_dir,
        )

        for alto_location in alto_locations:
            with sftp.file(str(alto_location), "w") as alto:
                alto.write("test content")

        operator = SaveAltosSFTPOperator(
            task_id="test_save_altos_remote",
            sftp_client=sftp,
            ssh_client=ssh_client,
            tmpdir=temp_path,
            file_dir=alto_dir,
            mets_path=mets_dir,
            dc_identifier=mets_dc_identifier,
        )

        operator.execute(context={})

        # Ensure that the alto files were not overwritten
        for alto_location in alto_locations:
            with sftp.file(str(alto_location), "r") as alto:
                assert alto.read().decode("utf-8") == "test content"
