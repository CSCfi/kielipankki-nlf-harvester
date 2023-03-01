import pytest
import os
from pathlib import Path
from airflow import settings

from harvester import utils
from harvester.mets import METSFileEmptyError
from pipeline.plugins.operators.custom_operators import *


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


def test_save_mets_operator(mets_dc_identifier, expected_mets_response, tmp_path):
    """
    Check that executing save_mets_operator does indeed fetch a METS file
    """

    create_nlf_connection = CreateConnectionOperator(
        task_id="create_nlf_connection",
        conn_id="nlf_http_conn",
        conn_type="HTTP",
        host="https://digi.kansalliskirjasto.fi/interfaces/OAI-PMH",
        schema="HTTPS",
    )

    create_nlf_connection.execute(context={})

    operator = SaveMetsOperator(
        task_id="test_save_mets_task",
        http_conn_id="nlf_http_conn",
        dc_identifier=mets_dc_identifier,
        base_path=tmp_path,
    )

    operator.execute(context={})

    binding_id = utils.binding_id_from_dc(mets_dc_identifier)
    mets_location = tmp_path / "mets" / f"{binding_id}_METS.xml"

    assert os.path.exists(mets_location)
    with open(mets_location, "r", encoding="utf-8") as saved_mets:
        assert saved_mets.read() == expected_mets_response


def test_save_altos_operator(
    mets_dc_identifier, tmp_path, mock_alto_download_for_test_mets
):
    """
    Check that executing SaveAltosOperator saves ALTO files as expected
    """

    alto_operator = SaveAltosOperator(
        task_id="test_save_altos_locally",
        dc_identifier=mets_dc_identifier,
        base_path=tmp_path,
        mets_path="tests/data",
    )

    alto_operator.execute(context={})

    binding_id = utils.binding_id_from_dc(mets_dc_identifier)
    alto_locations = [
        tmp_path / f"{binding_id}/alto/{file}"
        for file in ["00001.xml", "00002.xml", "00003.xml", "00004.xml"]
    ]
    for alto_location in alto_locations:
        assert os.path.exists(alto_location)
        with open(alto_location, "r", encoding="utf-8") as alto:
            assert alto.read() == mock_alto_download_for_test_mets


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
    output_path = str(Path(sftp_server.root) / "some" / "sub" / "path")
    temp_path = str(Path(sftp_server.root) / "tmp" / "sub" / "path")

    with ssh_server.client("user") as ssh_client:
        sftp = ssh_client.open_sftp()

        utils.make_intermediate_dirs(
            sftp_client=sftp,
            remote_directory=f"{output_path}/file_dir",
        )

        utils.make_intermediate_dirs(
            sftp_client=sftp,
            remote_directory=f"{temp_path}/file_dir",
        )

        sftp_mets_operator = SaveMetsSFTPOperator(
            task_id="test_save_mets_remote",
            api=api,
            sftp_client=sftp,
            ssh_client=ssh_client,
            tmpdir=temp_path,
            dc_identifier=mets_dc_identifier,
            base_path=output_path,
            file_dir="file_dir",
        )

        sftp_mets_operator.execute(context={})

        with sftp.file(f"{output_path}/file_dir/379973_METS.xml", "r") as file:
            assert file.read().decode("utf-8") == expected_mets_response


def test_empty_mets(
    oai_pmh_api_url,
    empty_mets_dc_identifier,
    empty_mets_response,
    sftp_server,
    ssh_server,
):
    """
    Ensure that an empty METS file raises an exception.
    """

    api = PMH_API(oai_pmh_api_url)
    output_path = str(Path(sftp_server.root) / "some" / "sub" / "path")
    temp_path = str(Path(sftp_server.root) / "tmp" / "sub" / "path")

    with ssh_server.client("user") as ssh_client:
        sftp = ssh_client.open_sftp()

        utils.make_intermediate_dirs(
            sftp_client=sftp,
            remote_directory=f"{output_path}/file_dir",
        )

        utils.make_intermediate_dirs(
            sftp_client=sftp,
            remote_directory=f"{temp_path}/file_dir",
        )

        sftp_mets_operator = SaveMetsSFTPOperator(
            task_id="test_save_mets_remote",
            api=api,
            sftp_client=sftp,
            ssh_client=ssh_client,
            tmpdir=temp_path,
            dc_identifier=empty_mets_dc_identifier,
            base_path=output_path,
            file_dir="file_dir",
        )

        mets_response = empty_mets_response
        with pytest.raises(METSFileEmptyError):
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

    output_path = str(Path(sftp_server.root) / "dir")
    temp_path = str(Path(sftp_server.root) / "tmp")

    with ssh_server.client("user") as ssh_client:
        sftp = ssh_client.open_sftp()

        utils.make_intermediate_dirs(
            sftp_client=sftp,
            remote_directory=f"{output_path}/sub_dir/mets",
        )

        with sftp.file(f"{output_path}/sub_dir/mets/379973_METS.xml", "w") as sftp_file:
            with open(simple_mets_path, encoding="utf-8") as local_file:
                sftp_file.write(local_file.read())

        operator = SaveAltosSFTPOperator(
            task_id="test_save_altos_remote",
            sftp_client=sftp,
            ssh_client=ssh_client,
            base_path=output_path,
            tmpdir=temp_path,
            file_dir="sub_dir",
            mets_path=f"{output_path}/sub_dir/mets",
            dc_identifier=mets_dc_identifier,
        )

        operator.execute(context={})

        alto_locations = [
            f"{output_path}/sub_dir/{file}"
            for file in ["00001.xml", "00002.xml", "00003.xml", "00004.xml"]
        ]
        for alto_location in alto_locations:
            with sftp.file(alto_location, "r") as alto:
                assert alto.read().decode("utf-8") == mock_alto_download_for_test_mets
