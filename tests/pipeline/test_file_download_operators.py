"""
Tests for file downloading operators.

The shared page file downloading functionality is mainly covered by testing
SaveAltosSFTPOperator. SaveAccessImagesSFTPOperator only has the tests necessary to
demonstrate that it is likely to work as well as ALTO downloading and minimal testing
for the functionality specific to it.
"""
import os
from pathlib import Path

import pytest
from requests.exceptions import RequestException

from harvester import utils
from harvester.mets import METSFileEmptyError
from harvester.pmh_interface import PMH_API
from pipeline.plugins.operators.file_download_operators import (
    SaveMetsSFTPOperator,
    SaveAltosSFTPOperator,
    SaveAccessImagesSFTPOperator,
)


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
    mets_dir = Path(sftp_server.root) / "tmp" / "batch_4" / "mets"

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
            dc_identifier=mets_dc_identifier,
            output_directory=mets_dir,
            ignore_files_set={"/mets/379973_METS.xml"},
        )

        sftp_mets_operator.execute(context={})

        # Ensure that the METS file was not overwritten:
        with sftp.file(str(mets_dir / "379973_METS.xml"), "r") as sftp_file:
            assert sftp_file.read().decode("utf-8") == "test"

        # Check that the METS file was not downloaded to another location within
        # output_path either
        files_in_output_path = sum(len(files) for _, _, files in os.walk(mets_dir))
        assert files_in_output_path == 1


def test_existing_tmp_file_does_not_prevent_download(
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
    mets_dir = Path(sftp_server.root) / "tmp" / "mets"

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
            dc_identifier=mets_dc_identifier,
            output_directory=mets_dir,
        )

        tmpfile_path = sftp_mets_operator.tmp_path(sftp_mets_operator.output_file)
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
    mets_dir = Path(sftp_server.root) / "tmp" / "sub" / "path"

    with ssh_server.client("user") as ssh_client:
        sftp = ssh_client.open_sftp()

        sftp_mets_operator = SaveMetsSFTPOperator(
            task_id="test_save_mets_remote",
            api=api,
            sftp_client=sftp,
            ssh_client=ssh_client,
            dc_identifier=mets_dc_identifier,
            output_directory=mets_dir,
        )

        sftp_mets_operator.execute(context={})

        with sftp.file(str(mets_dir / "379973_METS.xml"), "r") as file:
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
    mets_dir = Path(sftp_server.root) / "tmp" / "sub" / "path"

    with ssh_server.client("user") as ssh_client:
        sftp = ssh_client.open_sftp()

        sftp_mets_operator = SaveMetsSFTPOperator(
            task_id="test_save_mets_remote",
            api=api,
            sftp_client=sftp,
            ssh_client=ssh_client,
            dc_identifier=empty_mets_dc_identifier,
            output_directory=mets_dir,
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
    mets_dir = Path(sftp_server.root) / "tmp" / "sub" / "path"

    with ssh_server.client("user") as ssh_client:
        sftp = ssh_client.open_sftp()

        sftp_mets_operator = SaveMetsSFTPOperator(
            task_id="test_save_mets_remote",
            api=api,
            sftp_client=sftp,
            ssh_client=ssh_client,
            dc_identifier=failed_mets_dc_identifier,
            output_directory=mets_dir,
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
    tmp_path = Path(sftp_server.root) / "tmp"
    alto_dir = tmp_path / "sub_dir" / "alto"
    mets_file = tmp_path / "mets_dir" / "379973_METS.xml"

    with ssh_server.client("user") as ssh_client:
        sftp = ssh_client.open_sftp()

        utils.make_intermediate_dirs(
            sftp_client=sftp,
            remote_directory=mets_file.parent,
        )

        with sftp.file(str(mets_file), "w") as sftp_file:
            with open(simple_mets_path, encoding="utf-8") as local_file:
                sftp_file.write(local_file.read())

        operator = SaveAltosSFTPOperator(
            task_id="test_save_altos_remote",
            sftp_client=sftp,
            ssh_client=ssh_client,
            mets_path=mets_file,
            dc_identifier=mets_dc_identifier,
            output_directory=alto_dir,
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
    alto_dir = output_path / "batch_4" / "alto"
    alto_filenames = ["00001.xml", "00002.xml", "00003.xml", "00004.xml"]
    mets_file = output_path / "batch_4" / "mets_dir" / "379973_METS.xml"

    with ssh_server.client("user") as ssh_client:
        sftp = ssh_client.open_sftp()

        utils.make_intermediate_dirs(
            sftp_client=sftp,
            remote_directory=mets_file.parent,
        )

        with sftp.file(str(mets_file), "w") as sftp_file:
            with open(simple_mets_path, encoding="utf-8") as local_file:
                sftp_file.write(local_file.read())

        alto_locations = [alto_dir / filename for filename in alto_filenames]

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
            mets_path=mets_file,
            dc_identifier=mets_dc_identifier,
            output_directory=alto_dir,
            ignore_files_set={f"/alto/{filename}" for filename in alto_filenames},
        )

        operator.execute(context={})

        # Ensure that the alto files were not overwritten
        for alto_location in alto_locations:
            with sftp.file(str(alto_location), "r") as alto:
                assert alto.read().decode("utf-8") == "test content"


def test_save_access_images_sftp_operator(
    mets_dc_identifier,
    sftp_server,
    ssh_server,
    mock_access_image_download_for_test_mets,
    simple_mets_path,
):
    """
    Check that executing SaveAccessImagesSFTPOperator does download the files.
    """
    tmp_path = Path(sftp_server.root) / "tmp"
    image_dir = tmp_path / "sub_dir" / "access_images"
    mets_file = tmp_path / "mets_dir" / "379973_METS.xml"

    with ssh_server.client("user") as ssh_client:
        sftp = ssh_client.open_sftp()

        utils.make_intermediate_dirs(
            sftp_client=sftp,
            remote_directory=mets_file.parent,
        )

        with sftp.file(str(mets_file), "w") as sftp_file:
            with open(simple_mets_path, encoding="utf-8") as local_file:
                sftp_file.write(local_file.read())

        operator = SaveAccessImagesSFTPOperator(
            task_id="test_save_images_remote",
            sftp_client=sftp,
            ssh_client=ssh_client,
            mets_path=mets_file,
            dc_identifier=mets_dc_identifier,
            output_directory=image_dir,
        )

        operator.execute(context={})

        image_locations = [
            image_dir / filename
            for filename in [
                "00001.jp2",
                "00002.jp2",
                "00003.jp2",
                "00004.jp2",
            ]
        ]

        for location in image_locations:
            with sftp.file(str(location), "r") as image_file:
                assert image_file.read() == mock_access_image_download_for_test_mets


def test_save_altos_operator_file_type_str():
    """
    Check that SaveAltosSFTPOperator can report the type of files it downloads
    """
    operator = SaveAltosSFTPOperator(
        task_id="dummy-test-task",
        sftp_client=None,
        ssh_client=None,
        mets_path=None,
        dc_identifier=None,
        output_directory=None,
    )
    assert operator.file_type == "ALTO"


def test_save_access_images_operator_file_type_str():
    """
    Check that SaveAccessImagesSFTPOperator can report the type of files it downloads
    """
    operator = SaveAccessImagesSFTPOperator(
        task_id="dummy-test-task",
        sftp_client=None,
        ssh_client=None,
        mets_path=None,
        dc_identifier=None,
        output_directory=None,
    )
    assert operator.file_type == "access image"


def test_save_page_files_operator_capitalized_file_type_capitalized_first_letter():
    """
    Check that first (and only first) letter of all-lowercase file type is capitalized
    """
    operator = SaveAccessImagesSFTPOperator(
        task_id="dummy-test-task",
        sftp_client=None,
        ssh_client=None,
        mets_path=None,
        dc_identifier=None,
        output_directory=None,
    )
    assert operator.capitalized_file_type == "Access image"


def test_save_page_files_operator_capitalized_file_type_keeps_uppercase_as_uppercase():
    """
    Check that no letters in all-uppercase file type are converted to lower case
    """
    operator = SaveAltosSFTPOperator(
        task_id="dummy-test-task",
        sftp_client=None,
        ssh_client=None,
        mets_path=None,
        dc_identifier=None,
        output_directory=None,
    )
    assert operator.capitalized_file_type == "ALTO"
