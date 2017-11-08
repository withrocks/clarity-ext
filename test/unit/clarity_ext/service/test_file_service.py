import unittest
from mock import MagicMock
from clarity_ext.domain.artifact import Artifact
from clarity_ext.service.file_service import FileService


class TestUploadFileService(unittest.TestCase):

    def test_run_driver_file_service(self):
        artifact_service = MagicMock()
        shared_files = [fake_artifact("art1", "Handle Name 1"), fake_artifact("art2", "Handle Name 2")]
        artifact_service.shared_files = MagicMock(return_value=shared_files)
        os_service = MagicMock()
        session = MagicMock()
        file_service = FileService(artifact_service, MagicMock(), False, os_service,
                                   uploaded_to_stdout=False,
                                   disable_commits=False,
                                   session=session)
        file_service.upload("Handle Name 2", "file2.txt", "content")
        # Assert that the file was copied to the upload path
        os_service.copy_file.assert_called_with(
            "./context_files/temp/file2.txt", "./context_files/upload_queue/art2_file2.txt")


def fake_artifact(artifact_id, name):
    artifact = Artifact()
    artifact.name = name
    artifact.id = artifact_id
    return artifact
