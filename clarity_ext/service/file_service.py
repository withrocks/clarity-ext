from __future__ import print_function
import re
import os
import sys
import shutil
import logging
from lxml import objectify
from clarity_ext import utils
import requests


class FileService:
    """
    Handles downloading and uploading files from/to the LIMS and keeping local copies of them as
    well as cleaning up after a script as run
    """
    CONTEXT_FILES_ROOT = "./context_files"
    SERVER_FILE_NAME_PATTERN = r"(\d+-\d+)_(.+)"

    FILE_PREFIX_NONE = 0
    FILE_PREFIX_ARTIFACT_ID = 1
    FILE_PREFIX_RUNNING_NUMBER = 2

    def __init__(self, artifact_service, file_repo, should_cache, os_service, uploaded_to_stdout=False,
                 disable_commits=False, session=None):
        """
        :param artifact_service: An artifact service instance.
        :param should_cache: Set to True if files should be cached in .cache, mainly
        for faster integration tests.
        :param uploaded_to_stdout: Set to True to output uploaded files to stdout
        :param disable_commits: Set to True to not upload files when committing. Used for testing.
        """
        self._local_shared_files = []
        self.artifact_service = artifact_service
        self.logger = logging.getLogger(__name__)
        self.should_cache = should_cache
        self.file_repo = file_repo
        self.os_service = os_service

        self.upload_queue_path = os.path.join(self.CONTEXT_FILES_ROOT, "upload_queue")
        self.uploaded_path = os.path.join(self.CONTEXT_FILES_ROOT, "uploaded")
        self.temp_path = os.path.join(self.CONTEXT_FILES_ROOT, "temp")
        self.downloaded_path = os.path.join(self.CONTEXT_FILES_ROOT, "downloaded")
        self.session = session
        self.disable_commits = disable_commits
        self.uploaded_to_stdout = uploaded_to_stdout

        # Remove all files before starting
        if self.os_service.exists(self.CONTEXT_FILES_ROOT):
            self.os_service.rmtree(self.CONTEXT_FILES_ROOT)
        self.os_service.makedirs(self.upload_queue_path)
        self.os_service.makedirs(self.uploaded_path)
        self.os_service.makedirs(self.temp_path)
        self.os_service.makedirs(self.downloaded_path)

    def parse_xml(self, f):
        """
        Parses the file like object as XML and returns an object that provides simple access to
        the leaves, such as `parent.child.grandchild`
        """
        with f:
            tree = objectify.parse(f)
            return tree.getroot()

    def parse_csv(self, f):
        with f:
            return Csv(f)

    def local_shared_file(self, file_name, mode='r', extension="", modify_attached=False):
        """
        Downloads the local shared file and returns an open file-like object.

        If the file already exists, it will not be downloaded again.

        Details:
        The downloaded files will be removed when the context is cleaned up. This ensures
        that the LIMS will not upload them by accident
        """

        # Ensure that the user is only sending in a "name" (alphanumerical or spaces)
        # File paths are not allowed
        if not re.match(r"[\w ]+", file_name):
            raise ValueError(
                "File name can only contain alphanumeric characters, underscores and spaces")

        artifact = self._artifact_by_name(file_name)
        local_file_name = "{}_{}.{}".format(artifact.id, file_name.replace(" ", "_"), extension)
        directory = os.path.join(self.downloaded_path, local_file_name)
        downloaded_path = os.path.abspath(directory)
        cache_directory = os.path.abspath(".cache")
        cache_path = os.path.join(cache_directory, local_file_name)

        if self.should_cache and os.path.exists(cache_path):
            self.logger.info("Fetching cached artifact from '{}'".format(cache_path))
            # TODO: Mockable, file system repo
            shutil.copy(cache_path, ".")
        else:
            if not os.path.exists(downloaded_path):

                if len(artifact.files) == 0:
                    # No file has been uploaded yet
                    if modify_attached:
                        with self.os_service.open_file(downloaded_path, "w+") as fs:
                            pass
                else:
                    file = artifact.api_resource.files[0]  # TODO: Hide this logic
                    self.logger.info("Downloading file {} (artifact={} '{}')"
                                     .format(file.id, artifact.id, artifact.name))
                    self.file_repo.copy_remote_file(file.id, downloaded_path)
                    self.logger.info("Download completed, path='{}'".format(os.path.relpath(downloaded_path)))

                    if self.should_cache:
                        if not os.path.exists(cache_directory):
                            os.mkdir(cache_directory)
                        self.logger.info("Copying artifact to cache directory, {}=>{}".format(
                            downloaded_path, cache_directory))
                        shutil.copy(downloaded_path, cache_directory)

        if modify_attached:
            # Move the file to the upload directory and refer to it by that path afterwards. This way the local shared
            # file can be modified by the caller.
            local_path = self._queue(downloaded_path, artifact, FileService.FILE_PREFIX_NONE)
        else:
            local_path = downloaded_path

        f = self.file_repo.open_local_file(local_path, mode)
        self._local_shared_files.append(f)
        return f

    def _queue(self, downloaded_path, artifact, file_prefix):
        file_name = os.path.basename(downloaded_path)
        if file_prefix == FileService.FILE_PREFIX_ARTIFACT_ID and not file_name.startswith(artifact.id):
            file_name = "{}_{}".format(artifact.id, file_name)
        elif file_prefix == FileService.FILE_PREFIX_NONE:
            if file_name.startswith(artifact.id):
                file_name = file_name.split("_", 1)[1]
        elif file_prefix == FileService.FILE_PREFIX_RUNNING_NUMBER:
            # Prefix with the index of the shared file
            artifact_ids = sorted([tuple(map(int, shared_file.id.split("-")) + [shared_file.id])
                                   for shared_file in self.artifact_service.shared_files()])
            running_numbers = {artifact_id[2]: ix + 1 for ix, artifact_id in enumerate(artifact_ids)}
            file_name = "{}_{}".format(running_numbers[artifact.id], file_name)

        upload_dir = os.path.join(self.upload_queue_path, artifact.id)
        self.os_service.makedirs(upload_dir)
        upload_path = os.path.join(upload_dir, file_name)
        self.os_service.copy_file(downloaded_path, upload_path)
        return upload_path

    def _artifact_by_name(self, file_name):
        shared_files = self.artifact_service.shared_files()
        by_name = [shared_file for shared_file in shared_files
                   if shared_file.name == file_name]
        if len(by_name) != 1:
            files = ", ".join(map(lambda x: x.name, shared_files))
            raise SharedFileNotFound("Expected a shared file called '{}', got {}.\nFile: '{}'\nFiles: {}".format(
                file_name, len(by_name), file_name, files))
        artifact = by_name[0]
        return artifact

    def remove_files(self, file_handle, disabled):
        """Removes all files for the particular file handle.

        Note: The files are not actually removed from the server, only the link to the step.
        """
        artifacts = sorted([shared_file for shared_file in self.artifact_service.shared_files()
                            if shared_file.name == file_handle], key=lambda f: f.id)
        for artifact in artifacts:
            for f in artifact.files:
                if disabled:
                    self.logger.info("Removing (disabled) file: {}".format(f.uri))
                    continue
                # TODO: Add to another service
                r = requests.delete(f.uri, auth=(self.session.api.username, self.session.api.password))
                if r.status_code != 204:
                    raise RemoveFileException("Can't remove file with id {}. Status code was {}".format(
                        f.id, r.status_code))

    def upload_files(self, file_handle, files, stdout_max_lines=50):
        """
        Uploads one or more files to the particular file handle. The file handle must support
        at least the same number of files.

        # TODO: Check what happens if there are already n files uploaded but the user uploads n-1.
                It might lead to inconsistency and there should at least be a warning

        :param file_handle: The name that this should be attached to in Clarity, e.g. "Step Log"
        :param files: A list of tuples, (file_name, str)
        """
        artifacts = sorted([shared_file for shared_file in self.artifact_service.shared_files()
                            if shared_file.name == file_handle], key=lambda f: f.id)
        if len(files) > len(artifacts):
            raise SharedFileNotFound("Trying to upload {} files to '{}', but only {} are supported".format(
                            len(files), file_handle, len(artifacts)))

        for artifact, file_and_name in zip(artifacts, files):
            instance_name, content = file_and_name
            self._upload_single(artifact, file_handle, instance_name, content, FileService.FILE_PREFIX_ARTIFACT_ID)

    def upload(self, file_handle, instance_name, content, file_prefix):
        """
        :param file_handle: The handle of the file in the Clarity UI
        :param instance_name: The name of this particular file
        :param content: The content of the file. Should be a string.
        :param file_prefix: Any of the FILE_PREFIX_* values
        """
        artifacts = sorted([shared_file for shared_file in self.artifact_service.shared_files()
                     if shared_file.name == file_handle], key=lambda x: x.id)
        self._upload_single(artifacts[0], file_handle, instance_name, content, file_prefix)

    def _upload_single(self, artifact, file_handle, instance_name, content, file_prefix):
        """Queues the file for update. Call commit to send to the server."""
        local_path = self.save_locally(content, instance_name)
        self.logger.info("Queuing file '{}' for upload to the server, file handle '{}'".format(local_path, file_handle))
        self._queue(local_path, artifact, file_prefix)

    def close_local_shared_files(self):
        for f in self._local_shared_files:
            f.close()

    def commit(self, disable_commits):
        """Copies files in the upload queue to the server"""
        self.close_local_shared_files()
        for artifact_id in self.os_service.listdir(self.upload_queue_path):
            for file_name in self.os_service.listdir(os.path.join(self.upload_queue_path, artifact_id)):
                if disable_commits:
                    self.logger.info("Uploading (disabled) file: {}".format(os.path.abspath(file_name)))
                else:
                    artifact = utils.single([shared_file for shared_file in self.artifact_service.shared_files()
                                             if shared_file.id == artifact_id])
                    local_file = os.path.join(self.upload_queue_path, artifact_id, file_name)
                    self.logger.info("Uploading file {}".format(local_file))
                    self.session.api.upload_new_file(artifact.api_resource, local_file)

    def _split_file_name(self, name):
        m = re.match(self.SERVER_FILE_NAME_PATTERN, name)
        if not m:
            raise Exception("The file name {} is not of the expected format <artifact id>_<name>".format(name))
        return m.groups()

    def save_locally(self, content, filename):
        """
        Saves a file locally before uploading it to the server. Content should be a string.
        """
        full_path = os.path.join(self.temp_path, filename)
        # The file needs to be opened in binary form to ensure that Windows
        # line endings are used if specified
        with self.os_service.open_file(full_path, 'wb') as f:
            self.logger.debug("Writing output to {}.".format(full_path))
            # Content should be either a string or something else we can
            # iterate over, in which case we need newline
            if isinstance(content, basestring):
                try:
                    f.write(content)
                except UnicodeEncodeError:
                    f.write(content.encode("utf-8"))
            else:
                raise NotImplementedError("Type not supported")
        return full_path


class SharedFileNotFound(Exception):
    pass


class Csv:
    """A simple wrapper for csv files"""
    def __init__(self, file_stream=None, delim=",", file_name=None, newline="\n"):
        self.header = list()
        self.data = list()
        if file_stream:
            if isinstance(file_stream, basestring):
                with open(file_stream, "r") as fs:
                    self._init_from_file_stream(fs, delim)
            else:
                self._init_from_file_stream(file_stream, delim)
        self.file_name = file_name
        self.delim = delim
        self.newline = newline

    def _init_from_file_stream(self, file_stream, delim):
        lines = list()
        for ix, line in enumerate(file_stream):
            values = line.strip().split(delim)
            if ix == 0:
                self.set_header(values)
            else:
                self.append(values)

    def set_header(self, header):
        self.key_to_index = {key: ix for ix, key in enumerate(header)}
        self.header = header

    def append(self, values, tag=None):
        """Appends a data line to the CSV, values is a list"""
        csv_line = CsvLine(values, self, tag)
        self.data.append(csv_line)

    def __iter__(self):
        return iter(self.data)

    def to_string(self, include_header=True):
        ret = []
        if include_header:
            ret.append(self.delim.join(map(str, self.header)))
        for line in self.data:
            ret.append(self.delim.join(map(str, line)))
        return self.newline.join(ret)

    def __repr__(self):
        return "<Csv {}>".format(self.file_name)


class CsvLine:
    """Represents one line in a CSV file, items can be added or removed like this were a dictionary"""
    def __init__(self, line, csv, tag=None):
        self.line = line
        self.csv = csv
        self.tag = tag

    def __getitem__(self, key):
        index = self.csv.key_to_index[key]
        return self.line[index]

    def __setitem__(self, key, value):
        index = self.csv.key_to_index[key]
        self.line[index] = value

    def __iter__(self):
        return iter(self.values)

    @property
    def values(self):
        return self.line

    def __repr__(self):
        return repr(self.values)


class OSService(object):
    """Provides access to OS file methods for testability"""

    def __init__(self):
        pass

    def exists(self, path):
        return os.path.exists(path)

    def makedirs(self, path):
        os.makedirs(path)

    def open_file(self, path, mode):
        return open(path, mode)

    def rmdir(self, path):
        os.rmdir(path)

    def rmtree(self, path):
        shutil.rmtree(path)

    def mkdir(self, path):
        os.mkdir(path)

    def copy_file(self, source, dest):
        shutil.copyfile(source, dest)

    def listdir(self, path):
        return os.listdir(path)

    def attach_file_for_epp(self, local_file, artifact):
        # TODO: Remove epp from the name
        original_name = os.path.basename(local_file)
        new_name = artifact.id + '_' + original_name
        location = os.path.join(os.getcwd(), new_name)
        shutil.copy(local_file, location)
        return location


class RemoveFileException(Exception):
    pass
