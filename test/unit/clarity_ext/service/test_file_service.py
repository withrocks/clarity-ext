import unittest
from mock import MagicMock
from test.unit.clarity_ext import helpers


class TestGeneralFileService(unittest.TestCase):
    pass


def fake_context(shared_files=None, response=None):
    context = MagicMock()
    context.shared_files = shared_files
    context.response = response
    return context


def fake_extension(filename=None):
    extension = MagicMock()
    extension.filename = filename
    extension.newline = '\n'
    return extension
