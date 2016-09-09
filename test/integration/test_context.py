"""Temporary integration tests"""

from clarity_ext.context import ExtensionContext
import unittest
import logging
import random

class TestResultFile(unittest.TestCase):
    """Tests for interacting with result files through the context"""

    def test_can_update_udfs(self):
        def fetch():
            context = ExtensionContext.create("24-7880")
            result_file = context.output_result_file_by_id("92-20742")
            return context, result_file

        udf_name = "TS Length (bp)"
        logging.basicConfig(level=logging.DEBUG)
        context, result_file = fetch()
        self.assertIsNotNone(result_file)
        original_value = result_file.get_udf(udf_name)
        new_value = random.randint(1, 100)
        if original_value == new_value:
            new_value += 1
        result_file.set_udf(udf_name, new_value)
        context.update(result_file)
        context.commit()

        # Create a new context (NOTE: doing this to ensure we don't get a cached value)
        context, result_file = fetch()
        self.assertEqual(result_file.get_udf(udf_name), new_value)

