import unittest
from test.unit.clarity_ext.helpers import fake_analyte, fake_result_file
from test.unit.clarity_ext.helpers import fake_shared_result_file
from mock import MagicMock
from clarity_ext.unit_conversion import UnitConversion
from clarity_ext.domain import Artifact
from clarity_ext.domain.analyte import Analyte
from clarity_ext.domain.result_file import ResultFile
from clarity_ext.domain.shared_result_file import SharedResultFile
from clarity_ext.repository.step_repository import StepRepository


class TestArtifact(unittest.TestCase):

    def test_two_identical_artifacts_equal(self):
        """A copy of an artifact should be equal to another"""
        artifacts = [Artifact(), Artifact()]
        for artifact in artifacts:
            artifact.generation_type = Artifact.OUTPUT_TYPE_RESULT_FILE
            artifact.is_input = False
            artifact.any_patch = "13370"

        self.assertEqual(artifacts[0], artifacts[1])

    def test_artifact_should_not_equal_non_artifact(self):
        artifact = Artifact()
        self.assertNotEqual(artifact, "string")

    def test_equality_including_mutual_references(self):
        """
        Analyte referring to a well, that is referring back to the analyte
        """
        def two_identical_analytes():
            return [
                fake_analyte("cont-id1", "art-id1", "sample1", "art-name1", "D:5", True,
                              concentration=100, volume=20),
                fake_analyte("cont-id1", "art-id1", "sample1", "art-name1", "D:5", True,
                              concentration=100, volume=20)
            ]
        analytes = two_identical_analytes()
        self.assertEqual(analytes[0], analytes[1])

    def test_inequality_including_mutual_references(self):
        """
        Analyte referring to a well, that is referring back to the analyte
        """
        def two_identical_analytes():
            return [
                fake_analyte("cont-id1", "art-id1", "sample1", "art-name1", "D:5", True,
                              concentration=100, volume=20),
                fake_analyte("cont-id1", "art-id2", "sample1", "art-name2", "D:6", True,
                              concentration=100, volume=20)
            ]
        analytes = two_identical_analytes()
        self.assertNotEqual(analytes[0], analytes[1])

    def test_backward_udf_map_empty_if_no_udf_map(self):
        artifact = Artifact()
        self.assertEqual(artifact.udf_backward_map, dict())

    def test_unit_conversion(self):
        def api_resource():
            api_resource = MagicMock()
            api_resource.udf = dict()
            return api_resource

        artifact = Artifact(api_resource=api_resource())
        artifact.id = 'art1'
        units = UnitConversion()
        artifact.set_udf('test_udf', 1234, units.PICO, units.NANO)
        self.assertEqual(artifact.api_resource.udf['test_udf'], 1.234)

    def test_updated_rest_resource_analyte(self):
        def fake_analyte(artifact_id=None, is_input=None, **kwargs):
            udf_map = {"target_concentration": "Target Concentration"}
            api_resource = MagicMock()
            api_resource.udf = dict()
            analyte = Analyte(api_resource, is_input=is_input, id=artifact_id, sample=None, name=None,
                              well=None, artifact_specific_udf_map=udf_map, **kwargs)
            return analyte

        analyte1 = fake_analyte("art1", is_input=True, target_concentration=10)
        analyte2 = fake_analyte("art2", is_input=False)
        step_repo = StepRepository(session=None, udf_map=None)
        step_repo._add_to_orig_state_cache([(analyte1, analyte2)])
        analyte1.target_concentration = 11
        analyte1.set_udf("Not mapped udf", 2)
        updated_fields = step_repo._retrieve_updated_fields(analyte1)
        updated_rest_resource, log = analyte1.updated_rest_resource(
            analyte1.api_resource, updated_fields)

        expected_log = [("Analyte", "art1", "Target Concentration", "11"),
                        ("Analyte", "art1", "Not mapped udf", "2")]
        expected_log = sorted(expected_log)
        log = sorted(log)

        print("log: {}".format(log))
        print("expected log: {}".format(expected_log))

        self.assertEqual(expected_log, log)

    def test_create_from_rest_analyte(self):
        api_resource = MagicMock()
        api_resource.udf = {"conc from udf": 10}
        api_resource.location = [None, "B:2"]
        sample = MagicMock()
        sample.id = "sample1"
        api_resource.samples = [sample]
        api_resource.id = "art1"
        api_resource.name = "sample1"
        udf_map = {
            "Analyte": {"concentration": "conc from udf"}
        }
        container_repo = MagicMock()
        container_repo.get_container.return_value = None

        analyte = Analyte.create_from_rest_resource(
            api_resource, is_input=True, udf_map=udf_map,
            container_repo=container_repo)

        expected_analyte = fake_analyte(container_id=None, artifact_id="art1", sample_id="sample1",
                                        analyte_name="sample1", well_key="B:2", is_input=True,
                                        concentration=10)

        print("analyte:")
        for key in analyte.__dict__:
            print("{}\t{}".format(key, analyte.__dict__[key]))
        print("\nexpected analyte:")
        for key in expected_analyte.__dict__:
            print("{}\t{}".format(key, expected_analyte.__dict__[key]))

        self.assertEqual(expected_analyte.concentration, analyte.concentration)
        self.assertEqual(expected_analyte.id, analyte.id)
        self.assertEqual(expected_analyte.name, analyte.name)
        self.assertEqual(expected_analyte.well.__repr__(),
                         analyte.well.__repr__())

    def test_updated_rest_resource_result_file(self):
        def fake_result_file(artifact_id=None, future_field=None):
            udf_map = {"future_field": "Future Field"}
            api_resource = MagicMock()
            api_resource.udf = dict()
            result_file = ResultFile(api_resource, is_input=False, id=artifact_id, sample=None,
                                     name=None, well=None, artifact_specific_udf_map=udf_map)
            result_file.future_field = future_field
            return result_file

        result_file1 = fake_result_file(artifact_id="art1", future_field=10)
        analyte2 = fake_result_file("art2")
        step_repo = StepRepository(session=None, udf_map=None)
        step_repo._add_to_orig_state_cache([(result_file1, analyte2)])
        result_file1.future_field = 11
        result_file1.set_udf("Not mapped udf", 2)
        updated_fields = step_repo._retrieve_updated_fields(result_file1)
        updated_rest_resource, log = result_file1.updated_rest_resource(
            result_file1.api_resource, updated_fields)

        expected_log = [("ResultFile", "art1", "Future Field", "11"),
                        ("ResultFile", "art1", "Not mapped udf", "2")]
        expected_log = sorted(expected_log)
        log = sorted(log)

        print("log: {}".format(log))
        print("expected log: {}".format(expected_log))

        self.assertEqual(expected_log, log)

    def test_create_from_rest_result_file(self):
        api_resource = MagicMock()
        api_resource.udf = {"conc from udf": 10}
        api_resource.location = [None, "B:2"]
        sample = MagicMock()
        sample.id = "sample1"
        api_resource.samples = [sample]
        api_resource.id = "art1"
        api_resource.name = "sample1"
        udf_map = {
            "ResultFile": {"concentration": "conc from udf"}
        }
        container_repo = MagicMock()
        container_repo.get_container.return_value = None

        result_file = ResultFile.create_from_rest_resource(
            api_resource, is_input=True, udf_map=udf_map,
            container_repo=container_repo)

        expected_result_file = fake_result_file(
            artifact_id="art1", container_id=None, name="sample1", well_key="B:2",
            is_input=False, udf_map=udf_map, concentration=10)

        print("result_file:")
        for key in result_file.__dict__:
            print("{}\t{}".format(key, result_file.__dict__[key]))
        print("\nexpected result_file:")
        for key in expected_result_file.__dict__:
            print("{}\t{}".format(key, expected_result_file.__dict__[key]))

        self.assertEqual(expected_result_file.concentration,
                         result_file.concentration)
        self.assertEqual(expected_result_file.id, result_file.id)
        self.assertEqual(expected_result_file.name, result_file.name)
        self.assertEqual(expected_result_file.well.__repr__(),
                         result_file.well.__repr__())

    def test_create_from_rest_shared_result_file(self):
        api_resource = MagicMock()
        api_resource.id = "id1"
        api_resource.name = "name1"

        shared_result_file = SharedResultFile.create_from_rest_resource(
            api_resource=api_resource, udf_map=dict())
        expected_shared_result_file = fake_shared_result_file("id1", "name1")
        self.assertEqual(expected_shared_result_file.id, shared_result_file.id)
        self.assertEqual(expected_shared_result_file.name, shared_result_file.name)



