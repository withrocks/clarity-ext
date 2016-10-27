from clarity_ext.repository.step_repository import StepRepository
import unittest
import test.unit.clarity_ext.helpers
from test.unit.clarity_ext.helpers import fake_analyte
from mock import MagicMock

class TestStepRepository(unittest.TestCase):
    def test_update_nothing(self):
        session = MagicMock()
        session.api = MagicMock()
        udf_map = {
                       "concentration_ngul": "conc",
                       "requested_concentration_ngul": "rconc",
                   }
        step_repo = StepRepository(session=session, udf_map=udf_map)
        artifacts = artifact_set(udf_map=udf_map)
        artifacts = list(artifacts[0])
        step_repo._add_to_orig_state_cache(artifact_set(udf_map=udf_map))
        response = step_repo.update_artifacts(artifacts)
        self.assertEqual([], response)

    def test_update_one_field(self):
        session = MagicMock()
        session.api = MagicMock()
        udf_map = {
                       "concentration_ngul": "conc",
                       "requested_concentration_ngul": "rconc",
                   }
        step_repo = StepRepository(session=session, udf_map=udf_map)
        artifacts = artifact_set(udf_map=udf_map)
        artifacts = list(artifacts[0])
        step_repo._add_to_orig_state_cache(artifact_set(udf_map=udf_map))
        updated_artifact = artifacts[0]
        updated_artifact.concentration_ngul = 99
        response = step_repo.update_artifacts(artifacts)
        self.assertEqual([('Analyte', 'art1', 'conc', '99')], response)


def artifact_set(udf_map=None):
    api_resource = MagicMock()
    api_resource.udf = {"conc": 100,}
    return [
        (fake_analyte("cont1", "art1", "sample1", "sample1", "B:2", True, udf_map=udf_map,
                      api_resource=api_resource, concentration_ngul=100, volume=30),
         fake_analyte("cont2", "art2", "sample1", "sample1", "B:2", False, udf_map=udf_map,
                      api_resource=api_resource, requested_concentration_ngul=100, requested_volume=20)),
    ]
