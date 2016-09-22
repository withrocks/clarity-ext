import unittest
from mock import MagicMock
from clarity_ext.dilution import DilutionScheme, SourceOnlyDilutionScheme
from test.unit.clarity_ext.helpers import fake_analyte, fake_result_file
from test.unit.clarity_ext import helpers
from clarity_ext.service import ArtifactService


class TestDilutionScheme(unittest.TestCase):
    """
    Validates that the DilutionScheme property of the context object returns the expected values
    based on fake data from the LIMS
    """

    def test_dilution_scheme_hamilton_expected_results(self):
        """Dilution scheme created by mocked analytes is correctly generated for Hamilton"""
        # Setup:
        svc = helpers.mock_two_containers_artifact_service()
        dilution_scheme = DilutionScheme(svc, "Hamilton")

        expected = [
            ['art-name1', 36, 'DNA1', 14.9, 5.1, 34, 'END1'],
            ['art-name2', 33, 'DNA2', 14.9, 5.1, 17, 'END2'],
            ['art-name3', 50, 'DNA2', 14.9, 5.1, 44, 'END1'],
            ['art-name4', 93, 'DNA2', 14.9, 5.1, 69, 'END2']]

        # Test:
        actual = [
            [dilute.sample_name,
             dilute.source_well_index,
             dilute.source_plate_pos,
             round(dilute.sample_volume, 1),
             round(dilute.buffer_volume, 1),
             dilute.target_well_index,
             dilute.target_plate_pos] for dilute in dilution_scheme.transfers
        ]

        validation_results = list(dilution_scheme.validate())

        # Assert:
        self.assertEqual(expected, actual)
        self.assertEqual(0, len(validation_results))

    def test_dilution_scheme_for_qpcr(self):
        """Dilution scheme initialized for qPCR dilutions, containing no output analytes"""
        # Setup:
        def only_input_analyte_set():
            ret = [
                (fake_analyte("cont-id1", "art-id1", "sample1", "art-name1", "D:5", True,
                              concentration=134, volume=30),
                 fake_result_file("sample-measurement1")),
                (fake_analyte("cont-id2", "art-id2", "sample2", "art-name2", "A:5", True,
                              concentration=134, volume=40),
                 fake_result_file("sample-measurement2")),
                (fake_analyte("cont-id2", "art-id3", "sample3", "art-name3", "B:7", True,
                              concentration=134, volume=50),
                 fake_result_file("sample-measurement3")),
                (fake_analyte("cont-id2", "art-id4", "sample4", "art-name4", "E:12", True,
                              concentration=134, volume=60),
                 fake_result_file("sample-measurement4")),
            ]
            return ret

        svc = helpers.mock_artifact_service(only_input_analyte_set)

        dilution_scheme = SourceOnlyDilutionScheme(svc, "Hamilton")

        expected = [
            ['art-name1', 36, 'DNA1', 4],
            ['art-name2', 33, 'DNA2', 4],
            ['art-name3', 50, 'DNA2', 4],
            ['art-name4', 93, 'DNA2', 4]]

        # Test:
        actual = [
            [dilute.sample_name,
             dilute.source_well_index,
             dilute.source_plate_pos,
             4] for dilute in dilution_scheme.transfers
        ]

        # Assert:
        self.assertEqual(expected, actual)


    # TODO: Add a test for buffer volume validation
    def test_dilution_scheme_too_high_sample_volume(self):
        def invalid_analyte_set():
            return [
                (fake_analyte("cont-id1", "art-id1", "sample1", "art-name1", "D:5", True,
                              concentration=100, volume=20),
                 fake_analyte("cont-id1", "art-id1", "sample1", "art-name1", "B:5", False,
                              target_concentration=1000, target_volume=20))
            ]

            return inputs, outputs

        repo = MagicMock()
        repo.all_artifacts = invalid_analyte_set
        svc = ArtifactService(repo)
        dilution_scheme = DilutionScheme(svc, "Hamilton")
        actual = set(str(result) for result in dilution_scheme.validate())
        expected = set(["Error: Too high sample volume: cont-id1(D5)=>cont-id1(B5)",
                      "Warning: Sample has to be evaporated: cont-id1(D5)=>cont-id1(B5)"])
        self.assertEqual(expected, actual)

    def test_dilution_scheme_too_low_sample_volume(self):
        def invalid_analyte_set():
            return [(fake_analyte("cont-id1", "art-id1", "sample1", "art-name1", "D:5",
                                  True, concentration=100, volume=20),
                     fake_analyte("cont-id1", "art-id1", "sample1", "art-name1", "B:5",
                                  False, target_concentration=2, target_volume=20))
                    ]

        svc = helpers.mock_artifact_service(invalid_analyte_set)
        dilution_scheme = DilutionScheme(svc, "Hamilton")
        actual = set(str(result) for result in dilution_scheme.validate())
        expected = set(["Error: Too low sample volume: cont-id1(D5)=>cont-id1(B5)"])
        self.assertEqual(expected, actual)

    def test_dilution_scheme_source_volume_not_set(self):
        def invalid_analyte_set():
            return [(fake_analyte("cont-id1", "art-id1", "sample1", "art-name1", "D:5",
                                  True, concentration=100),
                     fake_analyte("cont-id1", "art-id1", "sample1", "art-name1", "B:5",
                                  False, target_concentration=100, target_volume=20))
                    ]
        svc = helpers.mock_artifact_service(invalid_analyte_set)
        dilution_scheme = DilutionScheme(svc, "Hamilton")
        actual = set(str(result) for result in dilution_scheme.validate())
        expected = set(["Error: Source volume is not set: cont-id1(D5)"])
        self.assertEqual(expected, actual)

    def test_dilution_scheme_source_concentration_not_set(self):
        def invalid_analyte_set():
            return [(fake_analyte("cont-id1", "art-id1", "sample1", "art-name1", "D:5",
                                  True, volume=20),
                     fake_analyte("cont-id1", "art-id1", "sample1", "art-name1", "B:5",
                                  False, target_concentration=100, target_volume=20))
                    ]
        svc = helpers.mock_artifact_service(invalid_analyte_set)
        dilution_scheme = DilutionScheme(svc, "Hamilton")
        actual = set(str(result) for result in dilution_scheme.validate())
        expected = set(["Error: Source concentration not set: cont-id1(D5)"])
        self.assertEqual(expected, actual)

    def test_dilution_scheme_source_concentration_zero(self):
        def invalid_analyte_set():
            return [(fake_analyte("cont-id1", "art-id1", "sample1", "art-name1", "D:5",
                                  True, volume=20, concentration=0),
                     fake_analyte("cont-id1", "art-id1", "sample1", "art-name1", "B:5",
                                  False, target_concentration=100, target_volume=20))
                    ]
        svc = helpers.mock_artifact_service(invalid_analyte_set)
        dilution_scheme = DilutionScheme(svc, "Hamilton")
        actual = set(str(result) for result in dilution_scheme.validate())
        expected = set(["Error: Source concentration not set: cont-id1(D5)"])
        self.assertEqual(expected, actual)


def create_real_repo(step_id):
    """
    A helper for creating an actual StepRepository. Only needed while the StepRepository is still
    in its early stages of development
    """
    from clarity_ext.clarity import ClaritySession
    from clarity_ext.repository.step_repository import StepRepository
    session = ClaritySession.create(step_id)
    return StepRepository(session)

if __name__ == "__main__":
    unittest.main()

