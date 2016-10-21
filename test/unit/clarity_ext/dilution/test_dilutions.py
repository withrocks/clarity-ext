import unittest
from mock import MagicMock
from clarity_ext.dilution import DilutionScheme
from clarity_ext.dilution import CONCENTRATION_REF_NGUL
from clarity_ext.dilution import CONCENTRATION_REF_NM
from test.unit.clarity_ext.helpers import fake_analyte, fake_result_file
from test.unit.clarity_ext import helpers
from clarity_ext.service import ArtifactService


class TestDilutionScheme(unittest.TestCase):
    """
    Validates that the DilutionScheme property of the context object returns the expected values
    based on fake data from the LIMS
    """

    def _default_dilution_scheme(self, artifact_service, concentration_ref=CONCENTRATION_REF_NGUL):
        return DilutionScheme(artifact_service=artifact_service, robot_name="Hamilton",
                              scale_up_low_volumes=True, concentration_ref=concentration_ref)

    def test_dilution_scheme_hamilton_base(self):
        """Dilution scheme created by mocked analytes is correctly generated for Hamilton"""
        # Setup:
        svc = helpers.mock_two_containers_artifact_service()
        dilution_scheme = self._default_dilution_scheme(svc)

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

    def test_dilution_scheme_hamilton_nm(self):
        """Dilution scheme based on nM concentration"""
        # Setup:
        svc = helpers.mock_artifact_service(two_containers_artifact_set_nm)
        dilution_scheme = self._default_dilution_scheme(svc, concentration_ref=CONCENTRATION_REF_NM)

        expected = [
            ['art-name1', 36, 'DNA1', 14.3, 5.7, 34, 'END1'],
            ['art-name2', 33, 'DNA2', 14.3, 5.7, 17, 'END2'],
            ['art-name3', 50, 'DNA2', 14.3, 5.7, 44, 'END1'],
            ['art-name4', 93, 'DNA2', 14.3, 5.7, 69, 'END2']]

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

    def test_dilution_scheme_including_blanks(self):
        """Generate driver file with a step containing blank controls"""
        # Setup:
        def analyte_set_with_blank():
            return [
                (fake_analyte("cont1", "art1", "sample1", "sample1", "B:2", True,
                              concentration_ngul=100),
                 fake_analyte("cont2", "art2", "sample1", "sample1", "B:2", False,
                              requested_concentration_ngul=10, requested_volume=20)),
                (fake_analyte("cont1", "art7", "sample4", "sample4", "E:2", True, is_control=True),
                 fake_analyte("cont2", "art8", "sample4", "sample4", "E:2", False, is_control=True,
                              requested_concentration_ngul=10, requested_volume=20)),
                (fake_analyte("cont1", "art3", "sample2", "sample2", "C:2", True,
                              concentration_ngul=100),
                 fake_analyte("cont2", "art4", "sample2", "sample2", "C:2", False,
                              requested_concentration_ngul=10, requested_volume=20)),
                (fake_analyte("cont1", "art5", "sample3", "sample3", "D:2", True,
                              concentration_ngul=100),
                 fake_analyte("cont2", "art6", "sample3", "sample3", "D:2", False,
                              requested_concentration_ngul=10, requested_volume=20)),
            ]

        svc = helpers.mock_artifact_service(analyte_set_with_blank)
        dilution_scheme = self._default_dilution_scheme(svc)

        mydict = analyte_set_with_blank()[3][0].__dict__
        print("control fake analyte: {}".format(mydict))
        for key in mydict:
            print("{} {}\n".format(key, mydict[key]))

        expected = [
            ['sample1', 10, 'DNA1', 2.0, 18.0, 10, 'END1'],
            ['sample2', 11, 'DNA1', 2.0, 18.0, 11, 'END1'],
            ['sample3', 12, 'DNA1', 2.0, 18.0, 12, 'END1'],
            ]

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

        # Assert:
        self.assertEqual(expected, actual)

    def test_scaled_up_volume(self):
        """
        Test scaling up volumes when sample volume is < 2 ul
        1st analyte pair: buffer volume > 0
        2nd analyte pair: target conc = source conc, volume transfer with v < 2 ul
        3rd analyte pair: Evaporation with sample volume < 2 ul
        4th analyte pair: No scaling up, no evaporation
        """
        def scaled_up_analyte_set():
            return [
                (fake_analyte("cont1", "art1", "sample1", "sample1", "B:2", True,
                              concentration_ngul=200),
                 fake_analyte("cont2", "art2", "sample1", "sample1", "B:2", False,
                              requested_concentration_ngul=10, requested_volume=20)),
                (fake_analyte("cont1", "art3", "sample2", "sample2", "C:2", True,
                              concentration_ngul=10),
                 fake_analyte("cont2", "art4", "sample2", "sample2", "C:2", False,
                              requested_concentration_ngul=10, requested_volume=1)),
                (fake_analyte("cont1", "art5", "sample3", "sample3", "D:2", True,
                              concentration_ngul=20),
                 fake_analyte("cont2", "art6", "sample3", "sample3", "D:2", False,
                              requested_concentration_ngul=40, requested_volume=0.5)),
                (fake_analyte("cont1", "art7", "sample4", "sample4", "E:2", True,
                              concentration_ngul=80),
                 fake_analyte("cont2", "art8", "sample4", "sample4", "E:2", False,
                              requested_concentration_ngul=40, requested_volume=10)),
            ]

        svc = helpers.mock_artifact_service(scaled_up_analyte_set)
        dilution_scheme = self._default_dilution_scheme(svc)

        expected = [
            ['sample1', 10, 'DNA1', 2.0, 38.0, 10, 'END1'],
            ['sample2', 11, 'DNA1', 2.0, 0, 11, 'END1'],
            ['sample3', 12, 'DNA1', 2.0, 0, 12, 'END1'],
            ['sample4', 13, 'DNA1', 5.0, 5.0, 13, 'END1'],
        ]

        actual = [
            [dilute.sample_name,
             dilute.source_well_index,
             dilute.source_plate_pos,
             round(dilute.sample_volume, 1),
             round(dilute.buffer_volume, 1),
             dilute.target_well_index,
             dilute.target_plate_pos] for dilute in dilution_scheme.transfers
        ]
        self.assertEqual(dilution_scheme.transfers[0].has_to_evaporate, False)
        self.assertEqual(dilution_scheme.transfers[1].has_to_evaporate, False)
        self.assertEqual(dilution_scheme.transfers[2].has_to_evaporate, True)
        self.assertEqual(dilution_scheme.transfers[3].has_to_evaporate, False)
        self.assertEqual(dilution_scheme.transfers[0].scaled_up, True)
        self.assertEqual(dilution_scheme.transfers[1].scaled_up, True)
        self.assertEqual(dilution_scheme.transfers[2].scaled_up, True)
        self.assertEqual(dilution_scheme.transfers[3].scaled_up, False)
        self.assertEqual(expected, actual)

    def test_split_rows_for_high_volume(self):
        """
        Test that sample/buffer volume > 50 ul is split up into multiple
        rows, so that the pipetting max volume is not exceeded
        1st analyte pair: sample volume = 50 ul
        2nd analyte pair: sample volume = 51 ul
        3rd analyte pair: buffer volume = 98 ul, scaled up
        4th analyte pair: buffer volume = 135 ul
        5th analyte pair: buffer volume = 60, sample volume = 90
        """
        def high_volume_analyte_set():
            return [
                (fake_analyte("cont-id1", "art1-id1", "sample1", "sample1", "B:2", True,
                              concentration_ngul=10),
                 fake_analyte("cont-id1", "art1-id2", "sample1", "sample1", "B:2", False,
                              requested_concentration_ngul=50, requested_volume=10)),
                (fake_analyte("cont-id1", "art1-id3", "sample2", "sample2", "C:2", True,
                              concentration_ngul=10),
                 fake_analyte("cont-id1", "art1-id4", "sample2", "sample2", "C:2", False,
                              requested_concentration_ngul=10, requested_volume=51)),
                (fake_analyte("cont-id1", "art1-id5", "sample3", "sample3", "D:2", True,
                              concentration_ngul=100),
                 fake_analyte("cont-id1", "art1-id6", "sample3", "sample3", "D:2", False,
                              requested_concentration_ngul=2, requested_volume=50)),
                (fake_analyte("cont-id1", "art1-id7", "sample4", "sample4", "E:2", True,
                              concentration_ngul=100),
                 fake_analyte("cont-id1", "art1-id8", "sample4", "sample4", "E:2", False,
                              requested_concentration_ngul=10, requested_volume=150)),
                (fake_analyte("cont-id1", "art1-id9", "sample5", "sample5", "F:2", True,
                              concentration_ngul=100),
                 fake_analyte("cont-id1", "art1-id10", "sample5", "sample5", "F:2", False,
                              requested_concentration_ngul=60, requested_volume=150)),
            ]

        svc = helpers.mock_artifact_service(high_volume_analyte_set)
        dilution_scheme = self._default_dilution_scheme(svc)
        expected = [
            ['sample1', 10, 'DNA1', 50.0, 0, 10, 'END1'],
            ['sample2', 11, 'DNA1', 25.5, 0, 11, 'END1'],
            ['sample2', 11, 'DNA1', 25.5, 0, 11, 'END1'],
            ['sample3', 12, 'DNA1', 2.0, 49.0, 12, 'END1'],
            ['sample3', 12, 'DNA1', 0, 49.0, 12, 'END1'],
            ['sample4', 13, 'DNA1', 15.0, 45.0, 13, 'END1'],
            ['sample4', 13, 'DNA1', 0, 45.0, 13, 'END1'],
            ['sample4', 13, 'DNA1', 0, 45.0, 13, 'END1'],
            ['sample5', 14, 'DNA1', 45.0, 30.0, 14, 'END1'],
            ['sample5', 14, 'DNA1', 45.0, 30.0, 14, 'END1'],
        ]

        actual = [
            [dilute.sample_name,
             dilute.source_well_index,
             dilute.source_plate_pos,
             round(dilute.sample_volume, 1),
             round(dilute.buffer_volume, 1),
             dilute.target_well_index,
             dilute.target_plate_pos] for dilute in dilution_scheme.transfers
        ]

        print("actual:")
        for row in actual:
            print("{}".format(row))

        self.assertEqual(expected, actual)

    def test_dilution_scheme_for_qpcr(self):
        """Dilution scheme initialized for qPCR dilutions, containing no output analytes"""
        # Setup:
        def analyte_result_file_set():
            ret = [
                (fake_analyte("cont-id1", "art-id1", "sample1", "art-name1", "D:5", True,
                              concentration=134, volume=30),
                 fake_result_file(artifact_id="sample-measurement1", name="sample1",
                                  container_id="cont1", well_key="D:5")),
                (fake_analyte("cont-id2", "art-id2", "sample2", "art-name2", "A:5", True,
                              concentration=134, volume=40),
                 fake_result_file(artifact_id="sample-measurement2", name="sample2",
                                  container_id="cont1", well_key="B:7")),
                (fake_analyte("cont-id2", "art-id3", "sample3", "art-name3", "B:7", True,
                              concentration=134, volume=50),
                 fake_result_file(artifact_id="sample-measurement3", name="sample3",
                                  container_id="cont1", well_key="B:7")),
                (fake_analyte("cont-id2", "art-id4", "sample4", "art-name4", "E:12", True,
                              concentration=134, volume=60),
                 fake_result_file(artifact_id="sample-measurement4", name="sample4",
                                  container_id="cont1", well_key="E:12")),
            ]
            return ret

        svc = helpers.mock_artifact_service(analyte_result_file_set)

        dilution_scheme = self._default_dilution_scheme(svc)

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
    @unittest.skip("Remove when error and warning checks are in place in scripts")
    # Remove this test, and all other error and warning tests, when these checks are
    # in place in the scripts
    def test_dilution_scheme_too_high_sample_volume(self):
        def invalid_analyte_set():
            return [
                (fake_analyte("cont-id1", "art-id1", "sample1", "art-name1", "D:5", True,
                              concentration=100, volume=20),
                 fake_analyte("cont-id1", "art-id1", "sample1", "art-name1", "B:5", False,
                              requested_concentration_ngul=1000, requested_volume=20))
            ]

            return inputs, outputs

        repo = MagicMock()
        logger_service = MagicMock()
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
                                  True, concentration_ngul=100, volume=20),
                     fake_analyte("cont-id1", "art-id1", "sample1", "art-name1", "B:5",
                                  False, requested_concentration_ngul=2, requested_volume=20))
                    ]

        svc = helpers.mock_artifact_service(invalid_analyte_set)
        dilution_scheme = DilutionScheme(
            svc, "Hamilton", scale_up_low_volumes=False, concentration_ref=CONCENTRATION_REF_NGUL)
        actual = set(str(result) for result in dilution_scheme.validate())
        expected = set(["Error: Too low sample volume: cont-id1(D5)=>cont-id1(B5)"])
        self.assertEqual(expected, actual)

    def test_dilution_scheme_source_volume_not_set(self):
        def invalid_analyte_set():
            return [(fake_analyte("cont-id1", "art-id1", "sample1", "art-name1", "D:5",
                                  True, concentration_ngul=100),
                     fake_analyte("cont-id1", "art-id1", "sample1", "art-name1", "B:5",
                                  False, requested_concentration_ngul=100, requested_volume=20))
                    ]
        svc = helpers.mock_artifact_service(invalid_analyte_set)
        dilution_scheme = self._default_dilution_scheme(svc)
        actual = set(str(result) for result in dilution_scheme.validate())
        expected = set(["Error: Source volume is not set: cont-id1(D5)"])
        self.assertEqual(expected, actual)

    def test_dilution_scheme_source_concentration_not_set(self):
        def invalid_analyte_set():
            return [(fake_analyte("cont-id1", "art-id1", "sample1", "art-name1", "D:5",
                                  True, volume=20),
                     fake_analyte("cont-id1", "art-id1", "sample1", "art-name1", "B:5",
                                  False, requested_concentration_ngul=100, requested_volume=20))
                    ]
        svc = helpers.mock_artifact_service(invalid_analyte_set)
        dilution_scheme = self._default_dilution_scheme(svc)
        actual = set(str(result) for result in dilution_scheme.validate())
        expected = set(["Error: Source concentration not set: cont-id1(D5)"])
        self.assertEqual(expected, actual)

    def test_dilution_scheme_source_concentration_zero(self):
        def invalid_analyte_set():
            return [(fake_analyte("cont-id1", "art-id1", "sample1", "art-name1", "D:5",
                                  True, volume=20, concentration=0),
                     fake_analyte("cont-id1", "art-id1", "sample1", "art-name1", "B:5",
                                  False, requested_concentration_ngul=100, requested_volume=20))
                    ]
        svc = helpers.mock_artifact_service(invalid_analyte_set)
        dilution_scheme = self._default_dilution_scheme(svc)
        actual = set(str(result) for result in dilution_scheme.validate())
        expected = set(["Error: Source concentration not set: cont-id1(D5)"])
        self.assertEqual(expected, actual)


def two_containers_artifact_set_nm():
    """
    Returns a list of (inputs, outputs) fake analytes for a particular step.

    Analytes have been sorted, as they would be when queried from the repository.
    """
    ret = [
        (fake_analyte("cont-id1", "art-id1", "sample1", "art-name1", "D:5", True,
                      concentration_nm=140, volume=20),
         fake_analyte("cont-id3", "art-id1", "sample1", "art-name1", "B:5", False,
                      requested_concentration_nm=100, requested_volume=20)),
        (fake_analyte("cont-id2", "art-id2", "sample2", "art-name2", "A:5", True,
                      concentration_nm=140, volume=40),
         fake_analyte("cont-id4", "art-id2", "sample2", "art-name2", "A:3", False,
                      requested_concentration_nm=100, requested_volume=20)),
        (fake_analyte("cont-id2", "art-id3", "sample3", "art-name3", "B:7", True,
                      concentration_nm=140, volume=50),
         fake_analyte("cont-id3", "art-id3", "sample3", "art-name3", "D:6", False,
                      requested_concentration_nm=100, requested_volume=20)),
        (fake_analyte("cont-id2", "art-id4", "sample4", "art-name4", "E:12", True,
                      concentration_nm=140, volume=60),
         fake_analyte("cont-id4", "art-id4", "sample4", "art-name4", "E:9", False,
                      requested_concentration_nm=100, requested_volume=20))
    ]
    return ret


if __name__ == "__main__":
    unittest.main()

