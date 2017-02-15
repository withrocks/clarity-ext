import unittest
from mock import MagicMock
from clarity_ext.service.dilution.service import *
from test.unit.clarity_ext.helpers import fake_analyte, fake_result_file
from test.unit.clarity_ext import helpers
from clarity_ext.service import ArtifactService
from clarity_ext.domain.validation import ValidationException
from clarity_ext.domain.validation import ValidationType
from clarity_ext import utils


class DilutionRobotSettings(RobotSettings):
    pass



class TestDilutionScheme(unittest.TestCase):
    """
    Validates that the DilutionScheme property of the context object returns the expected values
    based on fake data from the LIMS
    """

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
                              concentration_ngul=10.0),
                 fake_analyte("cont-id1", "art1-id2", "sample1", "sample1", "B:2", False,
                              requested_concentration_ngul=50.0, requested_volume=10.0)),
                (fake_analyte("cont-id1", "art1-id3", "sample2", "sample2", "C:2", True,
                              concentration_ngul=10.0),
                 fake_analyte("cont-id1", "art1-id4", "sample2", "sample2", "C:2", False,
                              requested_concentration_ngul=10.0, requested_volume=51.0)),
                (fake_analyte("cont-id1", "art1-id5", "sample3", "sample3", "D:2", True,
                              concentration_ngul=100.0),
                 fake_analyte("cont-id1", "art1-id6", "sample3", "sample3", "D:2", False,
                              requested_concentration_ngul=2.0, requested_volume=50.0)),
                (fake_analyte("cont-id1", "art1-id7", "sample4", "sample4", "E:2", True,
                              concentration_ngul=100.0),
                 fake_analyte("cont-id1", "art1-id8", "sample4", "sample4", "E:2", False,
                              requested_concentration_ngul=10.0, requested_volume=150.0)),
                (fake_analyte("cont-id1", "art1-id9", "sample5", "sample5", "F:2", True,
                              concentration_ngul=100.0),
                 fake_analyte("cont-id1", "art1-id10", "sample5", "sample5", "F:2", False,
                              requested_concentration_ngul=60.0, requested_volume=150.0)),
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
            [transfer.aliquot_name,
             transfer.source_well_index,
             transfer.source_plate_pos,
             round(transfer.sample_volume, 1),
             round(transfer.pipette_buffer_volume, 1),
             transfer.target_well_index,
             transfer.target_plate_pos] for transfer in dilution_scheme.split_row_transfers
        ]

        print("actual:")
        for row in actual:
            print("{}".format(row))

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
        actual = set(str(result)
                     for result in pre_validate_dilution(dilution_scheme))
        expected = set(["Error: art-name1, source volume is not set."])
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
        actual = set(str(result)
                     for result in pre_validate_dilution(dilution_scheme))
        expected = set(["Error: art-name1, source concentration not set."])
        self.assertEqual(expected, actual)

    def test_dilution_scheme_source_concentration_zero(self):
        def invalid_analyte_set():
            return [(fake_analyte("cont-id1", "art-id1", "sample1", "art-name1", "D:5",
                                  True, volume=20, concentration_ngul=0),
                     fake_analyte("cont-id1", "art-id1", "sample1", "art-name1", "B:5",
                                  False, requested_concentration_ngul=100, requested_volume=20))
                    ]
        svc = helpers.mock_artifact_service(invalid_analyte_set)
        dilution_scheme = self._default_dilution_scheme(svc)
        actual = set(str(result)
                     for result in pre_validate_dilution(dilution_scheme))
        expected = set(["Error: art-name1, source concentration not set."])
        self.assertEqual(expected, actual)

    def test_requested_concentration_not_set(self):
        def invalid_analyte_set():
            return [(fake_analyte("cont-id1", "art-id1", "sample1", "art-name1", "D:5",
                                  True, volume=20, concentration_ngul=10),
                     fake_analyte("cont-id1", "art-id1", "sample1", "art-name1", "B:5",
                                  False, requested_volume=20))
                    ]
        svc = helpers.mock_artifact_service(invalid_analyte_set)
        dilution_scheme = self._default_dilution_scheme(svc)
        actual = set(str(result)
                     for result in pre_validate_dilution(dilution_scheme))
        expected = set(["Error: art-name1, target concentration is not set."])
        self.assertEqual(expected, actual)

    def test_requested_volume_not_set(self):
        def invalid_analyte_set():
            return [(fake_analyte("cont-id1", "art-id1", "sample1", "art-name1", "D:5",
                                  True, volume=20, concentration_ngul=10),
                     fake_analyte("cont-id1", "art-id1", "sample1", "art-name1", "B:5",
                                  False, requested_concentration_ngul=20))
                    ]
        svc = helpers.mock_artifact_service(invalid_analyte_set)
        dilution_scheme = self._default_dilution_scheme(svc)
        actual = set(str(result)
                     for result in pre_validate_dilution(dilution_scheme))
        expected = set(["Error: art-name1, target volume is not set."])
        self.assertEqual(expected, actual)


def two_containers_artifact_set_nm():
    """
    Returns a list of (inputs, outputs) fake analytes for a particular step.

    Analytes have been sorted, as they would be when queried from the repository.
    """
    ret = [
        (fake_analyte("cont-id1", "art-id1", "sample1", "art-name1", "D:5", True,
                      concentration_nm=140.0, volume=20.0),
         fake_analyte("cont-id3", "art-id1", "sample1", "art-name1", "B:5", False,
                      requested_concentration_nm=100.0, requested_volume=20.0)),
        (fake_analyte("cont-id2", "art-id2", "sample2", "art-name2", "A:5", True,
                      concentration_nm=140.0, volume=40.0),
         fake_analyte("cont-id4", "art-id2", "sample2", "art-name2", "A:3", False,
                      requested_concentration_nm=100.0, requested_volume=20.0)),
        (fake_analyte("cont-id2", "art-id3", "sample3", "art-name3", "B:7", True,
                      concentration_nm=140.0, volume=50.0),
         fake_analyte("cont-id3", "art-id3", "sample3", "art-name3", "D:6", False,
                      requested_concentration_nm=100.0, requested_volume=20.0)),
        (fake_analyte("cont-id2", "art-id4", "sample4", "art-name4", "E:12", True,
                      concentration_nm=140.0, volume=60.0),
         fake_analyte("cont-id4", "art-id4", "sample4", "art-name4", "E:9", False,
                      requested_concentration_nm=100.0, requested_volume=20.0))
    ]
    return ret


def pre_validate_dilution(dilution_scheme):
    """
    Check that all pertinent variables are initiated so that calculations
    are possible to perform
    """
    for transfer in dilution_scheme.unsplit_transfers:
        if not transfer.source_initial_volume:
            yield ValidationException("{}, source volume is not set.".format(transfer.aliquot_name))
        if not transfer.source_concentration:
            yield ValidationException("{}, source concentration not set.".format(transfer.aliquot_name))
        if not transfer.requested_concentration:
            yield ValidationException("{}, target concentration is not set.".format(transfer.aliquot_name))
        if not transfer.requested_volume:
            yield ValidationException("{}, target volume is not set.".format(transfer.aliquot_name))


def post_validate_dilution(dilution_scheme):
    """
    Check calculation results if its conform to hardware restrictions
    """
    def pos_str(transfer):
        return "{}=>{}".format(transfer.source_well, transfer.target_well)

    for t in dilution_scheme.unsplit_transfers:
        if t.sample_volume + t.pipette_buffer_volume > 100:
            yield ValidationException("{}, too high destination volume ({}).".format(
                t.aliquot_name, pos_str(t)))
        if t.has_to_evaporate:
            yield ValidationException("{}, sample has to be evaporated ({}).".format(
                t.aliquot_name, pos_str(t)), ValidationType.WARNING)
        if t.scaled_up:
            yield ValidationException("{}, sample volume is scaled up due to pipetting min volume of 2 ul ({}).".format(
                t.aliquot_name, pos_str(t)), ValidationType.WARNING)


if __name__ == "__main__":
    unittest.main()

