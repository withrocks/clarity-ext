import unittest
from mock import MagicMock
from clarity_ext.dilution import DilutionScheme
from test.unit.clarity_ext.helpers import fake_analyte


class TestDilutionScheme(unittest.TestCase):
    """
    Validates that the DilutionScheme property of the context object returns the expected values
    based on fake data from the LIMS
    """

    def test_dilution_scheme_hamilton_expected_results(self):
        """Dilution scheme created by mocked analytes is correctly generated for Hamilton"""
        # Setup:
        repo = MagicMock()
        repo.all_analytes = all_analyte_test_set1
        dilution_scheme = DilutionScheme(repo, "Hamilton")

        expected = [
            ['art-name1', '36', 'DNA1', '14.9', '5.1', '34', 'END1'],
            ['art-name2', '33', 'DNA2', '14.9', '5.1', '17', 'END2'],
            ['art-name3', '50', 'DNA2', '14.9', '5.1', '44', 'END1'],
            ['art-name4', '93', 'DNA2', '14.9', '5.1', '69', 'END2']]

        # Test:
        actual = [
            [dilute.sample_name,
             "{}".format(dilute.source_well_index),
             dilute.source_plate_pos,
             "{:.1f}".format(dilute.sample_volume),
             "{:.1f}".format(dilute.buffer_volume),
             "{}".format(dilute.target_well_index),
             dilute.target_plate_pos] for dilute in dilution_scheme.dilutes
        ]

        # Assert:
        self.assertEqual(expected, actual)


def all_analyte_test_set1():
    """
    Returns a list of (inputs, outputs) fake analytes for a particular step.

    Analytes have been sorted, as they would be when queried from the repository.
    """
    inputs = [
        fake_analyte("cont-id1", "art-id1", "sample1", "art-name1", "D:5", concentration=134),
        fake_analyte("cont-id2", "art-id2", "sample2", "art-name2", "A:5", concentration=134),
        fake_analyte("cont-id2", "art-id3", "sample3", "art-name3", "B:7", concentration=134),
        fake_analyte("cont-id2", "art-id4", "sample4", "art-name4", "E:12", concentration=134)]

    outputs = [
        fake_analyte("cont-id1", "art-id1", "sample1", "art-name1", "B:5", target_concentration=100, target_volume=20),
        fake_analyte("cont-id2", "art-id2", "sample2", "art-name2", "A:3", target_concentration=100, target_volume=20),
        fake_analyte("cont-id1", "art-id3", "sample3", "art-name3", "D:6", target_concentration=100, target_volume=20),
        fake_analyte("cont-id2", "art-id4", "sample4", "art-name4", "E:9", target_concentration=100, target_volume=20)]

    return inputs, outputs


def create_real_repo(step_id):
    """
    A helper for creating an actual StepRepository. Only needed while the StepRepository is still
    in its early stages of development
    """
    from clarity_ext.clarity import ClaritySession
    from clarity_ext.domain.artifact import StepRepository
    session = ClaritySession.create(step_id)
    return StepRepository(session)

if __name__ == "__main__":
    unittest.main()

