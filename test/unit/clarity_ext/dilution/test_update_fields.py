import unittest
from clarity_ext.dilution import DILUTION_WASTE_VOLUME
from mock import MagicMock
from clarity_ext.dilution import DilutionScheme
from clarity_ext.dilution import CONCENTRATION_REF_NGUL
from test.unit.clarity_ext import helpers
from test.unit.clarity_ext.helpers import fake_analyte


class UpdateFieldsForDilutionTests(unittest.TestCase):

    def setUp(self):
        svc = helpers.mock_artifact_service(analyte_set_with_blank)
        self.dilution_scheme = DilutionScheme(
            svc, "Hamilton", concentration_ref=CONCENTRATION_REF_NGUL)

        analyte_pair_by_dilute = self.dilution_scheme.aliquot_pair_by_transfer
        for dilute in self.dilution_scheme.transfers:
            # Make preparation, fetch the analyte to be updated
            source_analyte = analyte_pair_by_dilute[dilute].input_artifact
            destination_analyte = analyte_pair_by_dilute[
                dilute].output_artifact

            # Update fields for analytes
            source_analyte.volume = dilute.source_initial_volume - \
                dilute.requested_volume - DILUTION_WASTE_VOLUME

            destination_analyte.concentration = dilute.requested_concentration

            destination_analyte.volume = dilute.requested_volume


    def test_source_volume_update_1(self):
        dilute = self.dilution_scheme.transfers[-1]
        expected = 29.0
        outcome = self.dilution_scheme.aliquot_pair_by_transfer[
            dilute].input_artifact.volume
        print(dilute.sample_name)
        self.assertEqual(expected, outcome)

    def test_source_volume_update_all(self):
        source_volume_sum = 0
        for dilute in self.dilution_scheme.transfers:
            outcome = self.dilution_scheme.aliquot_pair_by_transfer[
                dilute].input_artifact.volume
            source_volume_sum += outcome
        expected_sum = 57.0
        self.assertEqual(expected_sum, source_volume_sum)

    def test_dilute_aliqout_matching(self):
        aliquot_pair_by_transfer = self.dilution_scheme.aliquot_pair_by_transfer
        for transfer in self.dilution_scheme.transfers:
            source_aliquot = aliquot_pair_by_transfer[transfer].input_artifact
            print("transfer sample name: {}".format(transfer.sample_name))
            print("source aliquot sample name: {}".format(source_aliquot.name))
            self.assertEqual(transfer.sample_name, source_aliquot.name)


def analyte_set_with_blank():
    return [
        (fake_analyte("cont1", "art7", "sample4", "sample4", "E:2", True, is_control=True),
         fake_analyte("cont2", "art8", "sample4", "sample4", "E:2", False, is_control=True,
                      target_concentration=100, target_volume=20)),
        (fake_analyte("cont1", "art1", "sample1", "sample1", "B:2", True,
                      concentration=100, volume=30),
         fake_analyte("cont2", "art2", "sample1", "sample1", "B:2", False,
                      target_concentration=100, target_volume=20)),
        (fake_analyte("cont1", "art3", "sample2", "sample2", "C:2", True,
                      concentration=100, volume=40),
         fake_analyte("cont2", "art4", "sample2", "sample2", "C:2", False,
                      target_concentration=100, target_volume=20)),
        (fake_analyte("cont1", "art5", "sample3", "sample3", "D:2", True,
                      concentration=100, volume=50),
         fake_analyte("cont2", "art6", "sample3", "sample3", "D:2", False,
                      target_concentration=100, target_volume=20)),
    ]

