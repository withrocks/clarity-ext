import unittest
from clarity_ext.service.dilution_service import *
from test.unit.clarity_ext.helpers import *


class UpdateFieldsForDilutionTests(unittest.TestCase):
    def _init_dilution_scheme(self, concentration_ref, analyte_set=None):
        def udf_adapted_analyte_set():
            return analyte_set()

        repo = mock_step_repository(analyte_set=udf_adapted_analyte_set)
        artifact_svc = ArtifactService(step_repository=repo)
        context = mock_context(artifact_service=artifact_svc, step_repo=repo)
        service = DilutionService(artifact_svc)
        dilution_scheme = service.create_scheme(robot_name="Hamilton",
                                                concentration_ref=concentration_ref,
                                                volume_calc_method=VOLUME_CALC_BY_CONC)
        return dilution_scheme, context

    @staticmethod
    def _dilute(dilution_scheme):
        # TODO: Shouldn't this code be in a service (or is there already?)
        aliquot_pair_by_transfer = dilution_scheme.aliquot_pair_by_transfer
        for transfer in dilution_scheme.unsplit_transfers:
            # Make preparation, fetch the analyte to be updated
            source = aliquot_pair_by_transfer(transfer).input_artifact
            destination = aliquot_pair_by_transfer(
                transfer).output_artifact

            # Update fields for analytes
            source.udf_current_sample_volume_ul = max(
                transfer.source_initial_volume - transfer.sample_volume - DILUTION_WASTE_VOLUME, 0)

            destination.udf_target_conc_ngul = (transfer.sample_volume *
                                                transfer.source_concentration /
                                                (transfer.sample_volume + transfer.pipette_buffer_volume))
            destination.udf_target_vol_ul = transfer.sample_volume + transfer.pipette_buffer_volume
            yield source, destination

    def _update_fields(self, dilution_scheme, context):
        for source, destination in self._dilute(dilution_scheme):
            context.update(source)
            context.update(destination)
        context.commit()

    def test_source_volume_update_1(self):
        dilution_scheme, context = self._init_dilution_scheme(
            concentration_ref=CONCENTRATION_REF_NGUL, analyte_set=analyte_set_with_blank)
        self._update_fields(dilution_scheme, context)
        transfer = dilution_scheme.unsplit_transfers[-1]
        expected = 29.0
        outcome = dilution_scheme.aliquot_pair_by_transfer(
            transfer).input_artifact.udf_current_sample_volume_ul
        print(transfer.aliquot_name)
        self.assertEqual(expected, outcome)

    def test_source_volume_update_all(self):
        dilution_scheme, context = self._init_dilution_scheme(
            concentration_ref=CONCENTRATION_REF_NGUL, analyte_set=analyte_set_with_blank)
        self._update_fields(dilution_scheme, context)
        source_volume_sum = 0
        for transfer in dilution_scheme.unsplit_transfers:
            outcome = dilution_scheme.aliquot_pair_by_transfer(
                transfer).input_artifact.udf_current_sample_volume_ul
            source_volume_sum += outcome
        expected_sum = 57.0
        self.assertEqual(expected_sum, source_volume_sum)

    def test_dilute_aliqout_matching(self):
        dilution_scheme, context = self._init_dilution_scheme(
            concentration_ref=CONCENTRATION_REF_NGUL, analyte_set=analyte_set_with_blank)
        self._update_fields(dilution_scheme, context)
        aliquot_pair_by_transfer = dilution_scheme.aliquot_pair_by_transfer
        for transfer in dilution_scheme.unsplit_transfers:
            source_aliquot = aliquot_pair_by_transfer(transfer).input_artifact
            print("transfer sample name: {}".format(transfer.aliquot_name))
            print("source aliquot sample name: {}".format(source_aliquot.name))
            self.assertEqual(transfer.aliquot_name, source_aliquot.name)

    def test_update_split_rows(self):
        def analyte_set_split_rows(udfs=None):
            api_resource = MagicMock()
            api_resource.udf = {}
            return [
                (fake_analyte("cont1", "art7", "sample4", "sample4", "E:2", True, is_control=True,
                              udfs=udfs, api_resource=api_resource),
                 fake_analyte("cont2", "art8", "sample4", "sample4", "E:2", False, is_control=True,
                              udfs=udfs, api_resource=api_resource,
                              requested_concentration_ngul=100, requested_volume=20)),
                (fake_analyte("cont1", "art1", "sample1", "sample1", "B:2", True,
                              udfs=udfs, api_resource=api_resource,
                              concentration_ngul=100, volume=30),
                 fake_analyte("cont2", "art2", "sample1", "sample1", "B:2", False,
                              udfs=udfs, api_resource=api_resource,
                              requested_concentration_ngul=100, requested_volume=70)),
            ]

        dilution_scheme, context = self._init_dilution_scheme(
            concentration_ref=CONCENTRATION_REF_NGUL, analyte_set=analyte_set_split_rows)
        self._update_fields(dilution_scheme, context)

        diluted = list(self._dilute(dilution_scheme))
        assert len(diluted) == 1
        source, destination = diluted[0]
        self.assertEqual(source.udf_current_sample_volume_ul, 0)
        self.assertEqual(destination.udf_target_vol_ul, 70.0)
        self.assertEqual(destination.udf_target_conc_ngul, 100.0)

def analyte_set_with_blank(udfs=None):
    api_resource = MagicMock()
    api_resource.udf = {}
    return [
        (fake_analyte("cont1", "art7", "sample4", "sample4", "E:2", True, is_control=True,
                      udfs=udfs, api_resource=api_resource),
         fake_analyte("cont2", "art8", "sample4", "sample4", "E:2", False, is_control=True,
                      udfs=udfs, api_resource=api_resource,
                      requested_concentration_ngul=100, requested_volume=20)),
        (fake_analyte("cont1", "art1", "sample1", "sample1", "B:2", True,
                      udfs=udfs, api_resource=api_resource,
                      concentration_ngul=100, volume=30),
         fake_analyte("cont2", "art2", "sample1", "sample1", "B:2", False,
                      udfs=udfs, api_resource=api_resource,
                      requested_concentration_ngul=100, requested_volume=20)),
        (fake_analyte("cont1", "art3", "sample2", "sample2", "C:2", True,
                      udfs=udfs, api_resource=api_resource,
                      concentration_ngul=100, volume=40),
         fake_analyte("cont2", "art4", "sample2", "sample2", "C:2", False,
                      udfs=udfs, api_resource=api_resource,
                      requested_concentration_ngul=100, requested_volume=20)),
        (fake_analyte("cont1", "art5", "sample3", "sample3", "D:2", True,
                      udfs=udfs, api_resource=api_resource,
                      concentration_ngul=100, volume=50),
         fake_analyte("cont2", "art6", "sample3", "sample3", "D:2", False,
                      udfs=udfs, api_resource=api_resource,
                      requested_concentration_ngul=100, requested_volume=20)),
    ]
