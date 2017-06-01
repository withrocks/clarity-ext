from clarity_ext.service.dilution.service import TransferCalcHandlerBase


class OneToOneConcentrationCalcHandler(TransferCalcHandlerBase):
    """
    Implements sample volume calculations for a one to one dilution,
    referring that a single transfer has a single source well/tube
    and a single destination well/tube, i.e. no pooling involved.
    Transfer volumes are calculated on basis of a requested target
    concentration and target volume by the user.
    """

    def handle_transfer(self, transfer):
        if transfer.source_location.artifact.is_control:
            transfer.pipette_buffer_volume = transfer.target_vol
            return

        transfer.pipette_sample_volume = \
            transfer.target_conc * transfer.target_vol / float(transfer.source_conc)
        transfer.pipette_buffer_volume = \
            max(transfer.target_vol - transfer.pipette_sample_volume, 0)
        transfer.has_to_evaporate = \
            (transfer.target_vol - transfer.pipette_sample_volume) < 0

        transfer.source_vol_delta = -round(transfer.pipette_sample_volume +
                                           self.robot_settings.dilution_waste_volume, 1)
        transfer.pipette_sample_volume = round(transfer.pipette_sample_volume, 1)
        transfer.pipette_buffer_volume = round(transfer.pipette_buffer_volume, 1)


class PoolTransferCalcHandler(TransferCalcHandlerBase):
    def handle_batch(self, batch, dilution_settings, robot_settings):
        # Since we need the average in this handler, we override handle_batch rather than handle_transfer
        for target, transfers in batch.transfers_by_output.items():
            self.logger.debug("Grouped target={}, transfers={}".format(target, transfers))
            regular_transfers = [t for t in transfers if not t.source_location.artifact.is_control]
            sample_size = len(regular_transfers)

            # Validation:
            concs = list(set(t.source_conc for t in regular_transfers))
            if len(concs) > 1:
                # NOTE: Validation is now mixed between the validators and the handlers. It's probably clearer
                # to keep it all in the handlers and remove the validators.
                self.warning("Different source concentrations ({}) in the pool's input: {}. "
                             "Target concentration can't be set for the pool".format(concs, target), transfers)
                target_conc = None
            else:
                target_conc = concs[0]

            for transfer in regular_transfers:
                self.logger.debug("Transfer before transform: {}".format(transfer))
                transfer.pipette_sample_volume = float(transfer.target_vol) / sample_size
                transfer.source_vol_delta = -round(transfer.pipette_sample_volume +
                                                   robot_settings.dilution_waste_volume, 1)
                if target_conc:
                    transfer.target_conc = target_conc
                self.logger.debug("Transfer after transform:  {}".format(transfer))
