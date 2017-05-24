import abc
from clarity_ext.service.dilution.service import *


class TransferHandlerBase(object):
    """Base class for all handlers"""
    __metaclass__ = abc.ABCMeta

    def __init__(self, dilution_session):
        self.dilution_session = dilution_session
        self.validation_exceptions = list()
        self.logger = logging.getLogger(__name__)

    def error(self, msg, transfers):
        """
        Adds a validation exception to the list of validation errors and warnings. Errors are logged after the handler
        is done processing and then a UsageError is thrown.
        """
        self.validation_exceptions.extend(self._create_exceptions(msg, transfers, ValidationType.ERROR))

    def warning(self, msg, transfers):
        """
        Adds a validation warning to the list of validation warnings. Validation warnings are only logged.

        Transfers can be either a list of transfers or one transfer
        """
        self.validation_exceptions.extend(self._create_exceptions(msg, transfers, ValidationType.WARNING))


class TransferSplitHandlerBase(TransferHandlerBase):
    """Base class for handlers that can split one transfer into more"""
    __metaclass__ = abc.ABCMeta

    @abc.abstractmethod
    def needs_row_split(self, transfer, dilution_settings, robot_settings):
        pass

    @abc.abstractmethod
    def split_single_transfer(self, transfer, robot_settings):
        pass

    def handle_batch(self, transfer_batch, dilution_settings, robot_settings):
        require_row_split = [t for t in transfer_batch.transfers
                             if self.needs_row_split(t, dilution_settings, robot_settings)]
        for transfer in require_row_split:
            split_transfers = self.split_single_transfer(transfer, robot_settings)
            if sum(t.pipette_sample_volume + t.pipette_buffer_volume for t in split_transfers) > \
                    robot_settings.max_pipette_vol_for_row_split:
                raise UsageError("Total volume has reached the max well volume ({})".format(
                    robot_settings.max_pipette_vol_for_row_split))
            transfer_batch.transfers.remove(transfer)
            transfer_batch.transfers.extend(split_transfers)


class TransferBatchHandlerBase(TransferHandlerBase):
    __metaclass__ = abc.ABCMeta

    @abc.abstractmethod
    def needs_split(self, transfer, dilution_settings, robot_settings):
        pass

    def handle_batch(self, transfer_batch, dilution_settings, robot_settings):
        """
        Returns one or two transfer_batches, based on rules. Can be used to split a transfer_batch into original and
        temporary transfer_batches
        """
        # Raise an error if any transfer requires evaporation, that's currently not implemented
        """
        for transfer in transfer_batch.transfers:
            if transfer.pipette_sample_volume > transfer.target_vol:
                raise UsageError("Evaporation needed for '{}' - not implemented yet".format(
                    transfer.target_location.artifact.name))
        """

        split = [t for t in transfer_batch.transfers if self.needs_split(t, dilution_settings, robot_settings)]
        no_split = [t for t in transfer_batch.transfers if t not in split]
        print split, no_split

        if len(split) > 0:
            return self.split_transfer_batch(split, no_split, dilution_settings, robot_settings)
        else:
            # No split was required
            return TransferBatchCollection(transfer_batch)

    def split_transfer_batch(self, split, no_split, dilution_settings, robot_settings):
        first_transfers = list(self.calculate_split_transfers(split))
        temp_transfer_batch = TransferBatch(first_transfers, robot_settings, depth=1, is_temporary=True)
        self.dilution_session.dilution_service.execute_handlers(self.dilution_session.transfer_calc_handlers,
                                                                temp_transfer_batch,
                                                                dilution_settings, robot_settings)
        second_transfers = list()

        # We need to create a new transfers list with:
        #  - target_location should be the original target location
        #  - source_location should also bet the original source location
        for temp_transfer in temp_transfer_batch.transfers:
            # NOTE: It's correct that the source_location is being set to the target_location here:
            new_transfer = SingleTransfer(temp_transfer.target_conc, temp_transfer.target_vol,
                                          temp_transfer.original.target_conc, temp_transfer.original.target_vol,
                                          source_location=temp_transfer.target_location,
                                          target_location=temp_transfer.original.target_location)

            # In the case of a split TransferBatch, only the secondary transfer should update source volume:
            new_transfer.should_update_source_vol = False
            second_transfers.append(new_transfer)

        # Add other transfers just as they were:
        second_transfers.extend(no_split)

        final_transfer_batch = TransferBatch(second_transfers, robot_settings, depth=1)
        temp_transfer_batch.split = True
        final_transfer_batch.split = True

        self.dilution_session.dilution_service.execute_handlers(self.dilution_session.transfer_calc_handlers,
                                                                final_transfer_batch, dilution_settings, robot_settings)
        # For the analytes requiring splits
        return TransferBatchCollection(temp_transfer_batch, final_transfer_batch)

    def calculate_split_transfers(self, original_transfers):
        # For each target well, we need to push this to a temporary plate:

        # First we need a map from the actual target plates to temp plates:
        map_target_container_to_temp = dict()
        for transfer in original_transfers:
            target_container = transfer.target_location.container
            if target_container not in map_target_container_to_temp:
                temp_container = Container.create_from_container(target_container)
                temp_container.id = "temp{}".format(len(map_target_container_to_temp) + 1)
                temp_container.name = temp_container.id
                map_target_container_to_temp[target_container] = temp_container

        for transfer in original_transfers:
            temp_target_container = map_target_container_to_temp[transfer.target_location.container]
            # TODO: Copy the source location rather than using the original?

            # Create a temporary analyte representing the new one on the temp plate:
            temp_analyte = copy.copy(transfer.source_location.artifact)
            temp_analyte.id += "-temp"
            temp_analyte.name += "-temp"
            temp_target_location = temp_target_container.set_well(
                transfer.target_location.position,
                temp_analyte)

            # TODO: This should be defined by a rule provided by the inheriting class
            static_sample_volume = 4
            static_buffer_volume = 36
            transfer_copy = SingleTransfer(transfer.source_conc,
                                           transfer.source_vol,
                                           transfer.source_conc / 10.0,
                                           static_buffer_volume + static_sample_volume,
                                           transfer.source_location,
                                           temp_target_location)
            transfer_copy.is_primary = False
            transfer_copy.split_type = SingleTransfer.SPLIT_BATCH
            transfer_copy.should_update_target_vol = False
            transfer_copy.should_update_target_conc = False

            # In this case, we'll hardcode the values according to the lab's specs:
            transfer_copy.pipette_sample_volume = static_sample_volume
            transfer_copy.pipette_buffer_volume = static_buffer_volume
            transfer_copy.original = transfer

            yield transfer_copy


class TransferCalcHandlerBase(TransferHandlerBase):
    """
    Base class for handlers that change the transfer in some way, in particular calculating values
    """
    __metaclass__ = abc.ABCMeta

    def handle_batch(self, transfer_batch, dilution_settings, robot_settings):
        """By default, run through the entire batch and call calc."""
        # TODO: Shouldn't transfer_batch be an iterator?
        for transfer in transfer_batch.transfers:
            self.handle_transfer(transfer, dilution_settings, robot_settings)

    def handle_transfer(self, transfer, dilution_settings, robot_settings):
        pass

    def _create_exceptions(self, msg, transfers, validation_type):
        if isinstance(transfers, list):
            for transfer in transfers:
                yield TransferValidationException(transfer, msg, validation_type)
        else:
            yield TransferValidationException(transfers, msg, validation_type)


class FixedVolumeCalcHandler(TransferCalcHandlerBase):
    """
    Implements sample volume calculations for transfer only dilutions.
    I.e. no calculations at all. The fixed transfer volume is specified in
    individual scripts
    """

    def handle_transfer(self, transfer, dilution_settings, robot_settings):
        """Only updates the source volume"""
        transfer.source_vol_delta = -round(transfer.pipette_sample_volume +
                                           robot_settings.dilution_waste_volume, 1)


class OneToOneConcentrationCalcHandler(TransferCalcHandlerBase):
    """
    Implements sample volume calculations for a one to one dilution,
    referring that a single transfer has a single source well/tube
    and a single destination well/tube, i.e. no pooling involved.
    Transfer volumes are calculated on basis of a requested target
    concentration and target volume by the user.
    """

    def handle_transfer(self, transfer, dilution_settings, robot_settings):
        if transfer.source_location.artifact.is_control:
            transfer.pipette_buffer_volume = transfer.target_vol
            return

        transfer.pipette_sample_volume = \
            transfer.target_conc * transfer.target_vol / float(transfer.source_conc)
        transfer.pipette_buffer_volume = \
            max(transfer.target_vol - transfer.pipette_sample_volume, 0)
        transfer.has_to_evaporate = \
            (transfer.target_vol - transfer.pipette_sample_volume) < 0

        # In the case of looped dilutions, we scale up on the temporary plate only
        # Scaling up is not needed on the regular case because it's covered by looping
        # TODO: To support more complex calculations and reuse of code, move this into a separate rule,
        # then apply rules in an order specified by the user (in the DilutionSettings).
        if transfer.transfer_batch.split and transfer.pipette_sample_volume < robot_settings.pipette_min_volume:
            scale_factor = robot_settings.pipette_min_volume / float(transfer.pipette_sample_volume)
            logging.debug("Before applying scale_factor '{}': {}".format(scale_factor, transfer))
            transfer.pipette_sample_volume *= scale_factor
            transfer.pipette_buffer_volume *= scale_factor
            transfer.scaled_up = True
            logging.debug("After applying scale_factor: {}".format(transfer))

        transfer.source_vol_delta = -round(transfer.pipette_sample_volume +
                                           robot_settings.dilution_waste_volume, 1)
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
