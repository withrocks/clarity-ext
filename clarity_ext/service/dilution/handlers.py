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


class TransferBatchHandlerBase(TransferHandlerBase):
    __metaclass__ = abc.ABCMeta

    @abc.abstractmethod
    def needs_split(self, transfer, dilution_settings, robot_settings):
        """Returns True if the transfer requires a split"""
        pass

    @abc.abstractmethod
    def temp_container_prefix(self):
        pass

    @abc.abstractmethod
    def split_transfer(self, transfer, temp_target_container):
        """
        Given the original transfer, returns a pair of transfers, one that needs to go to a temporary plate
        and the one that takes from the temporary plate to the end destination
        """
        pass

    def should_calculate(self):
        """Returns true if the calculation handlers should be run after the split"""
        return False


class TransferCalcHandlerBase(TransferHandlerBase):
    """
    Base class for handlers that change the transfer in some way, in particular calculating values
    """
    __metaclass__ = abc.ABCMeta

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
