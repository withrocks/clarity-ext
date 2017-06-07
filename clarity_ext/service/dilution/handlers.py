import abc
import logging
import collections


class TransferHandlerBase(object):
    """Base class for all handlers"""
    __metaclass__ = abc.ABCMeta

    def __init__(self, dilution_session, dilution_settings, robot_settings, virtual_batch):
        self.dilution_session = dilution_session
        self.dilution_settings = dilution_settings
        self.robot_settings = robot_settings
        self.virtual_batch = virtual_batch
        self.validation_exceptions = list()
        self.logger = logging.getLogger(__name__)
        self.executed = False

    def tag(self):
        return None

    def run(self, transfer):
        """Called by the engine"""
        if not self.should_execute(transfer):
            return [transfer]
        ret = self.handle_transfer(transfer)
        ret = ret or transfer
        self.executed = True
        # After execution, we always return a list of transfers, since some handlers may split
        # transfers up
        if isinstance(ret, collections.Iterable):
            return ret
        else:
            return [ret]

    def handle_transfer(self, transfer):
        pass

    def should_execute(self, transfer):
        return True

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

    def _create_exceptions(self, msg, transfers, validation_type):
        if isinstance(transfers, list):
            for transfer in transfers:
                yield TransferValidationException(transfer, msg, validation_type)
        else:
            yield TransferValidationException(transfers, msg, validation_type)

    def __repr__(self):
        return self.__class__.__name__



class OneToOneConcentrationCalcHandler(TransferHandlerBase):
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


# TODO: Missing a test for this
class OneToOneFactorCalcHandler(TransferHandlerBase):
    """
    Transfer volumes are calculated on basis of a requested dilute
    factor and a target volume.
    Example:
        Target volume 10 ul
        Dilute factor = 10
        Take 1 part sample and 9 parts buffer, that sums up to 10 ul in target
    """
    def handle_transfer(self, transfer):
        transfer.pipette_sample_volume = transfer.target_vol/float(transfer.dilute_factor)
        transfer.pipette_buffer_volume = transfer.target_vol - transfer.pipette_sample_volume
        transfer.source_vol_delta = -round(transfer.pipette_sample_volume +
                                           self.robot_settings.dilution_waste_volume, 1)
        if transfer.source_conc is None:
            transfer.should_update_target_conc = False
        else:
            transfer.target_conc = transfer.source_conc/float(transfer.dilute_factor)
        transfer.pipette_sample_volume = round(transfer.pipette_sample_volume, 1)
        transfer.pipette_buffer_volume = round(transfer.pipette_buffer_volume, 1)





class PoolTransferCalcHandler(TransferHandlerBase):
    def handle_transfer(self, transfer):
        # TODO: Test with RE, some fundamental changes after refactoring
        # TODO: Since we need the average in this handler, we override handle_batch rather than handle_transfer
        for target, transfers in self.virtual_batch.transfers_by_output.items():
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
                                                   self.robot_settings.dilution_waste_volume, 1)
                if target_conc:
                    transfer.target_conc = target_conc
                self.logger.debug("Transfer after transform:  {}".format(transfer))


class TransferSplitHandlerBase(TransferHandlerBase):
    """Base class for handlers that can split one transfer into more"""
    __metaclass__ = abc.ABCMeta

    # TODO: Better naming so it's clear that this differs from the row-split
    def handle_split(self, transfer, temp_transfer, main_transfer):
        pass

    def run(self, transfer):
        if not self.should_execute(transfer):
            return None
        temp_transfer, main_transfer = self.dilution_session.split_transfer(transfer, self)
        temp_transfer.batch = self.tag()
        self.handle_split(transfer, temp_transfer, main_transfer)
        self.executed = True
        return [temp_transfer, main_transfer]


class FixedVolumeCalcHandler(TransferHandlerBase):
    """
    Implements sample volume calculations for transfer only dilutions.
    I.e. no calculations at all. The fixed transfer volume is specified in
    individual scripts
    """

    def handle_transfer(self, transfer):
        """Only updates the source volume"""
        transfer.source_vol_delta = -round(transfer.pipette_sample_volume +
                                           self.robot_settings.dilution_waste_volume, 1)

class OrTransferHandler(TransferHandlerBase):
    """A handler that stops executing the subhandlers when one of them succeeds"""
    def __init__(self, dilution_session, dilution_settings, robot_settings, virtual_batch, *sub_handlers):
        super(OrTransferHandler, self).__init__(dilution_session, dilution_settings, robot_settings, virtual_batch)
        self.sub_handlers = sub_handlers

    def run(self, transfer):
        evaluated = None
        for handler in self.sub_handlers:
            evaluated = handler.run(transfer)
            if evaluated and handler.executed:
                break
        return evaluated or [transfer]

    def __repr__(self):
        return "OR({})".format(", ".join(map(repr, self.sub_handlers)))

