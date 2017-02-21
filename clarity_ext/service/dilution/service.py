import abc
import copy
import codecs
from collections import namedtuple
from clarity_ext.utils import lazyprop
from clarity_ext.service.dilution.strategies import *
from jinja2 import Template
from clarity_ext.service.file_service import Csv
from clarity_ext.domain.validation import ValidationException, ValidationType, ValidationResults, UsageError
from clarity_ext import utils


class DilutionService(object):
    def __init__(self, validation_service):
        self.validation_service = validation_service

    def create_session(self, robots, dilution_settings, transfer_batch_handler, transfer_handler, transfer_validator):
        """
        Creates a DilutionSession based on the settings. Call evaluate to validate the entire session
        with a particular batch of objects.

        A DilutionSession contains several TransferBatch objects that need to be evaluated together
        """
        session = DilutionSession(self, robots, dilution_settings, transfer_batch_handler, transfer_handler,
                                  transfer_validator, self.validation_service)
        return session

    @staticmethod
    def create_strategy(dilution_settings, robot_settings):
        if dilution_settings.volume_calc_method == DilutionSettings.VOLUME_CALC_FIXED:
            return FixedVolumeCalc(dilution_settings, robot_settings)
        elif dilution_settings.volume_calc_method == DilutionSettings.VOLUME_CALC_BY_CONC and not dilution_settings.make_pools:
            return OneToOneConcentrationCalc(dilution_settings, robot_settings)
        elif dilution_settings.volume_calc_method == DilutionSettings.VOLUME_CALC_BY_CONC and dilution_settings.make_pools:
            return PoolConcentrationCalc(dilution_settings)
        else:
            raise ValueError("Volume calculation method is not implemented for these settings: '{}'".
                             format(dilution_settings))



class DilutionSession(object):
    """
    Encapsulates an entire dilution session, including validation of the dilution, generation of robot driver files
    and updating values.
    """

    def __init__(self, dilution_service, robots, dilution_settings, transfer_batch_handler, transfer_handler,
                 transfer_validator, validation_service):
        """
        Initializes a DilutionSession object for the robots.

        :param dilution_service: A service providing methods for creating batches
        :param robots: A list of RobotSettings objects
        :param dilution_settings: The list of settings to apply for the dilution
        :param transfer_batch_handler: A handler that support splitting TransferBatch objects
        :param transfer_handler: A handler that supports splitting SingleTransfer objects (row in a TransferBatch)
        :param transfer_validator: A validator that runs on an entire TransferBatch that has perhaps been split.
        :param validation_service: The service that handles the results of validation exceptions
        """
        self.dilution_service = dilution_service
        self.robot_settings_by_name = {robot.name: robot for robot in robots}
        self.dilution_settings = dilution_settings
        self.robot_settings = robots
        self._driver_files = dict()  # A dictionary of generated driver files
        self.validation_results = None
        self.transfer_batch_handler = transfer_batch_handler
        self.transfer_handler = transfer_handler
        self.transfer_validator = transfer_validator
        self.transfer_batches_by_robot = None
        self.pairs = None  # These are set on evaluation
        self.validation_service = validation_service

    def evaluate(self, pairs):
        """Refreshes all calculations for all registered robots and runs registered handlers and validators."""
        self.pairs = pairs
        self.transfer_batches_by_robot = dict()
        for robot_settings in self.robot_settings_by_name.values():
            self.transfer_batches_by_robot[robot_settings.name] = self.create_batches(
                self.pairs, self.dilution_settings, robot_settings, self.transfer_batch_handler,
                self.transfer_handler, self.transfer_validator)

    def create_batches(self, pairs, dilution_settings, robot_settings, transfer_batch_handler, transfer_handler,
                       transfer_validator):
        """
        Creates a batch and breaks it up if required by the validator
        """
        strategy = DilutionService.create_strategy(dilution_settings, robot_settings)

        # Create the "original transfer batch". This batch may be split up into other batches
        original_transfer_batch = self.create_batch(pairs, robot_settings,
                                                    dilution_settings, strategy)
        if transfer_batch_handler:
            transfer_batches = transfer_batch_handler.execute(original_transfer_batch, dilution_settings,
                                                              robot_settings, strategy)
        else:
            transfer_batches = [original_transfer_batch]

        for transfer_batch in transfer_batches:
            if transfer_handler:
                transfer_handler.execute(transfer_batch, dilution_settings, robot_settings)

            # Finally, run the validator on the transfer batch:
            if transfer_validator:
                results = transfer_validator.validate(transfer_batch, robot_settings, dilution_settings)
                self.validation_service.handle_validation(results)

        return transfer_batches

    def create_batch(self, pairs, robot_settings, dilution_settings, strategy):
        """
        Creates one batch (one-to-one relationship with a robot driver file) based on the input arguments.

        NOTE: The batch has not been validated in this call. Caller should validate.
        """
        if dilution_settings.volume_calc_method == DilutionSettings.VOLUME_CALC_FIXED:
            transfers = [self._create_fixed_transfer_from_pair(pair, dilution_settings)
                         for pair in pairs if self._should_include_pair(pair, dilution_settings)]
        else:
            # Get a list of SingleTransfer objects
            transfers = [SingleTransfer.create_from_analyte_pair(pair, dilution_settings.concentration_ref)
                         for pair in pairs if self._should_include_pair(pair, dilution_settings)]

        # Wrap the transfers in a TransferBatch object, it will do a basic validation on itself:
        # and raise a UsageError if it can't be used.
        batch = TransferBatch(transfers, name=robot_settings.name)

        # Based on the volume calculation strategy, calculate the volumes
        strategy.calculate_transfer_volumes(batch)
        return batch

    @staticmethod
    def _should_include_pair(pair, dilution_settings):
        return not pair.input_artifact.is_control or dilution_settings.include_control

    def _create_fixed_transfer_from_pair(self, pair, dilution_settings):
        transfer = SingleTransfer.create_from_analyte_pair_positions(pair)
        # Only interested in the sample volume in this case:
        transfer.source_vol = pair.input_artifact.udf_current_sample_volume_ul
        transfer.pipette_sample_volume = dilution_settings.fixed_sample_volume
        return transfer

    @lazyprop
    def container_mappings(self):
        # Returns a mapping of all containers we're diluting to/from
        container_pairs = set()
        for pair in self.pairs:
            container_pair = (pair.input_artifact.container, pair.output_artifact.container)
            container_pairs.add(container_pair)
        return list(container_pairs)

    @lazyprop
    def output_containers(self):
        """Returns a unique list of output containers involved in the dilution"""
        return list({output_container for _, output_container in self.container_mappings})

    def driver_files(self, robot_name):
        """Returns the driver file for the robot. Might be cached"""
        if robot_name not in self._driver_files:
            self._driver_files[robot_name] = list(self._create_robot_driver_files(robot_name))
        return self._driver_files[robot_name]

    def all_driver_files(self):
        """Returns all robot driver files in tuples (robot, robot_file)"""
        for robot_name in self.robot_settings_by_name:
            yield robot_name, self.driver_files(robot_name)

    def _create_robot_driver_files(self, robot_name):
        """Creates a csv for the robot"""
        robot_settings = self.robot_settings_by_name[robot_name]
        for transfer_batch in self.transfer_batches_by_robot[robot_name]:
            yield self._transfer_batch_to_robot_file(transfer_batch, robot_settings)

    def _transfer_batch_to_robot_file(self, transfer_batch, robot_settings):
        """
        Maps a transfer_batch to a robot file of type Csv. The transfer_batch is sorted and grouped as required.
        """
        csv = Csv(delim=robot_settings.delimiter, newline=robot_settings.newline)
        csv.set_header(robot_settings.header)
        sorted_transfers = sorted(transfer_batch.transfers, key=robot_settings.transfer_sort_key)

        for transfer in sorted_transfers:
            csv.append(robot_settings.map_transfer_to_row(transfer), transfer)
        return csv

    def create_general_driver_file(self, template_path, **kwargs):
        """
        Creates a driver file that has access to the DilutionSession object through the name `session`.
        """
        with open(template_path, 'r') as fs:
            text = fs.read()
            text = codecs.decode(text, "utf-8")
            template = Template(text)
            rendered = template.render(session=self, **kwargs)
            return rendered

    def enumerate_update_infos(self):
        """
        Returns the information that should be updated in the backend

        After the dilution has taken place, values should be updated in the backend:
         - Target conc. should be updated on the target analyte
         - Target vol. should be updated on the target analyte
         - Source vol. should be updated on the source analyte
        """
        for target_analyte, transfers in self.enumerate_transfers_by_target_analyte():
            # TODO: Yield a pair instead of source/target analyte
            if target_analyte.is_control:
                continue
            assert len(transfers) <= 2
            if len(transfers) == 1:
                transfer = transfers[0]
                yield (transfer.source_analyte, transfer.target_analyte,
                       UpdateInfo(transfer.target_conc, transfer.target_vol, transfer.updated_source_vol))
            else:
                temp_transfer = utils.single([t for t in transfers if t.transfer_batch.is_temporary])
                transfer = utils.single([t for t in transfers if not t.transfer_batch.is_temporary])
                yield (transfer.source_analyte, transfer.target_analyte,
                       UpdateInfo(transfer.target_conc, transfer.target_vol, temp_transfer.updated_source_vol))

    def enumerate_transfers_by_target_analyte(self):
        """
        Returns the transfers (for only one of the robots), including the information needed
        to update the analyte after dilution.

        Returns (source, target, [list of associated transfers])

        This information differs based on if the TransferBatches were split or not
        """
        transfer_batches = self.single_robot_transfer_batches_for_update()
        assert len(transfer_batches) <= 2, "Expecting max 2 transfer batches"

        if len(transfer_batches) == 1:
            for transfer in transfer_batches[0].transfers:
                yield transfer.target_analyte, [transfer]
        else:
            temp_transfer_batch = utils.single([batch for batch in transfer_batches if batch.is_temporary])
            non_temp_transfer_batch = utils.single([batch for batch in transfer_batches if not batch.is_temporary])

            ret = dict()
            for transfer in temp_transfer_batch.transfers:
                ret.setdefault(transfer.target_analyte, list())
                ret[transfer.target_analyte].append(transfer)
            for transfer in non_temp_transfer_batch.transfers:
                ret.setdefault(transfer.target_analyte, list())
                ret[transfer.target_analyte].append(transfer)

            for key, value in ret.items():
                yield key, value

    def single_robot_transfer_batches_for_update(self):
        """
        Helper method that returns the first robot transfer batch, but validates first that
        the updated_source_vol is the same on both. This supports the use case where the
        user doesn't have to tell us which robot driver file they used, because the results will be the same.
        """
        all_robots = self.transfer_batches_by_robot.items()
        candidate_name, candidate_batches = all_robots[0]

        for current_name, current_batches in all_robots[1:]:
            # Validate that the requirement holds:

            # Both need to have the same number of transfer batches:
            if len(candidate_batches) != len(current_batches):
                raise Exception("Can't select a single robot for update. Different number of batches between {} and {}".
                                format(candidate_name, current_name))

            # For each transfer in the candidate, we must have a corresponding transfer in
            # the current having the same update_source_vol. Other values can be different (e.g.
            # sort order, plate names on robots etc.)
            # TODO: This assumes that the transfer batches are always in the same order, which they should,
            # but it would make sense to enforce the order before this.
            for candidate_batch, current_batch in zip(candidate_batches, current_batches):
                if len(candidate_batch.transfers) != len(current_batch.transfers):
                    raise Exception("Number of transfers differ between {} and {}".format(candidate_name, current_name))
                for candidate_transfer in candidate_batch.transfers:
                    if candidate_transfer.updated_source_vol == 0:
                        continue
                    current_transfer = utils.single([t for t in current_batch.transfers
                                                     if t.source_analyte == candidate_transfer.source_analyte and
                                                     t.updated_source_vol != 0])
                    # Now, we require only that the updated source volume must be the same between the two, other
                    # values may differ. If there is a difference, the caller will have to have the
                    # user select which driver file was actually used before updating based on this session object
                    if candidate_transfer.updated_source_vol != current_transfer.updated_source_vol:
                        raise Exception("Inconsistent updated_source_vol between two transfers. It's not possible to "
                                        "infer the source volume without explicitly selecting the robot that was used.")
        return candidate_batches

    def enumerate_transfers_for_update(self):
        """
        Returns the transfers that require an update. Supports both the case of both regular dilution and "looped"
        (i.e. when we have more than one driver file). Relies on there being only one robot or that they are consistent
        in the reported updated_source_vol.
        """
        transfer_batches = self.single_robot_transfer_batches_for_update()
        for transfer_batch in transfer_batches:
            for transfer in transfer_batch.transfers:
                yield transfer  # TODO!


class TransferLocation(object):
    """
    Represents either a source or a target of a transfer
    """

    def __init__(self, container, position, well):
        """
        :param container: A Container that holds the analyte to be transferred
        :param position: The ContainerPosition pointing to a position within the container, e.g. A:1
        """
        self.container = container
        self.position = position
        self.well = well  # TODO: Use only well, not position

        # This is not set by the user but by the TransferBatch when the user queries for the object.
        self.container_pos = None

    @staticmethod
    def create_from_analyte(analyte):
        return TransferLocation(analyte.container, analyte.well.position, analyte.well)

    def __repr__(self):
        return "{}({})@{}".format(self.container.name, self.container_pos, self.position)


class SingleTransfer(object):
    """
    Encapsulates a single transfer between two positions:
      * From where: source (TransferEndpoint)
      * To where: target (TransferEndpoint)
      * How much sample volume and buffer volume are needed (in the robot file)
      * Other metadata that will be used for warnings etc, e.g. has_to_evaporate/scaled_up etc.
    """

    def __init__(self, source_conc, source_vol, target_conc, target_vol, source_location, target_location,
                 source_analyte, target_analyte):
        self.source_conc = source_conc
        self.source_vol = source_vol
        self.target_conc = target_conc
        self.target_vol = target_vol

        self.source_location = source_location
        self.target_location = target_location

        # The calculated values:
        self.pipette_sample_volume = 0
        self.pipette_buffer_volume = 0

        # Meta values for warnings etc.
        self.has_to_evaporate = None
        self.scaled_up = False

        # In the case of temporary transfers, we keep a pointer to the original for easier calculations
        self.original = None
        self.source_analyte = source_analyte
        self.target_analyte = target_analyte
        self.updated_source_vol = None

        # The TransferBatch takes care of marking the transfer as being a part of it
        self.transfer_batch = None

    @property
    def pipette_total_volume(self):
        return self.pipette_buffer_volume + self.pipette_sample_volume

    @staticmethod
    def _referenced_concentration(analyte=None, concentration_ref=None):
        if concentration_ref == DilutionSettings.CONCENTRATION_REF_NGUL:
            return analyte.udf_conc_current_ngul
        elif concentration_ref == DilutionSettings.CONCENTRATION_REF_NM:
            return analyte.udf_conc_current_nm
        else:
            raise NotImplementedError("Concentration ref {} not implemented".format(
                concentration_ref))

    @staticmethod
    def _referenced_requested_concentration(analyte=None, concentration_ref=None):
        if concentration_ref == DilutionSettings.CONCENTRATION_REF_NGUL:
            return analyte.udf_target_conc_ngul
        elif concentration_ref == DilutionSettings.CONCENTRATION_REF_NM:
            return analyte.udf_target_conc_nm
        else:
            raise NotImplementedError("Concentration ref {} not implemented".format(
                concentration_ref))

    @staticmethod
    def create_from_analyte_pair_positions(pair):
        """
        Creates a transfer based on the analyte pair, but is only conserned with the positions, i.e.
        ignores all UDFs etc.
        """
        source_location = TransferLocation.create_from_analyte(pair.input_artifact)
        target_location = TransferLocation.create_from_analyte(pair.output_artifact)
        transfer = SingleTransfer(
            None, None, None, None, source_location, target_location, pair.input_artifact, pair.output_artifact)
        return transfer

    @classmethod
    def create_from_analyte_pair(cls, pair, concentration_ref):
        single_transfer = cls.create_from_analyte_pair_positions(pair)

        def raise_target_measurements_missing(artifact_pair):
            raise UsageError("You need to provide target volume and concentration for all samples. "
                             "Missing for {}.".format(artifact_pair.output_artifact.id))

        # Now fill in with the UDF measurements
        if pair.input_artifact.is_control:
            try:
                # The transfer for controls does only require target volume. Other values will be ignored.
                single_transfer.target_vol = pair.output_artifact.udf_target_vol_ul
            except AttributeError:
                raise_target_measurements_missing(pair)
            return single_transfer

        try:
            single_transfer.source_conc = cls._referenced_concentration(pair.input_artifact, concentration_ref)
            single_transfer.source_vol = pair.input_artifact.udf_current_sample_volume_ul
            single_transfer.target_conc = cls._referenced_requested_concentration(pair.output_artifact,
                                                                                  concentration_ref)
            single_transfer.target_vol = pair.output_artifact.udf_target_vol_ul
        except AttributeError:
            raise_target_measurements_missing(pair)
        single_transfer.pair = pair  # TODO: Both setting the pair and source target!
        return single_transfer

    def identifier(self):
        source = "source({}, conc={})".format(
            self.source_well, self.source_concentration)
        target = "target({}, conc={}, vol={})".format(self.target_well,
                                                      self.requested_concentration, self.requested_volume)
        return "{} => {}".format(source, target)

    def __repr__(self):
        return "<SingleTransfer {}({},{})=>{}({},{})>".format(self.source_location,
                                                              self.source_conc,
                                                              self.source_vol,
                                                              self.target_location,
                                                              self.target_conc,
                                                              self.target_vol)


# Represents source conc/vol, target conc/vol as one unit. TODO: Better name
DilutionMeasurements = namedtuple('DilutionMeasurements', ['source_conc', 'source_vol', 'target_conc', 'target_vol'])
UpdateInfo = namedtuple("UpdateInfo", ['target_conc', 'target_vol', 'updated_source_vol'])


class DilutionSettings:
    """Defines the rules for how a dilution should be performed"""
    CONCENTRATION_REF_NGUL = 1
    CONCENTRATION_REF_NM = 2
    VOLUME_CALC_FIXED = 1
    VOLUME_CALC_BY_CONC = 2

    CONCENTRATION_REF_TO_STR = {
        CONCENTRATION_REF_NGUL: "ng/ul",
        CONCENTRATION_REF_NM: "nM"
    }

    def __init__(self, scale_up_low_volumes=False, concentration_ref=None, include_blanks=False,
                 volume_calc_method=None, make_pools=False, fixed_sample_volume=None):
        """
        :param dilution_waste_volume: Extra volume that should be subtracted from the sample volume
        to account for waste during dilution
        """
        self.scale_up_low_volumes = scale_up_low_volumes
        # TODO: Use py3 enums instead
        if concentration_ref is not None:
            concentration_ref = self._parse_conc_ref(concentration_ref)
            if concentration_ref not in [self.CONCENTRATION_REF_NM, self.CONCENTRATION_REF_NGUL]:
                raise ValueError("Unsupported concentration_ref '{}'".format(concentration_ref))
        self.concentration_ref = concentration_ref
        # TODO: include_blanks, has that to do with output only? If so, it should perhaps be in RobotSettings
        self.include_blanks = include_blanks
        self.volume_calc_method = volume_calc_method
        self.make_pools = make_pools
        self.include_control = True
        self.fixed_sample_volume = fixed_sample_volume

    @staticmethod
    def _parse_conc_ref(concentration_ref):
        if isinstance(concentration_ref, basestring):
            for key, value in DilutionSettings.CONCENTRATION_REF_TO_STR.items():
                if value.lower() == concentration_ref.lower():
                    return key
        else:
            return concentration_ref

    @staticmethod
    def concentration_unit_to_string(conc_ref):
        return DilutionSettings.CONCENTRATION_REF_TO_STR[conc_ref]


class RobotSettings(object):
    __metaclass__ = abc.ABCMeta

    def __init__(self):
        """
        Inherit from this file to supply new settings for a robot
        """
        self.name = None
        self.file_handle = None
        self.newline = None
        self.file_ext = None
        self.delimiter = None
        self.dilution_waste_volume = None
        self.pipette_min_volume = None
        self.pipette_max_volume = None
        self.max_pipette_vol_for_row_split = None

    @abc.abstractmethod
    def map_transfer_to_row(self, transfer):
        """
        Describes how to transform a SingleTransfer object to a csv row
        :return:
        """
        pass

    @abc.abstractmethod
    def get_index_from_well(self, well):
        """Returns the numerical index of the well"""
        pass

    @staticmethod
    def source_container_name(transfer_location):
        return "DNA{}".format(transfer_location.container_pos)

    @staticmethod
    def target_container_name(transfer_location):
        return "END{}".format(transfer_location.container_pos)

    @staticmethod
    def transfer_sort_key(transfer):
        """
        Sort the transfers based on:
            - source position (container_pos)
            - well index (down first)
            - pipette volume (descending)
        """
        assert transfer.source_location.container_pos is not None
        assert transfer.source_location.well is not None
        return (transfer.source_location.container_pos,
                transfer.source_location.well.index_down_first,
                -transfer.pipette_total_volume)

    def __repr__(self):
        return "<RobotSettings {}>".format(self.name)

    def __str__(self):
        return "<RobotSettings {name} file_ext='{file_ext}' file_handle='{file_handle}'>".format(
            name=self.name,
            file_handle=self.file_handle,
            file_ext=self.file_ext)


class TransferBatchHandlerBase(object):
    __metaclass__ = abc.ABCMeta

    def __init__(self, validation_service):
        self.validation_service = validation_service

    @abc.abstractmethod
    def needs_split(self, transfer, dilution_settings, robot_settings):
        pass

    def execute(self, transfer_batch, dilution_settings, robot_settings, strategy):
        """
        Returns one or two transfer_batches, based on rules. Can be used to split a transfer_batch into original and
        temporary transfer_batches
        """
        split = [t for t in transfer_batch.transfers if self.needs_split(t, dilution_settings, robot_settings)]
        no_split = [t for t in transfer_batch.transfers if t not in split]

        for transfer in split:
            # This may look strange, but for now we want to handle this like a validation exception
            val = NeedsBatchSplit(transfer)
            self.validation_service.handle_single_validation(val)

        if len(split) > 0:
            return self.split_transfer_batch(split, no_split, strategy)
        else:
            # No split was required
            return [transfer_batch]

    def split_transfer_batch(self, split, no_split, strategy):
        first_transfers = list(self.calculate_split_transfers(split))
        temp_transfer_batch = TransferBatch(first_transfers, depth=1, is_temporary=True)
        strategy.calculate_transfer_volumes(temp_transfer_batch)
        second_transfers = list()

        # We need to create a new transfers list with:
        #  - target_location should be the original target location
        #  - source_location should also bet the original source location
        for temp_transfer in temp_transfer_batch.transfers:
            # NOTE: It's correct that the source_location is being set to the target_location here:
            new_transfer = SingleTransfer(temp_transfer.target_conc, temp_transfer.target_vol,
                                          temp_transfer.original.target_conc, temp_transfer.original.target_vol,
                                          source_location=temp_transfer.target_location,
                                          target_location=temp_transfer.original.target_location,
                                          source_analyte=temp_transfer.source_analyte,
                                          target_analyte=temp_transfer.target_analyte)
            second_transfers.append(new_transfer)

        # Add other transfers just as they were:
        second_transfers.extend(no_split)

        final_transfer_batch = TransferBatch(second_transfers, depth=1)
        strategy.calculate_transfer_volumes(final_transfer_batch)

        # For the analytes requiring splits
        return [temp_transfer_batch, final_transfer_batch]

    def calculate_split_transfers(self, original_transfers):
        # For each target well, we need to push this to a temporary plate:
        from clarity_ext.domain import Container

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
            temp_target_location = TransferLocation(temp_target_container, transfer.target_location.position,
                                                    transfer.target_location.well)

            # TODO: This should be defined by a rule provided by the inheriting class
            static_sample_volume = 4
            static_buffer_volume = 36
            transfer_copy = SingleTransfer(transfer.source_conc,
                                           transfer.source_vol,
                                           transfer.source_conc / 10.0,
                                           static_buffer_volume + static_sample_volume,
                                           transfer.source_location,
                                           temp_target_location,
                                           transfer.source_analyte,
                                           transfer.target_analyte)

            # In this case, we'll hardcode the values according to the lab's specs:
            transfer_copy.pipette_sample_volume = static_sample_volume
            transfer_copy.pipette_buffer_volume = static_buffer_volume
            transfer_copy.original = transfer
            yield transfer_copy


class TransferHandlerBase(object):
    __metaclass__ = abc.ABCMeta

    def __init__(self, validation_service):
        self.validation_service = validation_service

    @abc.abstractmethod
    def needs_row_split(self, transfer, dilution_settings, robot_settings):
        pass

    @abc.abstractmethod
    def split_single_transfer(self, transfer, robot_settings):
        pass

    def execute(self, transfer_batch, dilution_settings, robot_settings):
        require_row_split = [t for t in transfer_batch.transfers
                             if self.needs_row_split(t, dilution_settings, robot_settings)]
        for transfer in require_row_split:
            # TODO: Looks weird to push the action to the validation handler like this, but fits right now.
            val = NeedsRowSplit(transfer)
            self.validation_service.handle_single_validation(val)

        for transfer in require_row_split:
            split_transfers = self.split_single_transfer(transfer, robot_settings)
            transfer_batch.transfers.remove(transfer)
            transfer_batch.transfers.extend(split_transfers)


class DilutionValidatorBase(object):
    """
    Validates transfer objects that are to be diluted. Inherit from this object to support behavior
    different from the default.
    """

    @staticmethod
    def error(msg):
        return TransferValidationException(None, msg, ValidationType.ERROR)

    @staticmethod
    def warning(msg):
        return TransferValidationException(None, msg, ValidationType.WARNING)

    def rules(self, transfer, robot_settings, dilution_settings):
        """
        Validates that the transfer is correct. Will not run if `can_start_calculation`
        returns False.

        This should be overridden to provide custom validation rules for a particular dilution.
        """
        return []

    def _group_by_type(self, results):
        def by_type(result):
            return result.type

        ret = dict()
        for k, g in groupby(sorted(results, key=by_type), key=by_type):
            ret[k] = list(g)
        return ret.get(ValidationType.ERROR, list()), ret.get(ValidationType.WARNING, list())

    def validate(self, transfer_batch, robot_settings, dilution_settings):
        """
        Validates the transfers, first by validating that calculation can be performed, then by
        running all custom validations.

        Returns a tuple of (errors, warnings).
        """
        results = ValidationResults()
        for transfer in transfer_batch._transfers:
            validation_exceptions = list(self.rules(transfer, robot_settings, dilution_settings))
            for exception in validation_exceptions:
                if not exception.transfer:
                    exception.transfer = transfer
            results.extend(validation_exceptions)
        return results


class TransferValidationException(ValidationException):
    """Wraps a validation exception for Dilution transfer objects"""

    def __init__(self, transfer, msg, result_type=ValidationType.ERROR):
        super(TransferValidationException, self).__init__(msg, result_type)
        self.transfer = transfer

    def __repr__(self):
        return "{}: {} transfer ({}@{} => {}@{}) - {}".format(
            self._repr_type(),
            self.transfer.transfer_batch.name,
            self.transfer.source_location.position, self.transfer.source_location.container.id,
            self.transfer.target_location.position, self.transfer.target_location.container.id,
            self.msg)


class TransferBatch(object):
    """
    Encapsulates a list of SingleTransfer objects. Used to generate robot driver files.
    """

    def __init__(self, transfers, depth=0, is_temporary=False, name=None):
        self.depth = depth
        self.is_temporary = is_temporary  # temp dilution, no plate will actually be saved.
        self.validation_results = list()
        self._set_transfers(transfers)
        self.name = name

    def _set_transfers(self, transfers):
        self._transfers = transfers
        self._set_container_refs()
        for transfer in transfers:
            transfer.transfer_batch = self

    def _set_container_refs(self):
        """
        Updates the container refs (DNA1, END1 etc) for each container in the transfer.
        These depend on the state of the entire batch, so they need to be updated
        if the transfers are updated.
        """
        self._container_to_container_ref = self._evaluate_container_refs()

        for transfer in self._transfers:
            # First get the index being used
            transfer.source_location.container_pos = self._container_to_container_ref[
                transfer.source_location.container]
            transfer.target_location.container_pos = self._container_to_container_ref[
                transfer.target_location.container]

    def _evaluate_container_refs(self):
        """
        Figures out the mapping from container to container refs
        (first input container => 1)
        (first output container => 1) etc

        The containers are sorted by (not is_temporary, id)

        NOTE: This assumes that containers having the same ID point to the same domain object.
        """

        def indexed_containers(all_containers):
            """Returns a map from the containers to an index. The containers are sorted by id"""
            return [(container, ix + 1) for ix, container in enumerate(
                sorted(all_containers, key=lambda x: (not x.is_temporary, x.id)))]

        container_to_container_ref = dict()
        container_to_container_ref.update(indexed_containers(
            set(transfer.source_location.container for transfer in self._transfers)))
        container_to_container_ref.update(indexed_containers(
            set(transfer.target_location.container for transfer in self._transfers)))
        return container_to_container_ref

    @property
    def transfers(self):
        """
        Enumerates the transfers. The underlying transfer list is sorted by self._transfer_sort_key
        and row split is performed if needed
        """
        return self._transfers

    def validate(self, validator, robot_settings, dilution_settings):
        # Run the validator on the object and save the results on the object.
        if validator:
            self.validation_results.extend(validator.validate(self, robot_settings, dilution_settings))

    def report(self):
        """
        Creates a detailed report of what's included in the transfer, for debug learning purposes.
        """
        report = list()
        report.append("TransferBatch:")
        report.append("-" * len(report[-1]))
        report.append(" - temporary: {}".format(self.is_temporary))
        for transfer in self._transfers:
            report.append("{}".format(transfer))
        return "\n".join(report)


class NeedsBatchSplit(TransferValidationException):
    def __init__(self, transfer):
        super(NeedsBatchSplit, self).__init__(transfer, "The transfer requires a split into another batch",
                                              ValidationType.WARNING)


class NeedsRowSplit(TransferValidationException):
    def __init__(self, transfer):
        super(NeedsRowSplit, self).__init__(transfer, "The transfer requires a row split", ValidationType.WARNING)


# TODO: Use NeedsBatchSplit with "reason"?
class NeedsEvaporation(TransferValidationException):
    def __init__(self):
        super(NeedsEvaporation, self).__init__(None, "The transfer requires evaporation", ValidationType.WARNING)
