import abc
import copy
import logging
from itertools import groupby
import collections
from collections import namedtuple
from clarity_ext.service.file_service import Csv
from clarity_ext.domain.validation import ValidationException, ValidationType, ValidationResults, UsageError
from clarity_ext import utils
from clarity_ext.domain import Container, Well


class DilutionService(object):
    def __init__(self, validation_service, logger=None):
        self.validation_service = validation_service
        self.logger = logger or logging.getLogger(__name__)

    def create_session(self, robots, dilution_settings, transfer_batch_handler_type, transfer_split_handler_type,
                       transfer_validator, context, transfer_calc_handler_types):
        """
        Creates a DilutionSession based on the settings. Call evaluate to validate the entire session
        with a particular batch of objects.

        A DilutionSession contains several TransferBatch objects that need to be evaluated together
        """
        session = DilutionSession(self, robots, dilution_settings, transfer_batch_handler_type,
                                  transfer_split_handler_type, transfer_validator, self.validation_service,
                                  context, transfer_calc_handler_types)
        return session


class DilutionSession(object):
    """
    Encapsulates an entire dilution session, including validation of the dilution, generation of robot driver files
    and updating values.
    """

    def __init__(self, dilution_service, robots, dilution_settings, transfer_batch_handler_types,
                 transfer_split_handler_type, transfer_validator, validation_service, context,
                 transfer_handler_types, logger=None):
        """
        Initializes a DilutionSession object for the robots.

        :param dilution_service: A service providing methods for creating batches
        :param robots: A list of RobotSettings objects
        :param dilution_settings: The list of settings to apply for the dilution
        :param transfer_batch_handlers: Handlers that split the transfers into several transfers
        :param transfer_handler: A handler that supports splitting SingleTransfer objects (row in a TransferBatch)
        :param transfer_validator: A validator that runs on an entire TransferBatch that has perhaps been split.
        :param validation_service: The service that handles the results of validation exceptions
        :param context: The context the session is being created in
        :param transfer_handler_types: A list of handlers that transform the SingleTransfer objects
        """
        self.dilution_service = dilution_service
        self.robot_settings_by_name = {robot.name: robot for robot in robots}
        self.dilution_settings = dilution_settings
        self.robot_settings = robots
        self._driver_files = dict()  # A dictionary of generated driver files
        self.validation_results = None
        self.transfer_validator = transfer_validator
        self.transfer_batches_by_robot = None
        self.pairs = None  # These are set on evaluation
        self.validation_service = validation_service
        self.context = context
        self.logger = logger or logging.getLogger(__name__)

        self.transfer_batch_handlers = [t(self) for t in transfer_batch_handler_types]
        self.transfer_split_handler = transfer_split_handler_type(self) if transfer_split_handler_type else None

        # TODO: Handlers evaluated before execution, and push in robot_settings!
        self.transfer_handler_types = list(transfer_handler_types)
        self.map_temporary_container_by_original = dict()

    def evaluate(self, pairs):
        """Refreshes all calculations for all registered robots and runs registered handlers and validators."""
        self.pairs = pairs
        self.transfer_batches_by_robot = dict()
        for robot_settings in self.robot_settings_by_name.values():
            transfer_handlers = self.init_transfer_handlers(self.transfer_handler_types,
                                                            self.dilution_settings, robot_settings)
            self.transfer_batches_by_robot[robot_settings.name] = self.create_batches(
                self.pairs, self.dilution_settings, robot_settings,
                self.transfer_validator, transfer_handlers)

    def init_transfer_handlers(self, transfer_handle_types, dilution_settings, robot_settings):
        """
        Initializes the transfer handlers for a particular robot and dilution settings

        Lists of handlers are implicitly understood to be short-circuited ORs.
        """
        handlers = list()
        for transfer_handle_type in transfer_handle_types:
            if isinstance(transfer_handle_type, collections.Iterable):
                transfer_handlers = [t(self, dilution_settings, robot_settings) for t in transfer_handle_type]
                handlers.append(OrTransferHandler(self, dilution_settings, robot_settings, *transfer_handlers))
            else:
                handlers.append(transfer_handle_type(self, dilution_settings, robot_settings))
        return handlers

    def _log_handler(self, handler, transfer):
        self.logger.debug("Executing handler '{}' for transfer'{}'".format(
            type(handler).__name__, transfer))

    def execute_handler(self, handler, transfers):
        """Executes the handler on all transfers at the current level in the path.

        Returns a list of evaluated transfers, perhaps unchanged.
        """
        evaluated = list()
        for transfer in transfers:
            self._log_handler(handler, transfer)
            print "HERE", handler
            evaluated.extend(handler.run(transfer))
        return evaluated

    def get_temporary_container(self, target_container, prefix):
        """Returns the temporary container that should be used rather than the original one, when splitting transfers"""
        if target_container.id not in self.map_temporary_container_by_original:
            temp_container = Container.create_from_container(target_container)
            temp_container.id = "{}{}".format(prefix, len(self.map_temporary_container_by_original) + 1)
            temp_container.name = temp_container.id
            self.map_temporary_container_by_original[target_container.id] = temp_container
        return self.map_temporary_container_by_original[target_container.id]

    def split_transfer(self, transfer, handler):
        """Returns two transfers where the first one will go on a temporary container"""
        # TODO: Base class
        temp_transfer = SingleTransfer(0, 0, 0, 0, None, None)
        main_transfer = SingleTransfer(0, 0, 0, 0, None, None)

        temp_transfer.is_primary = False
        temp_transfer.split_type = SingleTransfer.SPLIT_BATCH
        temp_transfer.should_update_target_vol = False
        temp_transfer.should_update_target_conc = False
        temp_transfer.original = transfer

        temp_analyte = copy.copy(transfer.source_location.artifact)
        tag = handler.tag()
        temp_analyte.id += "-" + tag
        temp_analyte.name += "-" + tag

        temp_target_container = self.get_temporary_container(transfer.target_location.container, tag)
        temp_transfer.source_location = transfer.source_location
        temp_transfer.target_location = temp_target_container.set_well(transfer.target_location.position, temp_analyte)

        # Main:
        main_transfer.source_location = temp_transfer.target_location
        main_transfer.target_location = transfer.target_location

        return temp_transfer, main_transfer

    def split_rows(self, row_split_handler, transfer_batch, dilution_settings, robot_settings):
        if not row_split_handler:
            return

        require_row_split = [t for t in transfer_batch.transfers
                             if row_split_handler.needs_row_split(t, dilution_settings, robot_settings)]
        for transfer in require_row_split:
            split_transfers = row_split_handler.split_single_transfer(transfer, robot_settings)
            # TODO: Validation to handler?
            if sum(t.pipette_sample_volume + t.pipette_buffer_volume for t in split_transfers) > \
                    robot_settings.max_pipette_vol_for_row_split:
                raise UsageError("Total volume has reached the max well volume ({})".format(
                    robot_settings.max_pipette_vol_for_row_split))
            transfer_batch.transfers.remove(transfer)
            transfer_batch.transfers.extend(split_transfers)

    def evaluate_transfer(self, transfer, transfer_handlers):
        """Runs the calculation handlers on the transfer, returning a list of one or two transfers (if split)"""
        evaluated = [transfer]
        for handler in transfer_handlers:
            #print "EVAL before", handler, evaluated
            evaluated = self.execute_handler(handler, evaluated)
            #print "     after", evaluated
        return evaluated

    def create_batches(self, pairs, dilution_settings, robot_settings, transfer_validator, transfer_handlers):
        transfers = self.create_transfers_from_pairs(pairs, robot_settings, dilution_settings)
        evaluated_transfers = list()

        # Evaluate the transfers, i.e. execute all handlers. This does not group them into transfer batches yet
        for transfer in transfers:
            # Executing the handler on the transfer can result in several transfers
            evaluated = self.evaluate_transfer(transfer, transfer_handlers)
            evaluated_transfers.extend(evaluated)
        print evaluated_transfers

        # Now group all evaluated transfers together into a batch
        transfer_by_batch = dict()

        for transfer in evaluated_transfers:
            batch = transfer.batch or "default"
            transfer_by_batch.setdefault(batch, list())
            transfer_by_batch[batch].append(transfer)

        transfer_batches = TransferBatchCollection()
        for key in transfer_by_batch:
            depth = 0 if key == "default" else 1  # TODO Used?
            is_temporary = key != "default"  # and this?
            transfer_batches.append(TransferBatch(transfer_by_batch[key], robot_settings, depth, is_temporary, key))

        # Run the validator on each transfer batch:
        for transfer_batch in transfer_batches:
            if transfer_validator:
                results = transfer_validator.validate(transfer_batch, robot_settings, dilution_settings)
                self.validation_service.handle_validation(results)

        for ix, transfer_batch in enumerate(transfer_batches):
            # Evaluate CSVs:
            csv = Csv(delim=robot_settings.delimiter, newline=robot_settings.newline)
            csv.file_name = robot_settings.get_filename(csv, self.context, ix)
            csv.set_header(robot_settings.header)
            sorted_transfers = sorted(transfer_batch.transfers, key=robot_settings.transfer_sort_key)
            for transfer in sorted_transfers:
                if robot_settings.include_transfer_in_output(transfer):
                    csv.append(robot_settings.map_transfer_to_row(transfer), transfer)
            transfer_batch.driver_file = csv

        for key, driver_file in transfer_batches.driver_files.items():
            print key
            print driver_file.to_string()

        return transfer_batches

    def create_transfers_from_pairs(self, pairs, robot_settings, dilution_settings):
        """
        Creates the original transfer objects from the pairs

        Runs only the pre-validation (which usually ensures that the user provided conc/vol)

        Does not validate the transfers in another way or run the calculations.
        """
        # NOTE: The original containers are copied, so the containers in the transfer batch can be modified at will
        containers = dict()
        # First ensure that we've taken copies of the original containers, since we want to be able to move
        # the artifacts to different wells, it's cleaner to do that in a copied container:
        original_containers = set()
        original_containers.update([pair.input_artifact.container for pair in pairs])
        original_containers.update([pair.output_artifact.container for pair in pairs])
        for original_container in original_containers:
            containers[original_container.id] = copy.copy(original_container)

        def create_well(artifact):
            return Well(artifact.well.position,
                        containers[artifact.container.id],
                        artifact)

        transfers = list()

        # First, get a list of SingleTransfer objects, pointing to Well objects in copies of the original
        for pair in pairs:
            source_well = create_well(pair.input_artifact)
            target_well = create_well(pair.output_artifact)
            transfers.append(SingleTransfer(None, None, None, None, source_well, target_well))

        for transfer in transfers:
            self.initialize_transfer_from_settings(transfer, dilution_settings)

        if self.transfer_validator:
            pre_results = self.transfer_validator.pre_validate(transfers, dilution_settings, robot_settings)
            self.validation_service.handle_validation(pre_results)

        return transfers

    def initialize_transfer_from_settings(self, transfer, dilution_settings):
        # TODO: Handler
        if dilution_settings.volume_calc_method == DilutionSettings.VOLUME_CALC_FIXED:
            transfer.source_vol = transfer.source_location.artifact.udf_current_sample_volume_ul
            transfer.pipette_sample_volume = dilution_settings.fixed_sample_volume
        else:
            # Get a list of SingleTransfer objects
            SingleTransfer.initialize_transfer(transfer, dilution_settings.concentration_ref)

    @staticmethod
    def _should_include_pair(pair, dilution_settings):
        return not pair.input_artifact.is_control or dilution_settings.include_control

    def transfer_batches(self, robot_name):
        """Returns the driver file for the robot. Might be cached"""
        return self.transfer_batches_by_robot[robot_name]

    def all_driver_files(self):
        """Returns all robot driver files in tuples (robot, robot_file)"""
        for robot_name in self.robot_settings_by_name:
            yield robot_name, self.driver_files(robot_name)

    def update_infos_by_target_analyte(self, transfer_batches):
        """
        Returns the information that should be updated in the backend

        After the dilution has taken place, values should be updated in the backend:
         - Target conc. should be updated on the target analyte
         - Target vol. should be updated on the target analyte
         - Source vol. should be updated on the source analyte
        """
        for target, transfers in self.group_transfers_by_target_analyte(transfer_batches).items():
            # TODO: The `is_pooled` check is a quick-fix.
            if target.is_pool and self.dilution_settings.is_pooled:
                regular_transfers = [t for t in transfers if not t.source_location.artifact.is_control]
                source_vol_delta = list(set(t.source_vol_delta for t in regular_transfers
                                            if t.should_update_source_vol))
                # We assume the same delta for all samples in the pool:
                source_vol_delta = utils.single(source_vol_delta)
                # We also assume the same conc for all (or all None)
                target_conc = utils.single(list(set(t.target_conc for t in regular_transfers)))
                target_vol = utils.single(list(set(t.target_vol for t in regular_transfers)))
                yield target, [UpdateInfo(target_conc, target_vol, source_vol_delta)]
            else:
                yield target, [t.update_info for t in transfers]

    @staticmethod
    def group_transfers_by_target_analyte(transfer_batches):
        """Returns transfers grouped by target analyte"""
        ret = dict()
        for transfer_batch in transfer_batches:
            for transfer in transfer_batch.transfers:
                artifact = transfer.final_target_location.artifact
                ret.setdefault(artifact, list())
                ret[artifact].append(transfer)
        return ret

    def single_robot_transfer_batches_for_update(self):
        """
        Helper method that returns the first robot transfer batch, but validates first that
        the updated_source_vol is the same on both. This supports the use case where the
        user doesn't have to tell us which robot driver file they used, because the results will be the same.
        """
        all_robots = self.transfer_batches_by_robot.items()
        candidate_name, candidate_batches = all_robots[0]
        candidate_update_infos = {key: value for key, value in self.update_infos_by_target_analyte(candidate_batches)}

        # Validate that selecting this robot will have the same effect as selecting any other robot
        for current_name, current_batches in all_robots[1:]:
            # Both need to have the same number of transfer batches:
            if len(candidate_batches) != len(current_batches):
                raise Exception("Can't select a single robot for update. Different number of batches between {} and {}".
                                format(candidate_name, current_name))
            # For each transfer in the candidate, we must have a corresponding transfer in
            # the current having the same update_source_vol. Other values can be different (e.g.
            # sort order, plate names on robots etc.)
            current_update_infos = {key: value for key, value in self.update_infos_by_target_analyte(current_batches)}
            for analyte, candidate_update_info in candidate_update_infos.items():
                current_update_info = current_update_infos[analyte]
                if candidate_update_info != current_update_info:
                    raise Exception("There is a difference between the update infos between {} and {}. You need "
                                    "to explicitly select a robot".format(candidate_name, current_name))
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
                yield transfer

    def report(self):
        report = list()
        report.append("Dilution Session:")
        report.append("")
        for robot, transfer_batches in self.transfer_batches_by_robot.items():
            report.append("Robot: {}".format(robot))
            for transfer_batch in transfer_batches:
                report.append(transfer_batch.report())
        return "\n".join(report)


class SingleTransfer(object):
    """
    Encapsulates a single transfer between two positions:
      * From where: source (TransferEndpoint)
      * To where: target (TransferEndpoint)
      * How much sample volume and buffer volume are needed (in the robot file)
      * Other metadata that will be used for warnings etc, e.g. has_to_evaporate/scaled_up etc.
    """
    SPLIT_NONE = 0
    SPLIT_ROW = 1
    SPLIT_BATCH = 2

    def __init__(self, source_conc, source_vol, target_conc, target_vol, source_location, target_location):
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
        self.source_vol_delta = None

        # The TransferBatch takes care of marking the transfer as being a part of it
        self.transfer_batch = None

        # Regular transfers are "primary", but if they are split into others, either into rows or other transfer,
        # the resulting transfers are "secondary"
        self.is_primary = True

        # Set to False if source vol should not be updated based on this transfer.
        # NOTE: This solves a use case where transfer objects shouldn't be used to update source volume. It would
        # make sense to just set updated_source_vol to zero in that case, but this is currently simpler as it
        # solves the case with TransferBatches. Look into changing back to the other approach later
        self.should_update_source_vol = True
        self.should_update_target_vol = True
        self.should_update_target_conc = True

        self.split_type = SingleTransfer.SPLIT_NONE

        # A string identifying the batch the transfer should be grouped into, None being the default one.
        self.batch = None

    def _container_slot(self, is_source):
        if self.transfer_batch is None or self.source_location is None or self.target_location is None:
            return None
        container = self.source_location.container if is_source else self.target_location.container
        return self.transfer_batch.get_container_slot(container)

    @property
    def source_slot(self):
        """Provides the source slot of the transfer if it has been positioned yet"""
        return self._container_slot(True)

    @property
    def target_slot(self):
        """Provides the target slot of the transfer if it has been positioned yet"""
        return self._container_slot(False)

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

    @classmethod
    def initialize_transfer(cls, single_transfer, concentration_ref):
        input_artifact = single_transfer.source_location.artifact
        output_artifact = single_transfer.target_location.artifact
        try:
            single_transfer.source_conc = cls._referenced_concentration(input_artifact, concentration_ref)
        except AttributeError:
            pass

        try:
            single_transfer.source_vol = input_artifact.udf_current_sample_volume_ul
        except AttributeError:
            pass

        try:
            single_transfer.target_conc = cls._referenced_requested_concentration(output_artifact, concentration_ref)
        except AttributeError:
            pass

        try:
            single_transfer.target_vol = output_artifact.udf_target_vol_ul
        except AttributeError:
            pass

        # single_transfer.pair = pair  # TODO: Both setting the pair and source target!, if needed, set this earlier!
        return single_transfer

    def identifier(self):
        source = "source({}, conc={})".format(
            self.source_well, self.source_concentration)
        target = "target({}, conc={}, vol={})".format(self.target_well,
                                                      self.requested_concentration, self.requested_volume)
        return "{} => {}".format(source, target)

    @property
    def updated_source_vol(self):
        if self.source_vol_delta:
            return self.source_vol + self.source_vol_delta
        else:
            return None

    @property
    def update_info(self):
        return UpdateInfo(target_conc=self.target_conc if self.should_update_target_conc else None,
                          target_vol=self.target_vol if self.should_update_target_vol else None,
                          source_vol_delta=self.source_vol_delta if self.should_update_source_vol else None)

    @property
    def final_target_location(self):
        if not self.is_primary and self.split_type == SingleTransfer.SPLIT_BATCH:
            return self.original.target_location
        else:
            return self.target_location

    def split_type_string(self):
        if self.split_type == SingleTransfer.SPLIT_BATCH:
            return "batch"
        elif self.split_type == SingleTransfer.SPLIT_ROW:
            return "row"
        else:
            return "no split"

    def __repr__(self):
        return "<SingleTransfer {}({},{}=>[{}]) =({},{})=> {}({},{}) {} / {}>".format(
            self.source_location,
            self.source_conc,
            self.source_vol,
            self.updated_source_vol if self.should_update_source_vol else "",
            self.pipette_sample_volume,
            self.pipette_buffer_volume,
            self.target_location,
            self.target_conc,
            self.target_vol,
            "primary" if self.is_primary else "secondary ({})".format(self.split_type_string()),
            tuple(self.update_info))


# Represents source conc/vol, target conc/vol as one unit. TODO: Better name
DilutionMeasurements = namedtuple('DilutionMeasurements', ['source_conc', 'source_vol', 'target_conc', 'target_vol'])
UpdateInfo = namedtuple("UpdateInfo", ['target_conc', 'target_vol', 'source_vol_delta'])


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

        # NOTE: This is part of a quick-fix (used in one particular corner case)
        self.is_pooled = False

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

    def include_transfer_in_output(self, transfer):
        return True

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

    @abc.abstractmethod
    def get_filename(self, csv, context):
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
            - source position (container.index)
            - well index (down first)
            - pipette volume (descending)
        """
        assert transfer.transfer_batch is not None
        assert transfer.source_slot is not None
        assert transfer.source_slot.index is not None
        return (transfer.source_slot.index,
                transfer.source_location.index_down_first,
                -transfer.pipette_total_volume)

    def __repr__(self):
        return "<RobotSettings {}>".format(self.name)

    def __str__(self):
        return "<RobotSettings {name} file_ext='{file_ext}' file_handle='{file_handle}'>".format(
            name=self.name,
            file_handle=self.file_handle,
            file_ext=self.file_ext)


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

    def pre_conditions(self, transfer, robot_settings, dilution_settings):
        """
        Validates that the transfer is set up for dilution
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
        for transfer in transfer_batch.transfers:
            validation_exceptions = list(self.rules(transfer, robot_settings, dilution_settings))
            for exception in validation_exceptions:
                if not exception.transfer:
                    exception.transfer = transfer
            results.extend(validation_exceptions)
        return results

    def pre_validate(self, transfers, robot_settings, dilution_settings):
        # TODO: Reuses code
        results = ValidationResults()
        for transfer in transfers:
            validation_exceptions = list(self.pre_conditions(transfer, robot_settings, dilution_settings))
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

    def __init__(self, transfers, robot_settings, depth=0, is_temporary=False, name=None):
        self.depth = depth
        self.is_temporary = is_temporary  # temp dilution, no plate will actually be saved.
        self.validation_results = list()
        self._set_transfers(transfers, robot_settings)
        self.name = name
        self._transfers_by_output_dict = None
        # Set to True if the transfer batch was split
        self.split = False

    def get_container_slot(self, container):
        return self.container_to_container_slot[container]

    def _set_transfers(self, transfers, robot_settings):
        self._transfers_by_output_dict = None
        self._transfers = transfers
        self._sort_and_name_containers(robot_settings)
        for transfer in transfers:
            transfer.transfer_batch = self

    def _sort_and_name_containers(self, robot_settings):
        """Updates the list of containers and assigns temporary names and positions to them"""
        def sort_key(c):
            return not c.is_temporary, c.id

        def contains_only_control(container):
            return all(well.artifact.is_control for well in container.occupied)

        print self._transfers
        # We need to ensure that the containers that contain only a control always get index=0
        # NOTE: This is very site-specific so it would be better to solve it with handlers
        all_source_containers = set(transfer.source_location.container for transfer in self._transfers)
        source_containers_only_control = set(container for container in all_source_containers
                                             if contains_only_control(container))

        self.container_to_container_slot = dict()
        for container in source_containers_only_control:
            self.container_to_container_slot[container] = self._container_to_slot(robot_settings, container, 0, True)

        source_containers = all_source_containers - source_containers_only_control
        target_containers = set(transfer.target_location.container for transfer in self._transfers)
        assert len(source_containers.intersection(target_containers)) == 0

        source_containers = sorted(source_containers, key=sort_key)
        target_containers = sorted(target_containers, key=sort_key)

        for ix, container in enumerate(source_containers):
            self.container_to_container_slot[container] = \
                self._container_to_slot(robot_settings, container, ix, True)
        for ix, container in enumerate(target_containers):
            self.container_to_container_slot[container] = \
                self._container_to_slot(robot_settings, container, ix, False)

    @staticmethod
    def _container_to_slot(robot_settings, container, ix, is_source):
        slot = ContainerSlot(container, ix, None, is_source)
        slot.name = robot_settings.get_container_handle_name(slot)
        return slot

    @property
    def transfers(self):
        """
        Enumerates the transfers. The underlying transfer list is sorted by self._transfer_sort_key
        and row split is performed if needed
        """
        return self._transfers

    @property
    def transfers_by_output(self):
        """Returns the transfers in the batch grouped by the artifact in the target well"""
        if self._transfers_by_output_dict is None:
            self._transfers_by_output_dict = self._transfers_by_output()
        return self._transfers_by_output_dict

    def _transfers_by_output(self):
        def group_key(transfer):
            # TODO: Use the artifact rather than the id
            return transfer.target_location.artifact.id
        transfers = sorted(self.transfers, key=group_key)
        return {k: list(t) for k, t in groupby(transfers, key=group_key)}

    @property
    def container_mappings(self):
        ret = set()
        for transfer in self.transfers:
            ret.add((transfer.source_slot, transfer.target_slot))
        ret = list(sorted(ret, key=lambda t: t[0].index))
        return ret

    @property
    def target_container_slots(self):
        return sorted(set(target for source, target in self.container_mappings),
                      key=lambda cont: cont.index)

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
        for source, target in self.container_mappings:
            report.append(" - {} => {}".format(source, target))
        for transfer in self._transfers:
            report.append("{}".format(transfer))
        return "\n".join(report)

    def __str__(self):
        return self.report()


class TransferBatchCollection(object):
    """
    Encapsulates the list of TransferBatch object that go together, i.e. as the result of splitting a
    TransferBatch.
    """
    def __init__(self, *args):
        self._batches = list()
        self._batches.extend(args)

    def append(self, obj):
        self._batches.append(obj)

    def __iter__(self):
        return iter(self._batches)

    def __len__(self):
        return len(self._batches)

    def __getitem__(self, item):
        return self._batches[item]

    def report(self):
        return self.__str__()

    @property
    def driver_files(self):
        """Returns the driver files (csvs) as a dictionary"""
        return {tb.name: tb.driver_file for tb in self}

    def __repr__(self):
        return repr(self._batches)

    def __str__(self):
        ret = list()
        for batch in self._batches:
            ret.append(batch.report())
        return "\n\n".join(ret)


class ContainerSlot(object):
    """
    During a dilution, containers are positioned on a robot in a sequential order.
    This is encapsulated by wrapping them in a 'ContainerSlot'.

    The name is the name used by the robot to reference the container.
    """
    def __init__(self, container, index, name, is_source):
        self.container = container
        self.index = index
        self.name = name
        self.is_source = is_source

    def __repr__(self):
        return "{} ({}): [{}]".format(self.name, "source" if self.is_source else "target", self.container)


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


class TransferHandlerBase(object):
    """Base class for all handlers"""
    __metaclass__ = abc.ABCMeta

    def __init__(self, dilution_session, dilution_settings, robot_settings):
        self.dilution_session = dilution_session
        self.dilution_settings = dilution_settings
        self.robot_settings = robot_settings
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

    def __repr__(self):
        return self.__class__.__name__


class OrTransferHandler(TransferHandlerBase):
    """A handler that stops executing the subhandlers when one of them succeeds"""
    def __init__(self, dilution_session, dilution_settings, robot_settings, *sub_handlers):
        super(OrTransferHandler, self).__init__(dilution_session, dilution_settings, robot_settings)
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


# TODO: Just one handler type!
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


class TransferCalcHandlerBase(TransferHandlerBase):
    """
    Base class for handlers that change the transfer in some way, in particular calculating values
    """
    __metaclass__ = abc.ABCMeta

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

