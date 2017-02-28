import abc
import codecs
from collections import namedtuple
from clarity_ext.utils import lazyprop
from clarity_ext.service.dilution.strategies import *
from jinja2 import Template
from clarity_ext.service.file_service import Csv
from clarity_ext.domain.validation import ValidationException, ValidationType, ValidationResults, UsageError
from clarity_ext import utils
from clarity_ext.domain import Container


class DilutionService(object):
    def __init__(self, validation_service):
        self.validation_service = validation_service

    def create_session(self, robots, dilution_settings, transfer_batch_handler, transfer_handler, transfer_validator,
                       context):
        """
        Creates a DilutionSession based on the settings. Call evaluate to validate the entire session
        with a particular batch of objects.

        A DilutionSession contains several TransferBatch objects that need to be evaluated together
        """
        session = DilutionSession(self, robots, dilution_settings, transfer_batch_handler, transfer_handler,
                                  transfer_validator, self.validation_service, context)
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
                 transfer_validator, validation_service, context):
        """
        Initializes a DilutionSession object for the robots.

        :param dilution_service: A service providing methods for creating batches
        :param robots: A list of RobotSettings objects
        :param dilution_settings: The list of settings to apply for the dilution
        :param transfer_batch_handler: A handler that support splitting TransferBatch objects
        :param transfer_handler: A handler that supports splitting SingleTransfer objects (row in a TransferBatch)
        :param transfer_validator: A validator that runs on an entire TransferBatch that has perhaps been split.
        :param validation_service: The service that handles the results of validation exceptions
        :param context: The context the session is being created in
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
        self.context = context

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
        import copy
        from clarity_ext.domain import Well

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

        # Wrap the transfers in a TransferBatch object, it will do a basic validation on itself:
        # and raise a UsageError if it can't be used.
        batch = TransferBatch(transfers, robot_settings, name=robot_settings.name)

        # Based on the volume calculation strategy, calculate the volumes
        strategy.calculate_transfer_volumes(batch)
        return batch

    def initialize_transfer_from_settings(self, transfer, dilution_settings):
        # TODO: Handler
        if dilution_settings.volume_calc_method == DilutionSettings.VOLUME_CALC_FIXED:
            transfer.source_vol = transfer.source_location.artifact.input_artifact.udf_current_sample_volume_ul
            transfer.pipette_sample_volume = dilution_settings.fixed_sample_volume
        else:
            # Get a list of SingleTransfer objects
            SingleTransfer.initialize_transfer(transfer, dilution_settings.concentration_ref)

    @staticmethod
    def _should_include_pair(pair, dilution_settings):
        return not pair.input_artifact.is_control or dilution_settings.include_control


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
        for ix, transfer_batch in enumerate(self.transfer_batches_by_robot[robot_name]):
            csv = self._transfer_batch_to_robot_file(transfer_batch, robot_settings)
            # Add the file name:
            csv.file_name = robot_settings.get_filename(csv, self.context, ix)
            yield csv

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

    def update_infos_by_source_analyte(self, transfer_batches=None):
        """
        Returns the information that should be updated in the backend

        After the dilution has taken place, values should be updated in the backend:
         - Target conc. should be updated on the target analyte
         - Target vol. should be updated on the target analyte
         - Source vol. should be updated on the source analyte
        """
        ret = dict()
        if not transfer_batches:
            transfer_batches = self.single_robot_transfer_batches_for_update()

        # TODO: Encapsulate transfer_batches in an object?
        for target_analyte, transfers in self.group_transfers_by_target_analyte(transfer_batches).items():
            if target_analyte.is_control:
                # TODO: Rather set "should_update_source_vol" on every transfer for a control to simplify this
                continue

            primary_transfer = utils.single([t for t in transfers if t.is_primary])
            updated_source_vol = utils.single([t.updated_source_vol for t in transfers
                                               if t.should_update_source_vol])
            ret[primary_transfer.source_location.artifact] = (
                (primary_transfer.source_location.artifact, primary_transfer.target_location.artifact),
                UpdateInfo(primary_transfer.target_conc, primary_transfer.target_vol, updated_source_vol))
        return ret

    def group_transfers_by_target_analyte(self, transfer_batches):
        """Returns transfers grouped by target analyte, selecting one transfer batch if there are several"""
        ret = dict()
        for transfer_batch in transfer_batches:
            for transfer in transfer_batch.transfers:
                artifact = transfer.target_location.artifact
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
        candidate_update_infos = self.update_infos_by_source_analyte(candidate_batches)

        # Validate that selecting this robot will have the same effect as selecting any other robot
        for current_name, current_batches in all_robots[1:]:
            # Both need to have the same number of transfer batches:
            if len(candidate_batches) != len(current_batches):
                raise Exception("Can't select a single robot for update. Different number of batches between {} and {}".
                                format(candidate_name, current_name))
            # For each transfer in the candidate, we must have a corresponding transfer in
            # the current having the same update_source_vol. Other values can be different (e.g.
            # sort order, plate names on robots etc.)
            current_update_infos = self.update_infos_by_source_analyte(current_batches)
            for analyte, candidate_update_info in candidate_update_infos.items():
                current_update_info = current_update_infos[analyte][1]
                if candidate_update_info[1] != current_update_info:
                    raise Exception("There is a difference between the update infos between {} and {}. You need "
                                    "to explicitly select a robot".format(candidate__name, current_name))
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
        self.updated_source_vol = None

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

        # TODO: Move this
        def raise_target_measurements_missing():
            raise UsageError("You need to provide target volume and concentration for all samples. "
                             "Missing for {}.".format(single_transfer.output_location.artifact.id))

        # Now fill in with the UDF measurements
        if single_transfer.source_location.artifact.is_control:
            try:
                # The transfer for controls does only require target volume. Other values will be ignored.
                single_transfer.target_vol = single_transfer.target_location.artifact.udf_target_vol_ul
            except AttributeError:
                raise_target_measurements_missing()
            return single_transfer

        try:
            single_transfer.source_conc = cls._referenced_concentration(input_artifact, concentration_ref)
            single_transfer.source_vol = input_artifact.udf_current_sample_volume_ul
            single_transfer.target_conc = cls._referenced_requested_concentration(output_artifact,
                                                                                  concentration_ref)
            single_transfer.target_vol = output_artifact.udf_target_vol_ul
        except AttributeError:
            raise_target_measurements_missing()
        # single_transfer.pair = pair  # TODO: Both setting the pair and source target!, if needed, set this earlier!
        return single_transfer

    def identifier(self):
        source = "source({}, conc={})".format(
            self.source_well, self.source_concentration)
        target = "target({}, conc={}, vol={})".format(self.target_well,
                                                      self.requested_concentration, self.requested_volume)
        return "{} => {}".format(source, target)

    def __repr__(self):
        return "<SingleTransfer {}({},{}=>[{}]) =({},{})=> {}({},{}) {}>".format(
            self.source_location,
            self.source_conc,
            self.source_vol,
            self.updated_source_vol if self.should_update_source_vol else "",
            self.pipette_sample_volume,
            self.pipette_buffer_volume,
            self.target_location,
            self.target_conc,
            self.target_vol,
            "primary" if self.is_primary else "secondary")


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
        assert transfer.source_location.container.index is not None
        return (transfer.source_location.container.index,
                transfer.source_location.index_down_first,
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
            return self.split_transfer_batch(split, no_split, strategy, robot_settings)
        else:
            # No split was required
            return [transfer_batch]

    def split_transfer_batch(self, split, no_split, strategy, robot_settings):
        first_transfers = list(self.calculate_split_transfers(split))
        temp_transfer_batch = TransferBatch(first_transfers, robot_settings, depth=1, is_temporary=True)
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
                                          target_location=temp_transfer.original.target_location)

            # In the case of a split TransferBatch, only the secondary transfer should update source volume:
            new_transfer.should_update_source_vol = False
            second_transfers.append(new_transfer)

        # Add other transfers just as they were:
        second_transfers.extend(no_split)

        final_transfer_batch = TransferBatch(second_transfers, robot_settings, depth=1)
        strategy.calculate_transfer_volumes(final_transfer_batch)


        # For the analytes requiring splits
        return [temp_transfer_batch, final_transfer_batch]

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
            from clarity_ext.domain import Well
            # TODO: Copy the source location rather than using the original?
            temp_target_location = Well(transfer.target_location.position,
                                        temp_target_container,
                                        transfer.target_location.artifact)

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

    def __init__(self, transfers, robot_settings, depth=0, is_temporary=False, name=None):
        self.depth = depth
        self.is_temporary = is_temporary  # temp dilution, no plate will actually be saved.
        self.validation_results = list()
        self._set_transfers(transfers, robot_settings)
        self.name = name

        # A list of containers used for this transfer. These are not the original containers, but copies of them
        # so that they can be manipulated at will
        self.containers = None

    def _set_transfers(self, transfers, robot_settings):
        self.containers = list()
        self._transfers = transfers
        self._sort_and_name_containers(robot_settings)
        for transfer in transfers:
            transfer.transfer_batch = self

    def _sort_and_name_containers(self, robot_settings):
        """Updates the list of containers and assigns temporary names and positions to them"""

        def sort_key(c):
            return not c.is_temporary, c.id

        source_containers = set(transfer.source_location.container for transfer in self._transfers)
        target_containers = set(transfer.target_location.container for transfer in self._transfers)

        source_containers = list(sorted(source_containers, key=sort_key))
        target_containers = list(sorted(target_containers, key=sort_key))

        for ix, container in enumerate(source_containers):
            container.index = ix
        for ix, container in enumerate(target_containers):
            container.index = ix

        self.containers = source_containers + target_containers

        # Have the user update the temporary name:
        for container in self.containers:
            container.temp_name = robot_settings.get_container_handle_name(container)

    @property
    def transfers(self):
        """
        Enumerates the transfers. The underlying transfer list is sorted by self._transfer_sort_key
        and row split is performed if needed
        """
        return self._transfers

    @property
    def container_mappings(self):
        ret = set()
        for transfer in self.transfers:
            ret.add((transfer.source_location.container, transfer.target_location.container))
        return ret

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
