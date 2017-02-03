import abc
import copy
import codecs
from clarity_ext.utils import lazyprop
from clarity_ext.service.dilution_strategies import *
from jinja2 import Template
from clarity_ext.service.file_service import Csv
from clarity_ext.domain.validation import ValidationException, ValidationType, ValidationResults, UsageError
from clarity_ext.domain import ArtifactPair


class DilutionService(object):
    def __init__(self, validation_service):
        self.validation_service = validation_service

    def create_session(self, analyte_pairs, robots, dilution_settings, validator):
        """
        Creates and validates a DilutionSession based on the settings

        A DilutionSession contains several TransferBatch objects that need to be evaluated together
        """
        # TODO: Makes sense that the dilution_settings can return a strategy
        session = DilutionSession(self, robots, dilution_settings, analyte_pairs, validator)
        session.evaluate()
        # Push the validation results to the validation_service, which logs accordingly etc.
        # TODO: It should throw a UsageError if there are any errors left.
        self.validation_service.handle_validation(session.validation_results)
        return session

    def create_batches(self, pairs, robot_settings, dilution_settings, validator):
        """
        Creates a batch and breaks it up if required by the validator
        """
        transfer_batch = self.create_batch(pairs, robot_settings, dilution_settings, validator)
        #print("Initial transfer batch"); print(transfer_batch.report(True))

        # This will try to resolve validation errors with the transfer once, in particular the split validation
        # error, in which case it will split the batch into two:
        ret = self._try_resolve_transfer_batch_validation(transfer_batch, dilution_settings)

        # Now ret either has the original transfer_batch or a list of splits:
        return ret

    def create_batch(self, pairs, robot_settings, dilution_settings, validator):
        """
        Creates one batch (one-to-one relationship with a robot driver file) based on the input arguments.

        """
        # 1. Get a list of SingleTransfer objects
        transfers = [SingleTransfer.create_from_analyte_pair(pair, dilution_settings.concentration_ref)
                     for pair in pairs if self._include_pair(pair, dilution_settings)]

        # 2. Wrap the transfers in a TransferBatch object, it will do a basic validation on itself:
        #    and raise a UsageError if it can't be used.
        batch = TransferBatch(transfers)

        # 3. Based on the robot settings, create a RobotDeckPositioner that sets the correct
        #    position for each transfer, given that we're mapping using the default settings.
        #    NOTE: if split, this will be overwritten
        #    Currently limited to only one-to-one containers (TODO)
        #    The end result is that the index will be set so that it makes sense for the robt and
        #    the container_pos on the transfers will be set
        container_size = pairs[0].output_artifact.container.size
        robot_deck_positioner = RobotDeckPositioner(robot_settings, container_size)
        #robot_deck_positioner.position_transfers(transfers)
        # TODO: Rethinking this a bit...
        #robot_deck_positioner.position_transfers(transfers)

        # 5. Based on the volume calculation strategy, calculate the volumes
        dilution_settings.volume_calc_strategy.calculate_transfer_volumes(batch)

        # 6. Finished calculating volumes so we can safely validate the batch:
        batch.validate(validator)
        return batch

    def _try_resolve_transfer_batch_validation(self, transfer_batch, dilution_settings):
        """
        Validates a single TransferBatch, trying to resolve errors that can be resolved.

        Currently, the only error we can live through is transfers requiring splitting.
        If we see those, we'll split the transfers and return two dilution_schemes instead.
        """
        split = list()
        no_split = list()

        for validation_result in transfer_batch.validation_results:
            if isinstance(validation_result, NeedsSplitValidationException):
                split.append(validation_result.transfer)

        for transfer in transfer_batch.transfers:
            if transfer not in split:
                no_split.append(transfer)

        if len(split) > 0:
            # One or more pairs require a split. We need to take out these pairs and create two new
            # dilution schemes for these
            if transfer_batch.depth > 0:
                raise UsageError(
                    "The dilution can not be performed. Splits of transfer batches of depth {} are not supported".format(dilution_scheme.depth))
            return self._split_transfer_batch(split, no_split, dilution_settings)
        else:
            # No validation errors, we can go ahead with the single transfer_batch
            return [transfer_batch]

    def _split_transfer_batch(self, split, no_split, dilution_settings):
        # Now, TB1 should map from source to target in a temporary plate
        # The target location should be the same as the actual target location

        # Create two new transfer batches:
        # The first transfer batch should map to the same location as before but with constant values:

        # TODO: Locations are missing, should be same locations and DNA1 -> END1
        first_transfers = list(self._calculate_split_transfers(split))
        temp_transfer_batch = TransferBatch(first_transfers, depth=1, is_temporary=True)

        dilution_settings.volume_calc_strategy.calculate_transfer_volumes(temp_transfer_batch)

        #print ("TB1"); print temp_transfer_batch.report(True)

        second_transfers = list()

        # We need to create a new transfers list with:
        #  - target_location should be the original target location
        #  - source_location should also bet the original source location
        for temp_transfer in temp_transfer_batch.transfers:
            # NOTE: It's correct that the source_location is being set to the target_location here:
            from clarity_ext.domain import Analyte
            # TODO: api_resource should not be required
            source_analyte = Analyte(None, is_input=True, name="temp-{}".format(temp_transfer.source_analyte.id))
            new_transfer = SingleTransfer(temp_transfer.target_conc, temp_transfer.target_vol,
                                          temp_transfer.original.target_conc, temp_transfer.original.target_vol,
                                          source_location=temp_transfer.target_location,
                                          target_location=temp_transfer.original.target_location,
                                          source_analyte=source_analyte)
            second_transfers.append(new_transfer)

        # Add other transfers just as they were:
        second_transfers.extend(no_split)

        final_transfer_batch = TransferBatch(second_transfers, depth=1)
        dilution_settings.volume_calc_strategy.calculate_transfer_volumes(final_transfer_batch)

        #print("TB2"); print final_transfer_batch.report(True)

        # For the analytes requiring splits
        return [temp_transfer_batch, final_transfer_batch]

    def _calculate_split_transfers(self, original_transfers):
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
            temp_target_location = TransferLocation(temp_target_container, transfer.target_location.position)

            # TODO: This should be defined by a rule provided by the end user
            static_sample_volume = 4
            static_buffer_volume = 36
            copy = SingleTransfer(transfer.source_conc,
                                  transfer.source_vol,
                                  transfer.source_conc / 10.0,
                                  static_buffer_volume + static_sample_volume,
                                  transfer.source_location,
                                  temp_target_location,
                                  transfer.source_analyte)

            # In this case, we'll hardcode the values according to the lab's specs:
            copy.pipette_sample_volume = static_sample_volume
            copy.pipette_buffer_volume = static_buffer_volume
            copy.original = transfer
            yield copy

    def _include_pair(self, pair, dilution_settings):
        # TODO: Rename the dilution_setting rule to include_control
        return not pair.input_artifact.is_control or dilution_settings.include_blanks


class DilutionSession(object):
    """
    Encapsulates an entire dilution session, including validation of the dilution, generation of robot driver files
    and updating values.

    NOTE: This class might be merged with DilutionScheme later on.
    """
    def __init__(self, dilution_service, robots, dilution_settings, pairs, validator):
        """
        Creates a DilutionSession object for the robots. Use the DilutionSession object to create
        robot driver files and update values.

        The constructor does not automatically calculate and validate, call calculate() for that.

        :param robots: A list of RobotSettings objects
        :param dilution_settings: The list of settings to apply for the dilution
        :param pairs: A list of ArtifactPair items. Can be retrieved through ArtifactService.all_aliquot_pairs()
        """
        # TODO: Consider sending in the analytes instead of the artifact_service
        # For now, we're creating one DilutionScheme per robot. It might not be required later, i.e. if
        # the validation etc. doesn't differ between them
        self.dilution_service = dilution_service
        self.pairs = pairs
        self.transfer_batches = None
        self.robot_settings_by_name = {robot.name: robot for robot in robots}
        self.dilution_settings = dilution_settings
        self._driver_files = dict()  # A dictionary of generated driver files
        self.validation_results = None
        self.validator = validator

        # The transfer_batches created by this session. There may be several of these, but at the minimum
        # one per robot. Evaluated when you call evaluate()
        self.transfer_batches = None

    def evaluate(self):
        """
        Refreshes all calculations for all registered robots and validates.

        Broken validation rules that can be acted upon automatically will be and the system will be validated again.
        """
        self.transfer_batches = dict()
        # 1. For each robot, get the transfer batches for that robot.
        #    NOTE: There may be more than one transfer batches, if a split is required.
        for robot_settings in self.robot_settings_by_name.values():
            self.transfer_batches[robot_settings.name] = self.dilution_service.create_batches(
                self.pairs, robot_settings, self.dilution_settings, self.validator)

        # Now we have one transfer batch per robot. Let's dump hamilton!
        """
        for transfer_batch in self.transfer_batches["hamilton"]:
            print(transfer_batch.report(True))
            print("")

        """
        # ... biomek will look the same, but having different transfer_batch per robot gives us the
        # possibility of having different validations for them
        """
        for transfer_batch in self.transfer_batches["biomek"]:
            print(transfer_batch.report())
        """

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
        ret = set()
        for inp, outp in self.container_mappings:
            ret.add(outp)
        return list(ret)

    def driver_files(self, robot_name):
        """Returns the driver file for the robot. Might be cached"""
        # TODO: Now there can be more robot driver files per robot (because of splitting)
        if robot_name not in self._driver_files:
            self._driver_files[robot_name] = list(self._create_robot_driver_files(robot_name))
        return self._driver_files[robot_name]

    def all_driver_files(self):
        """Returns all robot driver files in tuples (robot, robot_file)"""
        for robot_name in self.robots:
            # TODO
            yield robot_name, self.driver_file(robot_name)

    def _create_robot_driver_files(self, robot_name):
        """
        Creates a csv for the robot
        """
        # TODO: Get this from the robot settings class, which is provided when setting up the DilutionSession
        robot_settings = self.robot_settings_by_name[robot_name]
        for transfer_batch in self.transfer_batches[robot_name]:
            yield self._transfer_batch_to_robot_file(transfer_batch, robot_settings)

    def _transfer_batch_to_robot_file(self, transfer_batch, robot_settings):
        """
        Maps a transfer_batch to a robot file of type Csv
        """
        csv = Csv(delim="\t")  # TODO: \t should be in the robot settings
        csv.header.extend(robot_settings.header)
        for transfer in transfer_batch.transfers:
            csv.append(robot_settings.map_transfer_to_row(transfer), transfer)
        return csv

    def create_general_driver_file(self, template_path, **kwargs):
        """
        Creates a driver file that has access to the DilutionSession object throught the name `session`.
        """
        with open(template_path, 'r') as fs:
            text = fs.read()
            text = codecs.decode(text, "utf-8")
            template = Template(text)
            rendered = template.render(session=self, **kwargs)
            return rendered


class TransferLocation(object):
    """
    Represents either a source or a target of a transfer
    """
    def __init__(self, container, position):
        """
        :param container: A Container that holds the analyte to be transferred
        :param position: The ContainerPosition pointing to a position within the container, e.g. A:1
        """
        self.container = container
        self.position = position

        # This is not set by the user but by the TransferBatch when the user queries for the object.
        # TODO: Rename to container_pos
        self.container_pos = None

    @staticmethod
    def create_from_analyte(analyte):
        return TransferLocation(analyte.container, analyte.well.position)

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
                 source_analyte):
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

        self.updated_source_vol = None

        # TODO: positions after actually positioning on a robot

    def report(self):
        """Returns a detailed view of this transfer object for debugging and validation"""
        report = list()
        report.append("SingleTransfer from {}".format(self.source_analyte.name))
        report.append("-" * len(report[-1]))
        report.append(" - source_conc: {}".format(self.source_conc))
        report.append(" - source_vol: {}".format(self.source_vol))
        report.append(" - pipette_sample_volume: {}".format(self.pipette_sample_volume))
        report.append(" - pipette_buffer_volume: {}".format(self.pipette_buffer_volume))
        report.append(" - target_conc: {}".format(self.target_conc))
        report.append(" - target_vol: {}".format(self.target_vol))
        report.append(" - updated_source_vol: {}".format(self.updated_source_vol))
        return "\n".join(report)


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
    def create_from_analyte_pair(cls, pair, concentration_ref):
        conc1 = cls._referenced_concentration(pair.input_artifact, concentration_ref)
        vol1 = pair.input_artifact.udf_current_sample_volume_ul
        conc2 = cls._referenced_requested_concentration(pair.output_artifact, concentration_ref)
        vol2 = pair.output_artifact.udf_target_vol_ul
        source_location = TransferLocation.create_from_analyte(pair.input_artifact)
        target_location = TransferLocation.create_from_analyte(pair.output_artifact)
        transfer = SingleTransfer(conc1, vol1, conc2, vol2, source_location, target_location, pair.input_artifact)
        transfer.pair = pair
        return transfer

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


class EndpointPositioner(object):
    """
    Handles positions for all plates and wells for either source or
    destination placement on a robot deck
    """

    def __init__(self, robot_settings, transfer_endpoints, plate_size, plate_pos_prefix):
        self.robot_settings = robot_settings
        self._plate_size = plate_size
        self.plate_sorting_map = self._build_plate_sorting_map(
            [transfer_endpoint.analyte.container for transfer_endpoint in transfer_endpoints])
        self.plate_position_map = self._build_plate_position_map(
            self.plate_sorting_map, plate_pos_prefix)

    @staticmethod
    def _build_plate_position_map(plate_sorting_map, plate_pos_prefix):
        # Fetch a unique list of container names from input
        # Make a dictionary with container names and plate positions
        # eg. END1, DNA2
        plate_positions = []
        for key, value in plate_sorting_map.iteritems():
            plate_position = "{}{}".format(plate_pos_prefix, value)
            plate_positions.append((key, plate_position))

        plate_positions = dict(plate_positions)
        return plate_positions

    @staticmethod
    def _build_plate_sorting_map(containers):
        # Fetch an unique list of container names from input
        # Make a dictionary with container names and plate position sort numbers
        unique_containers = sorted(list(
            {container.id for container in containers}))
        positions = range(1, len(unique_containers) + 1)
        plate_position_numbers = dict(zip(unique_containers, positions))
        return plate_position_numbers

    def find_sort_number(self, transfer):
        """Sort dilutes according to plate and well positions
        """
        plate_base_number = self._plate_size.width * self._plate_size.height + 1
        plate_sorting = self.plate_sorting_map[
            transfer.source_container.id
        ]
        # Sort order for wells are always based on down first indexing
        # regardless the robot type
        return plate_sorting * plate_base_number + transfer.source_well.index_down_first

    def __str__(self):
        return "<{type} {robot} {height}x{width}>".format(type=self.__class__.__name__,
                                                          robot=self.robot_name,
                                                          height=self._plate_size.size.height,
                                                          width=self._plate_size.size.width)


class RobotDeckPositioner(object):
    """
    Handle plate positions on the robot deck (target and source)
    as well as well indexing
    """

    def __init__(self, robot_settings, plate_size):
        self.robot_settings = robot_settings
        self.plate_size = plate_size
        """
        print "HERE", robot_settings, transfers, plate_size
        source_endpoints = [transfer.source for transfer in dilutes]
        source_positioner = EndpointPositioner(robot_settings, source_endpoints,
                                               plate_size, "DNA")
        destination_endpoints = [transfer.destination for transfer in dilutes]
        destination_positioner = EndpointPositioner(
            robot_settings, destination_endpoints, plate_size, "END")

        self._robot_settings = robot_settings
        self._plate_size = plate_size
        self._source_positioner = source_positioner
        self.source_plate_position_map = source_positioner.plate_position_map
        self.target_plate_position_map = destination_positioner.plate_position_map
        """

    def position_transfers(self, transfers):
        """
        TODO: Have this handle only the indexing. Setting DNA1, END1 etc should happen before
        """
        return
        for transfer in transfers:
            transfer.source_well_index = self.get_index_from_well(
                transfer.source_well)
            transfer.source_plate_pos = self.source_plate_position_map[transfer.source_container.id]
            transfer.target_well_index = self.robot_settings.get_index_from_well(transfer.target_well)
            transfer.target_plate_pos = self.target_plate_position_map[transfer.target_container.id]

    def __str__(self):
        return "<{type} {robot} {height}x{width}>".format(type=self.__class__.__name__,
                                                          robot=self._robot_name,
                                                          height=self._plate_size.height,
                                                          width=self._plate_size.width)


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

    def __init__(self, scale_up_low_volumes=True, concentration_ref=None, include_blanks=False,
                 volume_calc_method=None, make_pools=False, pipette_max_volume=None,
                 dilution_waste_volume=0):
        """
        :param dilution_waste_volume: Extra volume that should be subtracted from the sample volume
        to account for waste during dilution
        """
        self.scale_up_low_volumes = scale_up_low_volumes
        # TODO: Use py3 enums instead
        concentration_ref = self._parse_conc_ref(concentration_ref)
        if concentration_ref not in [self.CONCENTRATION_REF_NM, self.CONCENTRATION_REF_NGUL]:
            raise ValueError("Unsupported concentration_ref '{}'".format(concentration_ref))
        self.concentration_ref = concentration_ref
        # TODO: include_blanks, has that to do with output only? If so, it should perhaps be in RobotSettings
        self.include_blanks = include_blanks
        self.volume_calc_method = volume_calc_method
        self.make_pools = make_pools
        self.pipette_max_volume = pipette_max_volume
        self.dilution_waste_volume = dilution_waste_volume
        self.volume_calc_strategy = self._create_strategy()

    def _create_strategy(self):
        if self.volume_calc_method == self.VOLUME_CALC_FIXED:
            return FixedVolumeCalc(self)
        elif self.volume_calc_method == self.VOLUME_CALC_BY_CONC and self.make_pools is False:
            return OneToOneConcentrationCalc(self)
        elif self.volume_calc_method == self.VOLUME_CALC_BY_CONC and self.make_pools is True:
            return PoolConcentrationCalc(self)
        else:
            raise ValueError("Volume calculation method is not implemented for these settings: '{}'".
                             format(self))

    def _parse_conc_ref(self, concentration_ref):
        if isinstance(concentration_ref, basestring):
            for key, value in DilutionSettings.CONCENTRATION_REF_TO_STR.items():
                if value == concentration_ref:
                    return key
        else:
            return concentration_ref

    @staticmethod
    def concentration_unit_to_string(conc_ref):
        return DilutionSettings.CONCENTRATION_REF_TO_STR[conc_ref]


class RobotSettings(object):
    __metaclass__ = abc.ABCMeta

    def __init__(self, name, file_handle, file_ext):
        """
        Inherit from this file to supply new settings for a robot
        """
        self.name = name
        self.file_handle = file_handle
        self.file_ext = file_ext

    @abc.abstractmethod
    def map_transfer_to_row(self, transfer):
        """
        Describes how to transform a SingleTransfer object to a csv row
        :return:
        """
        pass

    @property
    def delimiter(self):
        """
        :return: The delimiter used in the generated CSV file.
        """
        return "\t"

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

    def __repr__(self):
        return "<RobotSettings {}>".format(self.name)

    def __str__(self):
        return "<RobotSettings {name} file_ext='{file_ext}' file_handle='{file_handle}'>".format(
            name=self.name,
            file_handle=self.file_handle,
            file_ext=self.file_ext)


# TODO: Move
class TemplateHelper:
    @staticmethod
    def get_from_package(package, name):
        """Loads a Jinja template from the package"""
        import os
        templates_dir = os.path.dirname(package.__file__)
        for candidate_file in os.listdir(templates_dir):
            if candidate_file == name:
                return os.path.join(templates_dir, candidate_file)


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

    @staticmethod
    def needs_split():
        """
        Certain transfers may require a split to be successful in the current state
        """
        return NeedsSplitValidationException()

    def rules(self, transfer):
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

    def validate(self, transfer_batch):
        """
        Validates the transfers, first by validating that calculation can be performed, then by
        running all custom validations.

        Returns a tuple of (errors, warnings).
        """
        results = ValidationResults()
        for transfer in transfer_batch.transfers:
            validation_exceptions = list(self.rules(transfer))
            for exception in validation_exceptions:
                if not exception.transfer:
                    exception.transfer = transfer
            results.extend(validation_exceptions)
        return results


class TransferValidationException(ValidationException):
    """Wraps a validation exception for Dilution transfer objects"""

    # TODO: This is a convenient wrapper for the design right now, but it would
    # be preferable to rather switch to a tuple of ValidationException and Transfer object when validating
    # and handle the formatting in the code outputting the errors.
    def __init__(self, transfer, msg, result_type=ValidationType.ERROR):
        super(TransferValidationException, self).__init__(msg, result_type)
        self.transfer = transfer

    def __repr__(self):
        return "{}({}): {}".format(self._repr_type(), self.transfer, self.msg)


class TransferBatch(object):
    """
    Encapsulates a list of SingleTransfer objects. Used to generate robot driver files.
    """
    def __init__(self, transfers, depth=0, is_temporary=False):
        self.depth = depth
        self.is_temporary = is_temporary  # temp dilution, no plate will actually be saved.
        self.validation_results = list()
        self._set_transfers(transfers)

    def _set_transfers(self, transfers):
        self._transfers = transfers
        self._set_container_refs()

    def _set_container_refs(self):
        """
        Updates the container refs (DNA1, END1 etc) for each container in the transfer.
        These depend on the state of the entire batch, so they need to be updated
        if the transfers are updated.
        """
        self._container_to_container_ref = self._evaluate_container_refs()

        for transfer in self._transfers:
            # First get the index being used
            transfer.source_location.container_pos = self._container_to_container_ref[transfer.source_location.container]
            transfer.target_location.container_pos = self._container_to_container_ref[transfer.target_location.container]

    def _evaluate_container_refs(self):
        """
        Figures out the mapping from container to container refs
        (first input container => 1)
        (first output container => 1) etc

        The containers are sorted by (not is_temporary, id)
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

    def enumerate_split_row_transfers(self):
        return self.split_up_high_volume_rows(self.sorted_transfers(self._transfers))

    @property
    def transfers(self):
        return sorted(self._transfers, key=self._transfer_sort_key)

    def _transfer_sort_key(self, transfer):
        """
        Sort the transfers based on:
            - source position (container_pos)
            - well index (down first)
            - pipette volume
        """
        # TODO: Rest of the sort stuff
        # TODO: Makes sense that the user can provide the sorting
        # print "SORT", (transfer.source_location.container_pos)
        assert transfer.source_location.container_pos is not None
        return transfer.source_location.container_pos

    """
    @property
    def max_pipette_volume(self):
        volume = max(t.pipette_total_volume for t in self._transfers)
        print "HERE", volume
        return volume
    """

    def pre_validate(self):
        """
        Validate that we can run a validation. If there are any errors, we raise a UsageException
        containing these validation errors.
        """
        def pre_conditions(transfer):
            if not transfer.source_vol:
                yield TransferValidationException(transfer, "source volume is not set.")
            if not transfer.source_conc:
                yield TransferValidationException(transfer, "source concentration not set.")
            if not transfer.target_conc:
                yield TransferValidationException(transfer, "target concentration is not set.")
            if not transfer.target_vol:
                yield TransferValidationException(transfer, "target volume is not set.")
        # 1. Go through the built-in validations first:
        for transfer in self.transfers:
            self.results.extend(list(pre_conditions(transfer)))

        if len(results.errors) > 0:
            raise UsageError("There were validation errors", results)

    def validate(self, validator):
        # Run the validator on the object and save the results on the object.
        self.validation_results.extend(validator.validate(self))

    def report(self, details=False):
        """
        Creates a detailed report of what's included in the transfer, for debug learning purposes.
        """
        report = list()
        report.append("TransferBatch:")
        report.append("-" * len(report[-1]))
        report.append(" - temporary: {}".format(self.is_temporary))

        for transfer in self.transfers:
            if details:
                report.append(transfer.report())
            else:
                report.append("[{}({})/{}({}, {}) => {}({})/{}({}, {})]".format(
                    transfer.source_location.container.name,
                    transfer.source_location.container_pos,
                    transfer.source_location.position,
                    transfer.source_conc, transfer.source_vol,
                    transfer.target_location.container.name,
                    transfer.target_location.container_pos,
                    transfer.target_location.position,
                    transfer.target_conc, transfer.target_vol))
        return "\n".join(report)


class DilutionSchemeNoExists(object):
    """
    Creates a dilution scheme, given input and output analytes.

    One dilution scheme wraps all transfers required together (i.e. that will go with one robot file)
    as well as containing validation errors etc.
    """
    """
    def validate(self):
        return self.validator.validate(self.enumerate_transfers())
    """

    def split_up_high_volume_rows(self, transfers):
        """
        Split up a transfer between source well x and target well y into
        several rows, if sample volume or buffer volume exceeds 50 ul
        :return:
        """
        split_row_transfers = []
        for transfer in transfers:
            calculation_volume = max(transfer.sample_volume, transfer.pipette_buffer_volume)
            (n, residual) = divmod(calculation_volume, self.dilution_settings.pipette_max_volume)
            if residual > 0:
                total_rows = int(n + 1)
            else:
                total_rows = int(n)

            copies = self._create_copies(transfer, total_rows)
            split_row_transfers += copies
            self._split_up_volumes(
                copies, transfer.sample_volume, transfer.pipette_buffer_volume)

        return split_row_transfers

    @staticmethod
    def _create_copies(transfer, total_rows):
        """
        Create copies of transfer
        that will cause extra rows in the driver file if
        pipetting volume exceeds 50 ul.
        Initiate both buffer volume and sample volume to zero
        :param transfer: The transfer to be copied
        :param total_rows: The total number of rows needed for a
        transfer between source well x and target well y
        :return:
        """
        copies = [copy.copy(transfer)]
        for i in xrange(0, total_rows - 1):
            t = copy.copy(transfer)
            t.pipette_buffer_volume = 0
            t.sample_volume = 0
            copies.append(t)

        return copies

    def _split_up_volumes(self, transfers, original_sample_volume, original_buffer_volume):
        """
        Split up any transferring volume exceeding 50 ul to multiple rows represented by the
         list 'transfers'
        :param transfers: Represents rows in driver file,
        needed for a transfer between source well x and target well y
        :param original_sample_volume: Sample volume first calculated for a single row transfer,
        that might exceed 50 ul
        :param original_buffer_volume: Buffer volume first calculated for a single row transfer,
        that might exceed 50 ul
        :return:
        """
        number_rows = len(transfers)
        for t in transfers:
            # Only split up pipetting volume if the actual volume exceeds
            # max of 50 ul. Otherwise, the min volume of 2 ul might be
            # violated.
            if original_buffer_volume > self.dilution_settings.pipette_max_volume:
                t.pipette_buffer_volume = float(
                    original_buffer_volume / number_rows)

            if original_sample_volume > self.dilution_settings.pipette_max_volume:
                t.sample_volume = float(
                    original_sample_volume / number_rows)

    def __str__(self):
        return "<DilutionScheme {}>".format(self.robot_settings.name)


class NeedsSplitValidationException(TransferValidationException):
    def __init__(self):
        super(NeedsSplitValidationException, self).__init__(None,
                                                            "The transfer requires a split",
                                                            ValidationType.ERROR)

