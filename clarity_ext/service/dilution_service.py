import copy
import codecs
from clarity_ext.utils import lazyprop
from clarity_ext.service.dilution_strategies import *
from jinja2 import Template
from clarity_ext.service.file_service import Csv


class DilutionService(object):

    def __init__(self, artifact_service):
        self.artifact_service = artifact_service

    def create_scheme(self, robot_name, dilution_settings):
        # TODO: It's currently necessary to create one dilution scheme per robot type,
        # but it would be preferable that there would be only one
        volume_calc_strategy = self.create_strategy(dilution_settings)
        return DilutionScheme(artifact_service=self.artifact_service, robot_name=robot_name,
                              dilution_settings=dilution_settings, volume_calc_strategy=volume_calc_strategy)

    @staticmethod
    def create_strategy(settings):
        if settings.volume_calc_method == settings.VOLUME_CALC_FIXED:
            return FixedVolumeCalc()
        elif settings.volume_calc_method == settings.VOLUME_CALC_BY_CONC and settings.make_pools is False:
            return OneToOneConcentrationCalc()
        elif settings.volume_calc_method == settings.VOLUME_CALC_BY_CONC and settings.make_pools is True:
            return PoolConcentrationCalc()
        else:
            raise ValueError("Volume calculation method is not implemented for these settings: '{}'".
                             format(settings))

    def create_session(self, robots, dilution_settings):
        volume_calc_strategy = self.create_strategy(dilution_settings)
        pairs = self.artifact_service.all_aliquot_pairs()
        return DilutionSession(robots, dilution_settings, volume_calc_strategy, pairs)


class DilutionSession(object):
    """
    Encapsulates an entire dilution session, including validation of the dilution, generation of robot driver files
    and updating values.

    NOTE: This class might be merged with DilutionScheme later on.
    """
    def __init__(self, robots, dilution_settings, volume_calc_strategy, pairs):
        """
        Creates a DilutionSession object for the robots. Use the DilutionSession object to create
        robot driver files and update values.

        :param robots: A list of RobotSettings objects
        :param dilution_settings: The list of settings to apply for the dilution
        :param pairs: A list of ArtifactPair items. Can be retrieved through ArtifactService.all_aliquot_pairs()
        """
        # TODO: Consider sending in the analytes instead of the artifact_service
        # For now, we're creating one DilutionScheme per robot. It might not be required later, i.e. if
        # the validation etc. doesn't differ between them
        self.pairs = pairs

        self.dilution_schemes = dict()
        self.robots = {robot.name: robot for robot in robots}
        self.dilution_settings = dilution_settings
        self._driver_files = dict()  # A dictionary of generated driver files
        for robot in robots:
            self.dilution_schemes[robot.name] = DilutionScheme(self.pairs, robot.name,
                                                               dilution_settings, volume_calc_strategy)

    def validate(self):
        # Current limitation: Each analyte should have the same target volume and concentration
        for dilution_scheme in self.dilution_schemes.values():
            dilution_scheme.validate()

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

    def driver_file(self, robot_name):
        """Returns the driver file for the robot. Might be cached"""
        if robot_name not in self._driver_files:
            self._driver_files[robot_name] = self.create_robot_driver_file(robot_name)
        return self._driver_files[robot_name]

    def all_driver_files(self):
        """Returns all robot driver files in tuples (robot, robot_file)"""
        for robot_name in self.robots:
            yield robot_name, self.driver_file(robot_name)

    def create_robot_driver_file(self, robot_name):
        """
        Creates a csv for the robot
        """
        # TODO: Get this from the robot settings class, which is provided when setting up the DilutionSession
        robot_settings = self.robots[robot_name]
        csv = Csv()
        csv.header.extend(robot_settings.header)
        for transfer in self.dilution_schemes["hamilton"].enumerate_transfers():
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


class TransferEndpoint(object):
    """
    TransferEndpoint wraps an source or destination analyte involved in a dilution
    """
    # TODO: Handle tube racks

    def __init__(self, aliquot, concentration_ref=None):
        self.aliquot_name = aliquot.name
        self.well = aliquot.well
        self.container = aliquot.container
        self.concentration = self._referenced_concentration(
            aliquot=aliquot, concentration_ref=concentration_ref)
        # TODO: Temporary fix. This udf is not available on all objects
        #       The same goes for all other AttributeErrors in this commit 
        try:
            self.volume = aliquot.udf_current_sample_volume_ul
        except AttributeError:
            self.volume = None
        self.is_control = False
        if hasattr(aliquot, "is_control"):
            self.is_control = aliquot.is_control
        self.is_from_original = aliquot.is_from_original
        self.requested_concentration = self._referenced_requested_concentration(
            aliquot, concentration_ref)

        try:
            self.requested_volume = aliquot.udf_target_vol_ul
        except AttributeError:
            self.requested_volume = None
        self.well_index = None
        self.plate_pos = None

    def _referenced_concentration(self, aliquot=None, concentration_ref=None):
        if concentration_ref == DilutionSettings.CONCENTRATION_REF_NGUL:
            try:
                return aliquot.udf_conc_current_ngul
            except AttributeError:
                return None
        elif concentration_ref == DilutionSettings.CONCENTRATION_REF_NM:
            try:
                return aliquot.udf_conc_current_nm
            except AttributeError:
                return None
        else:
            raise NotImplementedError(
                "Concentration ref {} not implemented".format(
                    concentration_ref)
            )

    def _referenced_requested_concentration(self, aliquot=None, concentration_ref=None):
        if concentration_ref == DilutionSettings.CONCENTRATION_REF_NGUL:
            try:
                return aliquot.udf_target_conc_ngul
            except AttributeError:
                return None
        elif concentration_ref == DilutionSettings.CONCENTRATION_REF_NM:
            try:
                return aliquot.udf_target_conc_nm
            except AttributeError:
                return None
        else:
            raise NotImplementedError(
                "Concentration ref {} not implemented".format(
                    concentration_ref)
            )


class SingleTransfer(object):
    # Enclose sample data, user input and derived variables for a
    # single row in a dilution

    def __init__(self, source_endpoint, destination_endpoint=None, pair_id=None):
        self.pair_id = pair_id
        self.aliquot_name = source_endpoint.aliquot_name
        self.is_control = source_endpoint.is_control
        self.is_source_from_original = source_endpoint.is_from_original

        self.source = source_endpoint
        self.destination = destination_endpoint

        self.source_well = source_endpoint.well
        self.source_container = source_endpoint.container
        self.source_concentration = source_endpoint.concentration
        self.source_initial_volume = source_endpoint.volume
        self.source_well_index = None
        self.source_plate_pos = None

        self.target_aliquot_name = None
        self.target_well = None
        self.target_container = None
        self.requested_concentration = None
        self.requested_volume = None
        self.target_well_index = None
        self.target_plate_pos = None

        if destination_endpoint:
            self.set_destination_endpoint(destination_endpoint)

        self.sample_volume = None
        self.buffer_volume = None
        self.has_to_evaporate = None
        self.scaled_up = False

    def set_destination_endpoint(self, destination_endpoint):
        self.target_well = destination_endpoint.well
        self.target_container = destination_endpoint.container
        self.requested_concentration = destination_endpoint.requested_concentration
        self.requested_volume = destination_endpoint.requested_volume
        self.target_aliquot_name = destination_endpoint.aliquot_name

    def identifier(self):
        source = "source({}, conc={})".format(
            self.source_well, self.source_concentration)
        target = "target({}, conc={}, vol={})".format(self.target_well,
                                                      self.requested_concentration, self.requested_volume)
        return "{} => {}".format(source, target)

    def __str__(self):
        source = "source({}, conc={})".format(
            self.source_well, self.source_concentration)
        target = "target({}, conc={}, vol={})".format(self.target_well,
                                                      self.requested_concentration, self.requested_volume)
        return "{} => {}".format(source, target)

    def __repr__(self):
        return "<SingleTransfer {}>".format(self.aliquot_name)


class EndpointPositioner(object):
    """
    Handles positions for all plates and wells for either source or
    destination placement on a robot deck
    """

    def __init__(self, robot_name, transfer_endpoints, plate_size, plate_pos_prefix):
        self.robot_name = robot_name
        self._plate_size = plate_size
        # TODO: Remove robot specific settings, should be provided in the robot_settings
        index_method_map = {"hamilton": lambda well: well.index_down_first,
                            "biomek": lambda well: well.index_right_first}
        self.indexer = index_method_map[robot_name]
        self.plate_sorting_map = self._build_plate_sorting_map(
            [transfer_endpoint.container for transfer_endpoint in transfer_endpoints])
        self.plate_position_map = self._build_plate_position_map(
            self.plate_sorting_map, plate_pos_prefix)

    @staticmethod
    def _build_plate_position_map(plate_sorting_map, plate_pos_prefix):
        # Fetch an unique list of container names from input
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

    def __init__(self, robot_name, dilutes, plate_size):
        source_endpoints = [dilute.source for dilute in dilutes]
        source_positioner = EndpointPositioner(robot_name, source_endpoints,
                                               plate_size, "DNA")
        destination_endpoints = [
            dilute.destination for dilute in dilutes]
        destination_positioner = EndpointPositioner(
            robot_name, destination_endpoints, plate_size, "END")

        self._robot_name = robot_name
        self._plate_size = plate_size
        self._source_positioner = source_positioner
        self.indexer = source_positioner.indexer
        self.source_plate_position_map = source_positioner.plate_position_map
        self.target_plate_position_map = destination_positioner.plate_position_map

    def find_sort_number(self, dilute):
        """Sort dilutes according to plate and well positions in source
        """
        return self._source_positioner.find_sort_number(dilute)

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

    def __init__(self, scale_up_low_volumes=True, concentration_ref=None, include_blanks=False,
                 volume_calc_method=None, make_pools=False, pipette_max_volume=None,
                 dilution_waste_volume=0):
        """
        :param dilution_waste_volume: Extra volume that should be subtracted from the sample volume
        to account for waste during dilution
        """
        self.scale_up_low_volumes = scale_up_low_volumes
        # TODO: Use py3 enums instead
        if concentration_ref not in [self.CONCENTRATION_REF_NM, self.CONCENTRATION_REF_NGUL]:
            raise ValueError("Unsupported concentration_ref '{}'".format(concentration_ref))
        self.concentration_ref = concentration_ref
        # TODO: include_blanks, has that to do with output only? If so, it should perhaps be in RobotSettings
        self.include_blanks = include_blanks
        self.volume_calc_method = volume_calc_method
        self.make_pools = make_pools
        self.pipette_max_volume = pipette_max_volume
        self.dilution_waste_volume = dilution_waste_volume

    @property
    def concentration_ref_string(self):
        if self.concentration_ref == self.CONCENTRATION_REF_NGUL:
            return "ng/ul"
        else:
            return "nM"

import abc


class RobotSettings(object):
    __metaclass__ = abc.ABCMeta

    def __init__(self, name):
        """
        Inherit from this file to supply new settings for a robot
        """
        self.name = name

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

    def __repr__(self):
        return "<RobotSettings {}>".format(self.name)


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


class DilutionScheme(object):
    """Creates a dilution scheme, given input and output analytes."""

    def __init__(self, pairs, robot_name, dilution_settings, volume_calc_strategy):
        """
        Calculates all derived values needed in dilute driver file.

        :param pairs: A list of input/output analytes, wrapped in an ArtifactPair
        """
        # TODO: Use settings object without mapping over
        self.dilution_settings = dilution_settings
        self.scale_up_low_volumes = dilution_settings.scale_up_low_volumes
        self.volume_calc_strategy = volume_calc_strategy

        # TODO: Support many-to-many containers
        container = pairs[0].output_artifact.container
        all_transfers = self._create_transfers(
            pairs, concentration_ref=dilution_settings.concentration_ref)
        self._transfers = self._filtered_transfers(
            all_transfers=all_transfers, include_blanks=dilution_settings.include_blanks)

        self.aliquot_pair_by_transfer = self._map_pair_and_transfers(
            pairs=pairs)
        self.robot_deck_positioner = RobotDeckPositioner(
            robot_name, self._transfers, container.size)

        self.calculate_transfer_volumes()
        self.do_positioning()

    def enumerate_split_row_transfers(self):
        return self.split_up_high_volume_rows(self.sorted_transfers(self._transfers))

    def enumerate_transfers(self):
        return self.sorted_transfers(self._transfers)

    def _filtered_transfers(self, all_transfers, include_blanks):
        if include_blanks:
            return all_transfers
        else:
            return list(t for t in all_transfers if t.is_control is False)

    def _create_transfers(self, aliquot_pairs, concentration_ref=None):
        transfers = []
        for pair in aliquot_pairs:
            source_endpoint = TransferEndpoint(
                pair.input_artifact, concentration_ref=concentration_ref)
            destination_endpoint = TransferEndpoint(
                pair.output_artifact, concentration_ref=concentration_ref)
            transfers.append(SingleTransfer(
                source_endpoint, destination_endpoint, pair_id=id(pair)))
        return transfers

    def _map_pair_and_transfers(self, pairs):
        """
        :param pairs: input artifact --- output artifact pair
        :return: A function returning an artifact pair, given a transfer object
        """
        pair_dict = {id(pair): pair for pair in pairs}

        def pair_by_transfer(transfer):
            by_transfer_dict = {transfer.identifier: pair_dict[
                transfer.pair_id] for transfer in self._transfers}
            return by_transfer_dict[transfer.identifier]
        return pair_by_transfer

    def calculate_transfer_volumes(self):
        # Handle volumes etc.
        self.volume_calc_strategy.calculate_transfer_volumes(
            transfers=self._transfers, scale_up_low_volumes=self.scale_up_low_volumes)

    def split_up_high_volume_rows(self, transfers):
        """
        Split up a transfer between source well x and target well y into
        several rows, if sample volume or buffer volume exceeds 50 ul
        :return:
        """
        split_row_transfers = []
        for transfer in transfers:
            calculation_volume = max(
                self._get_volume(transfer.sample_volume), self._get_volume(transfer.buffer_volume))
            (n, residual) = divmod(calculation_volume, self.dilution_settings.pipette_max_volume)
            if residual > 0:
                total_rows = int(n + 1)
            else:
                total_rows = int(n)

            copies = self._create_copies(transfer, total_rows)
            split_row_transfers += copies
            self._split_up_volumes(
                copies, transfer.sample_volume, transfer.buffer_volume)

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
            t.buffer_volume = 0
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
                t.buffer_volume = float(
                    original_buffer_volume / number_rows)

            if original_sample_volume > self.dilution_settings.pipette_max_volume:
                t.sample_volume = float(
                    original_sample_volume / number_rows)

    def sorted_transfers(self, transfers):
        def pipetting_volume(transfer):
            return self._get_volume(transfer.buffer_volume) + self._get_volume(transfer.sample_volume)

        def max_added_pip_volume():
            volumes = map(lambda t: (self._get_volume(t.buffer_volume),
                                     self._get_volume(t.sample_volume)), self._transfers)
            return max(map(lambda (buffer_vol, sample_vol): buffer_vol + sample_vol, volumes))

        # Sort on source position, and in case of splitted rows, pipetting
        # volumes. Let max pipetting volumes be shown first
        max_vol = max_added_pip_volume()
        return sorted(
            transfers, key=lambda t: self.robot_deck_positioner.find_sort_number(t) +
            (max_vol - pipetting_volume(t)) / (max_vol + 1.0))

    @staticmethod
    def _get_volume(volume):
        # In cases when some parameter is not set (Source conc, Target concentration
        # or volume), let the error and warnings check in script
        # catch these exceptions
        if not volume:
            return 0
        else:
            return volume

    def do_positioning(self):
        # Handle positioning
        for transfer in self._transfers:
            transfer.source_well_index = self.robot_deck_positioner.indexer(
                transfer.source_well)
            transfer.source_plate_pos = self.robot_deck_positioner. \
                source_plate_position_map[transfer.source_container.id]
            transfer.target_well_index = self.robot_deck_positioner.indexer(
                transfer.target_well)
            transfer.target_plate_pos = self.robot_deck_positioner \
                .target_plate_position_map[transfer.target_container.id]

    def __str__(self):
        return "<DilutionScheme positioner={}>".format(self.robot_deck_positioner)

