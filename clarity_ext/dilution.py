from clarity_ext.domain.validation import ValidationException, ValidationType
import copy
from clarity_ext.utils import get_and_apply

DILUTION_WASTE_VOLUME = 1
ROBOT_MIN_VOLUME = 2
PIPETTING_MAX_VOLUME = 50


class TransferEndpoint(object):
    """
    TransferEndpoint wraps an source or destination analyte involved in a dilution
    """
    # TODO: Handle tube racks

    def __init__(self, aliquot):
        self.sample_name = aliquot.name
        self.well = aliquot.well
        self.container = aliquot.container
        self.concentration = aliquot.concentration
        self.volume = aliquot.volume
        self.is_control = False
        if hasattr(aliquot, "is_control"):
            self.is_control = aliquot.is_control
        self.requested_concentration = get_and_apply(
            aliquot.__dict__, "target_concentration", None, float)
        self.requested_volume = get_and_apply(
            aliquot.__dict__, "target_volume", None, float)
        self.well_index = None
        self.plate_pos = None


class SingleTransfer(object):
    # Enclose sample data, user input and derived variables for a
    # single row in a dilution

    def __init__(self, source_endpoint, destination_endpoint=None, pair_id=None):
        self.pair_id = pair_id
        self.sample_name = source_endpoint.sample_name
        self.is_control = source_endpoint.is_control

        self.source_endpoint = source_endpoint
        self.destination_endpoint = destination_endpoint

        self.source_well = source_endpoint.well
        self.source_container = source_endpoint.container
        self.source_concentration = source_endpoint.concentration
        self.source_initial_volume = source_endpoint.volume
        self.source_well_index = None
        self.source_plate_pos = None

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

    def __str__(self):
        source = "source({}/{}, conc={})".format(self.source_container,
                                                 self.source_well, self.source_concentration)
        target = "target({}/{}, conc={}, vol={})".format(self.target_container, self.target_well,
                                                         self.requested_concentration, self.requested_volume)
        return "{} => {}".format(source, target)

    def __repr__(self):
        return "<SingleTransfer {}>".format(self.sample_name)


class EndpointPositioner(object):
    """
    Handles positions for all plates and wells for either source or
    destination placement on a robot deck
    """

    def __init__(self, robot_name, transfer_endpoints, plate_size, plate_pos_prefix):
        self.robot_name = robot_name
        self._plate_size = plate_size
        index_method_map = {"Hamilton": lambda well: well.index_down_first}
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

        source_endpoints = [dilute.source_endpoint for dilute in dilutes]
        source_positioner = EndpointPositioner(robot_name, source_endpoints,
                                               plate_size, "DNA")
        destination_endpoints = [
            dilute.destination_endpoint for dilute in dilutes]
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


class DilutionScheme(object):
    """Creates a dilution scheme, given input and output analytes."""

    def __init__(self, artifact_service, robot_name, scale_up_low_volumes=True):
        """
        Calculates all derived values needed in dilute driver file.
        """
        self.scale_up_low_volumes = scale_up_low_volumes
        pairs = artifact_service.all_aliquot_pairs()

        # TODO: Is it safe to just check for the container for the first output
        # analyte?
        container = pairs[0].output_artifact.container
        all_transfers = self._create_transfers(pairs)
        self.transfers = list(t for t in all_transfers if t.is_control is False)

        self.aliquot_pair_by_transfer = self._map_pair_and_transfers(pairs=pairs)
        self.robot_deck_positioner = RobotDeckPositioner(
            robot_name, self.transfers, container.size)

        self.calculate_transfer_volumes()
        self.split_up_high_volume_rows()
        self.do_positioning()
        self.sort_transfers()

    def _create_transfers(self, aliquot_pairs):
        # TODO: handle tube racks
        transfers = []
        for pair in aliquot_pairs:
            source_endpoint = TransferEndpoint(pair.input_artifact)
            destination_endpoint = TransferEndpoint(pair.output_artifact)
            transfers.append(SingleTransfer(
                source_endpoint, destination_endpoint, pair_id=id(pair)))
        return transfers

    def _map_pair_and_transfers(self, pairs):
        pair_dict = {id(pair): pair for pair in pairs}
        return {transfer: pair_dict[transfer.pair_id] for transfer in self.transfers}

    def calculate_transfer_volumes(self):
        # Handle volumes etc.
        for transfer in self.transfers:
            try:
                transfer.sample_volume = \
                    transfer.requested_concentration * transfer.requested_volume / \
                    transfer.source_concentration
                transfer.buffer_volume = \
                    max(transfer.requested_volume - transfer.sample_volume, 0)
                transfer.has_to_evaporate = \
                    (transfer.requested_volume - transfer.sample_volume) < 0
                if self.scale_up_low_volumes and transfer.sample_volume < ROBOT_MIN_VOLUME:
                    scale_factor = float(
                        ROBOT_MIN_VOLUME / transfer.sample_volume)
                    transfer.sample_volume *= scale_factor
                    transfer.buffer_volume *= scale_factor
                    transfer.scaled_up = True
            except (TypeError, ZeroDivisionError) as e:
                transfer.sample_volume = 0
                transfer.buffer_volume = 0
                transfer.has_to_evaporate = False

    def split_up_high_volume_rows(self):
        """
        Split up a transfer between source well x and target well y into
        several rows, if sample volume or buffer volume exceeds 50 ul
        :return:
        """
        added_transfers = []
        for transfer in self.transfers:
            calculation_volume = max(
                self._get_volume(transfer.sample_volume), self._get_volume(transfer.buffer_volume))
            (n, residual) = divmod(calculation_volume, PIPETTING_MAX_VOLUME)
            if residual > 0:
                total_rows = int(n + 1)
            else:
                total_rows = int(n)

            copies = self._create_copies(transfer, total_rows)
            added_transfers += copies
            self._split_up_volumes(
                [transfer] + copies, transfer.sample_volume, transfer.buffer_volume)

        self.transfers += added_transfers

    @staticmethod
    def _create_copies(transfer, total_rows):
        """
        Create copies of transfer, if duplication_number > 1,
        that will cause extra rows in the driver file.
        Initiate both buffer volume and sample volume to zero
        :param transfer: The transfer to be copied
        :param total_rows: The total number of rows needed for a
        transfer between source well x and target well y
        :return:
        """
        copies = []
        for i in xrange(0, total_rows - 1):
            t = copy.copy(transfer)
            t.buffer_volume = 0
            t.sample_volume = 0
            copies.append(t)

        return copies

    @staticmethod
    def _split_up_volumes(transfers, original_sample_volume, original_buffer_volume):
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
            if original_buffer_volume > PIPETTING_MAX_VOLUME:
                t.buffer_volume = float(
                    original_buffer_volume / number_rows)

            if original_sample_volume > PIPETTING_MAX_VOLUME:
                t.sample_volume = float(
                    original_sample_volume / number_rows)

    def sort_transfers(self):
        def pipetting_volume(transfer):
            return self._get_volume(transfer.buffer_volume) + self._get_volume(transfer.sample_volume)

        def max_added_pip_volume():
            volumes = map(lambda t: (self._get_volume(t.buffer_volume),
                                     self._get_volume(t.sample_volume)), self.transfers)
            return max(map(lambda (buffer_vol, sample_vol): buffer_vol + sample_vol, volumes))

        # Sort on source position, and in case of splitted rows, pipetting
        # volumes. Let max pipetting volumes be shown first
        max_vol = max_added_pip_volume()
        self.transfers = sorted(self.transfers,
                                key=lambda t:
                                self.robot_deck_positioner.find_sort_number(t) +
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
        for transfer in self.transfers:
            transfer.source_well_index = self.robot_deck_positioner.indexer(
                transfer.source_well)
            transfer.source_plate_pos = self.robot_deck_positioner. \
                source_plate_position_map[transfer.source_container.id]
            transfer.target_well_index = self.robot_deck_positioner.indexer(
                transfer.target_well)
            transfer.target_plate_pos = self.robot_deck_positioner \
                .target_plate_position_map[transfer.target_container.id]

    def validate(self):
        """
        Yields validation errors or warnings

        TODO: These validation errors should not be in clarity-ext (implementation specific)
        """
        def pos_str(transfer):
            return "{}=>{}".format(transfer.source_well, transfer.target_well)

        for transfer in self.transfers:
            if not transfer.source_initial_volume:
                yield ValidationException("Source volume is not set: {}".format(transfer.source_well))
            if not transfer.source_concentration:
                yield ValidationException("Source concentration not set: {}".format(transfer.source_well))
            else:
                if transfer.sample_volume < 2:
                    yield ValidationException("Too low sample volume: " + pos_str(transfer))
                elif transfer.sample_volume > 50:
                    yield ValidationException("Too high sample volume: " + pos_str(transfer))
                if transfer.has_to_evaporate:
                    yield ValidationException("Sample has to be evaporated: " + pos_str(transfer), ValidationType.WARNING)
                if transfer.buffer_volume > 50:
                    yield ValidationException("Too high buffer volume: " + pos_str(transfer))

    def __str__(self):
        return "<DilutionScheme positioner={}>".format(self.robot_deck_positioner)
