from clarity_ext.domain.validation import ValidationException, ValidationType

DILUTION_WASTE_VOLUME = 1
ROBOT_MIN_VOLUME = 2


class TransferEndpoint(object):
    """
    TransferEndpoint wraps an source or destination analyte involved in a dilution
    """
    # TODO: Handle tube racks

    def __init__(self, analyte):
        self.sample_name = analyte.name
        self.well = analyte.well
        self.container = analyte.container
        self.concentration = analyte.concentration
        self.volume = analyte.volume
        self.requested_concentration = analyte.target_concentration
        self.requested_volume = analyte.target_volume
        self.well_index = None
        self.plate_pos = None


class SingleTransfer(object):
    # Enclose sample data, user input and derived variables for a
    # single row in a dilution

    def __init__(self, source_endpoint, destination_endpoint=None):
        self.sample_name = source_endpoint.sample_name

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

    def find_sort_number(self, dilute):
        """Sort dilutes according to plate and well positions
        """
        plate_base_number = self._plate_size.width * self._plate_size.height + 1
        plate_sorting = self.plate_sorting_map[
            dilute.source_container.id
        ]
        # Sort order for wells are always based on down first indexing
        # regardless the robot type
        return plate_sorting * plate_base_number + dilute.source_well.index_down_first

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


class SourceOnlyDilutionScheme(object):
    """
    Creates a dilution scheme for a source only dilution (qPCR)
    Here, the destination position doesn't matter. Only columns for
    the source position and a fixed transfer volume is showed in the
    driver file
    """

    def __init__(self, artifact_service, robot_name):
        input_analytes = artifact_service.all_input_analytes()
        container = input_analytes[0].container
        self.transfers = self.create_transfers(input_analytes)
        self.analyte_by_transfer = {dilute: analyte for dilute, analyte in
                                    zip(self.transfers, input_analytes)}
        transfer_endpoints = [
            dilute.source_endpoint for dilute in self.transfers]
        self.source_positioner = EndpointPositioner(robot_name, transfer_endpoints,
                                                    container.size, "DNA")
        self.do_positioning()

    def create_transfers(self, input_analytes):
        transfers = []
        for analyte in input_analytes:
            source_endpoint = TransferEndpoint(analyte)
            transfers.append(SingleTransfer(source_endpoint))
        return transfers

    def do_positioning(self):
        for transfer in self.transfers:
            transfer.source_well_index = self.source_positioner.indexer(
                transfer.source_well)
            transfer.source_plate_pos = self.source_positioner. \
                plate_position_map[transfer.source_container.id]

        self.transfers = sorted(self.transfers,
                                key=lambda curr_dil: self.source_positioner.find_sort_number(curr_dil))


class DilutionScheme(object):
    """Creates a dilution scheme, given input and output analytes."""

    def __init__(self, artifact_service, robot_name, scale_up_low_volumes=True):
        """
        Calculates all derived values needed in dilute driver file.
        """
        self.scale_up_low_volumes = scale_up_low_volumes
        pairs = artifact_service.all_analyte_pairs()

        # TODO: Is it safe to just check for the container for the first output
        # analyte?
        container = pairs[0].output_artifact.container
        self.transfers = self.create_transfers(pairs)

        self.analyte_pair_by_transfer = {
            transfer: pair for transfer, pair in zip(self.transfers, pairs)}
        self.robot_deck_positioner = RobotDeckPositioner(
            robot_name, self.transfers, container.size)

        self.calculate_transfer_volumes()
        self.do_positioning()

    def create_transfers(self, analyte_pairs):
        # TODO: handle tube racks
        transfers = []
        for pair in analyte_pairs:
            source_endpoint = TransferEndpoint(pair.input_artifact)
            destination_endpoint = TransferEndpoint(pair.output_artifact)
            transfers.append(SingleTransfer(
                source_endpoint, destination_endpoint))
        return transfers

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
                    scale_factor = float(ROBOT_MIN_VOLUME / transfer.sample_volume)
                    transfer.sample_volume *= scale_factor
                    transfer.buffer_volume *= scale_factor
                    transfer.scaled_up = True
            except (TypeError, ZeroDivisionError) as e:
                transfer.sample_volume = None
                transfer.buffer_volume = None
                transfer.has_to_evaporate = None

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

        self.transfers = sorted(self.transfers,
                                key=lambda curr_dil: self.robot_deck_positioner.find_sort_number(curr_dil))

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
