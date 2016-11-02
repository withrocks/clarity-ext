ROBOT_MIN_VOLUME = 2


class FixedVolumeCalc:
    """
    Implements sample volume calculations for transfer only dilutions.
    I.e. no calculations at all. The fixed transfer volume is specified in
    individual scripts
    """

    def calculate_transfer_volumes(self, transfers=None, scale_up_low_volumes=None):
        """
        Do nothing
        """
        pass


class OneToOneConcentrationCalc:
    """
    Implements sample volume calculations for a one to one dilution,
    referring that a single transfer has a single source well/tube
    and a single destination well/tube, i.e. no pooling involved.
    Transfer volumes are calculated on basis of a requested target
    concentration and target volume by the user.
    """

    def __init__(self):
        pass

    def calculate_transfer_volumes(self, transfers=None, scale_up_low_volumes=None):
        for transfer in transfers:
            try:
                transfer.sample_volume = \
                    transfer.requested_concentration * transfer.requested_volume / \
                    transfer.source_concentration
                transfer.buffer_volume = \
                    max(transfer.requested_volume - transfer.sample_volume, 0)
                transfer.has_to_evaporate = \
                    (transfer.requested_volume - transfer.sample_volume) < 0
                if scale_up_low_volumes and transfer.sample_volume < ROBOT_MIN_VOLUME:
                    scale_factor = float(
                        ROBOT_MIN_VOLUME / transfer.sample_volume)
                    transfer.sample_volume *= scale_factor
                    transfer.buffer_volume *= scale_factor
                    transfer.scaled_up = True
            except (TypeError, ZeroDivisionError) as e:
                transfer.sample_volume = None
                transfer.buffer_volume = None
                transfer.has_to_evaporate = None
