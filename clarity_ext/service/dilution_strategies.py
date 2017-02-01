# These values, residing in SingleTransfer objects, are calculated in
# the these strategy classes (except FixedVolumeCalc)
#
# Variables to be used in driver file and udf update calculations:
#   sample_volume,
#   the volume in ul taken from the source sample into the destination
#
#   buffer_volume,
#   the volume in ul taken from water/buffer/EB
#
# Variables to serve as basis of producing warning messages at validation.
#   has_to_evaporate,
#   Is true when the specified required concentration exceeds the source
#   concentration
#
#   scaled_up,
#   When a sample volume is less than the pipetting min volume of 2 ul,
#   both sample volume and buffer volume will be scaled up, still rendering
#   the right concentration but higher volume than desired.

from itertools import groupby

ROBOT_MIN_VOLUME = 2


class FixedVolumeCalc:
    """
    Implements sample volume calculations for transfer only dilutions.
    I.e. no calculations at all. The fixed transfer volume is specified in
    individual scripts
    """

    def calculate_transfer_volumes(self, batch=None, dilution_settings=None):
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

    def calculate_transfer_volumes(self, batch=None, dilution_settings=None):
        for transfer in batch.transfers:
            transfer.sample_volume = \
                transfer.requested_concentration * transfer.requested_volume / \
                transfer.source_concentration
            transfer.buffer_volume = \
                max(transfer.requested_volume - transfer.sample_volume, 0)
            transfer.has_to_evaporate = \
                (transfer.requested_volume - transfer.sample_volume) < 0
            if dilution_settings.scale_up_low_volumes and transfer.sample_volume < ROBOT_MIN_VOLUME:
                scale_factor = float(
                    ROBOT_MIN_VOLUME / transfer.sample_volume)
                transfer.sample_volume *= scale_factor
                transfer.buffer_volume *= scale_factor
                transfer.scaled_up = True


class PoolConcentrationCalc:
    """
    Comments on key variable calculations:
    * sample_volume,
      depends on the number of samples contained within the pool, otherwise
      like a one-to-one dilution
    * buffer_volume,
      based on requested volume and all sample volumes within the pool.
      The buffer volume is specified at one single row in the driver
      file, chosen at random among samples within the pool.
    * has_to_evaporate,
      same as previous, based on requested volume and all sample
      volumes within the pool
    * scaled_up,
      the scale up factor is based on the min sample volume within the pool.
      If volumes are to be scaled up, all sample volumes and the buffer volume
      have to be scaled up.
    """

    def __init__(self):
        pass

    def calculate_transfer_volumes(self, transfers=None, scale_up_low_volumes=None):
        transfers = sorted(transfers, key=lambda t: t.target_aliquot_name)
        grouped_transfers = groupby(
            transfers, key=lambda t: t.target_aliquot_name)
        for key, group in grouped_transfers:
            self._calc_single_pool(list(group), scale_up_low_volumes)

    def _calc_single_pool(self, transfers, scale_up_low_volumes):
        """
        Calculate transfer volumes for a single pool
        :param transfers: A list of transfers associated with a single pool
        :return:
        """
        for transfer in transfers:
            try:
                transfer.sample_volume = \
                    transfer.requested_concentration * transfer.requested_volume / \
                    transfer.source_concentration / len(transfers)
                transfer.buffer_volume = 0
            except (TypeError, ZeroDivisionError):
                transfer.sample_volume = 0
                transfer.buffer_volume = 0
                transfer.has_to_evaporate = False

        t = transfers[0]
        t.buffer_volume = max(t.requested_volume -
                              sum([tt.sample_volume for tt in transfers]), 0)

        min_sample_volume = min(map(lambda t: t.sample_volume, transfers))
        for t in transfers:
            t.has_to_evaporate = t.requested_volume - \
                sum([tt.sample_volume for tt in transfers]) < 0
            try:
                if scale_up_low_volumes is True and min_sample_volume < ROBOT_MIN_VOLUME:
                    scale_factor = float(ROBOT_MIN_VOLUME/min_sample_volume)
                    t.sample_volume *= scale_factor
                    t.buffer_volume *= scale_factor
                    t.scaled_up = True
            except ZeroDivisionError:
                # TODO: Move this catch-all exception handler. Can cause difficult to debug issues
                # when code is refactored

                # Zero sample volume indicates an error that should be caught
                # later by validation
                pass
