from clarity_ext.service.validation_service import ERRORS_AND_WARNING_ENTRY_NAME
from clarity_ext.dilution import DilutionScheme, RobotDeckPositioner
from StringIO import StringIO


class DilutionService(object):
    # TODO: Move service to dilution? (or that module here)
    ROBOT_HAMILTON = "Hamilton"
    ROBOT_BIOMEK = "Biomek"

    def __init__(self, artifact_service):
        self.dilution_scheme = None
        self.artifact_service = artifact_service

    def validate_can_execute_in_context(self):
        """
        Call this at the start of an extension to ensure that the step has been correctly set up
        for dilution
        """
        # TODO: This should not be required until later. The user should be able to create a dilutionscheme
        # without requiring a certain error file
        file_list = [file for file in self.artifact_service.shared_files() if file.name ==
                     ERRORS_AND_WARNING_ENTRY_NAME]
        if not len(file_list) == 1:
            raise ValueError("This step is not configured with the shared file entry for {}".format(
                ERRORS_AND_WARNING_ENTRY_NAME))
        error_log_artifact = file_list[0]
        # TODO: error_log_artifact was part of the dilution_scheme. Errors should be wired back to the context
        # in a separate call, not as a part if initializing a dilution scheme...

    def init_dilution_scheme(self, pairs, concentration_ref=None, include_blanks=False,
                             volume_calc_method=None, make_pools=False):
        """
        Creates a new DilutionScheme object, which can be used to create driver files
        and execute the dilution on a set of analytes.
        """
        self.dilution_scheme = DilutionScheme.create(
            pairs,
            concentration_ref=concentration_ref, include_blanks=include_blanks,
            volume_calc_method=volume_calc_method, make_pools=make_pools)
        return self.dilution_scheme

    def create_robot_driver_file(self, dilution_scheme, robot, container_size):
        """
        Returns a driver file for the robot as a file-like object
        """
        robot_deck_positioner = RobotDeckPositioner(robot, dilution_scheme.transfers, container_size)
        lines = list()
        for transfer in robot_deck_positioner.enumerate_split_row_transfers(dilution_scheme):
            row = [transfer.aliquot_name,
                   "{}".format(transfer.source_well_index),
                   transfer.source_plate_pos,
                   "{:.1f}".format(transfer.sample_volume),
                   "{:.1f}".format(transfer.buffer_volume),
                   "{}".format(transfer.target_well_index),
                   transfer.target_plate_pos]
            lines.append("\t".join(row))
        content = "\n".join(lines)
        return StringIO(content)


