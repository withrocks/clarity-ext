from clarity_ext.extensions import GeneralExtension
from clarity_ext.utils import lazyprop
import abc
import datetime


class DilutionExtension(GeneralExtension):
    """
    The base DilutionExtension is used by all DilutionExtensions. Override
    get_dilution_settings and get_robot_settings as required.

    Configuration requirements:
        Analyte udfs:
            On Input:
                Conc. Current (nM)
                Current sample volume (ul)
            On Output:
                Conc. Current (nM)
                Current sample volume (ul)
                Target conc. (nM)
                Target vol. (ul)
        ResultFile udfs:
            Has errors2
              Should have a default preset value of True or False,
              True will prevent user for going further without creating a
              driver file
        Step configuration, "Shared Output: ResultFile" (op. interface):
            Hamilton Driver File
            Biomek Driver File
            Metadata
            Step log
        Step configuration, "Output Details" tab (op. interface):
            Add listed udfs shall be ticked in the list

    Commit the dilution by running dilution/commit_dilution.py
    """
    def execute(self):
        # Upload all robot files:
        for robot in self.get_robot_settings():
            self.logger.info("Creating robot file for {}".format(robot.name))
            # We might get more than one file, in the case of a split
            robot_files = self.dilution_session.driver_files(robot.name)

            files = [(self._get_filename(robot.name, ext=robot.file_ext, seq=ix + 1),
                      f.to_string(include_header=False, new_line='\r\n'))
                      for ix, f in enumerate(robot_files)]
            self.context.upload_file_service.upload_files(robot.file_handle, files)

        # Upload the metadata file:
        metafile = self.generate_metadata_file()
        self.context.upload_file_service.upload("Metadata",
                                                self._get_filename("metadata", ".xml"),
                                                metafile,
                                                stdout_max_lines=3)

        # Need to update from the correct transfer_batch if there is more than one (the temporary one)
        # Update the temporary UDFs. These will only be committed when the script should finish
        for _, target, update_info in self.dilution_session.enumerate_update_infos():
            # Set the volume on the target to be committed. Not strictly required but clearer and less error-prone
            target.udf_dil_calc_target_vol = update_info.target_vol
            target.udf_dil_calc_target_conc = update_info.target_conc

            # Update the source vol on the target object. It will be moved to the source object later:
            target.udf_dil_calc_source_vol = update_info.updated_source_vol
            self.context.update(target)
        self.context.commit()

        # Ensure that errors and warnings are logged:
        # TODO: Ensure that the usage errors are reported correctly and the step log is filled
        # TODO: Handle errors
        # Update the error state for the next script, update_fields. It will not run if there
        #error_log_artifact = self.context.error_log_artifact
        #error_log_artifact.udf_has_errors2 = self.dilution_session.has_errors
        #self.context.update(error_log_artifact)
        #self.context.commit()

    @lazyprop
    def dilution_session(self):
        """
        Returns a validated DilutionSession with settings from the extension writer, defined in
        a subclass of this class.

        TODO: Do validation rules work for both update and create driver file as they are?
        """
        robot_settings = list(self.get_robot_settings())
        dilution_settings = self.get_dilution_settings()
        validator = self.get_validator()
        pairs = self.context.artifact_service.all_aliquot_pairs()
        session = self.context.dilution_service.create_session(robot_settings,
                                                               dilution_settings,
                                                               validator)
        session.evaluate(pairs)

        return session

    def output_file_name(self, context, robot_name, extension):
        initials = context.current_user.initials
        today = datetime.date.today().strftime("%y%m%d")
        # TODO: Naming standard for the generated files
        return "{}_DriverFile_{today}_{initials}_{pid}.{ext}".format(
            robot_name, initials=initials, today=today, pid=context.pid, ext=extension)

    @abc.abstractmethod
    def get_validator(self):
        pass

    @abc.abstractmethod
    def get_robot_settings(self):
        pass

    @abc.abstractmethod
    def get_dilution_settings(self):
        pass

    def generate_metadata_file(self):
        from clarity_ext.utils import get_jinja_template_from_package
        from clarity_ext_scripts.dilution.settings import MetadataInfo
        import clarity_ext_scripts.dilution.resources as templates_module
        #from clarity_ext_scripts.dilution.settings import
        template = get_jinja_template_from_package(templates_module, "metadata.xml.j2")
        metadata_info = MetadataInfo(self.dilution_session,
                                     "somefile_TODO", self.context.current_user, self.context)

        # NOTE: Using the dilution_scheme for hamilton when generating the metadata file, temporary
        # TODO: Rename scheme to batch
        from clarity_ext import utils
        # TODO: Change this to support more transfer batches!
        transfer_batch = self.dilution_session.single_robot_transfer_batches_for_update()[0]

        metafile = self.dilution_session.create_general_driver_file(
            template,
            info=metadata_info,
            scheme=transfer_batch,
            context=self.context)
        return metafile

    def _get_filename(self, prefix, ext=".txt", seq=""):
        # TODO: Naming standard
        initials = self.context.current_user.initials
        today = datetime.date.today().strftime("%y%m%d")
        pid = self.context.pid
        return "{prefix}{seq}_{today}_{initials}_{pid}{ext}".format(
            prefix=prefix, seq=seq, initials=initials, today=today, pid=pid, ext=ext)


