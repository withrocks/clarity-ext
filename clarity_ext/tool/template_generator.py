import os
from clarity_ext.resources import templates
import logging
import shutil
import codecs
from jinja2 import Template
from clarity_ext.integration import ConfigFromConventionProvider


logger = logging.getLogger(__name__)


class TemplateGenerator(object):
    """Generates extensions from templates available in clarity-ext/resources/templates"""
    def __init__(self):
        pass

    def list_templates(self):
        """Lists all extension templates defined in the application"""
        templates_dir = os.path.dirname(templates.__file__)
        return [ExtensionTemplate(template_dir)
                for template_dir in self._iterate_directories(templates_dir)]

    def create(self, template_name, qualified_module):
        """
        Creates the template in the module provided. The package of the module is created
        if required. E.g. if you provide package.subpackage.module, package and subpackage will be created
        as packages and module will be created as a python file (module).

        Extensions are not overwritten if they already exist, but the PyCharm run configuration will be overwritten.
        """
        template = self.find_by_name(template_name)
        split = qualified_module.split(".")
        if len(split) > 1:
            packages = split[0:-1]
        else:
            packages = []
        module = split[-1]

        root_path = os.getcwd()
        for ix in xrange(len(packages)):
            full_path = os.path.join(root_path, *packages[0:ix + 1])
            self._create_py_package(full_path)

        template_target_root = os.path.join(root_path, *packages)

        # All files that end with "j2" should be copied over
        for source_file_name in os.listdir(template.template_dir):
            source_file_name_no_ext, ext = os.path.splitext(source_file_name)
            if ext == ".j2":
                target_file_name = source_file_name_no_ext.replace("extension", module)
                template_source_path = os.path.join(template.template_dir, source_file_name)
                template_target_path = os.path.join(template_target_root, target_file_name)

                if os.path.exists(template_target_path):
                    logger.info("File already exists at '{}'".format(template_target_path))
                else:
                    logger.info("Copying template file '{}'".format(template_target_path))
                    shutil.copy(template_source_path, template_target_path)

        self._install_pycharm_configuration(qualified_module)

    def fix_pycharm(self, root_pkg):
        """Fixes the PyCharm run configurations for all extensions in the module"""
        for info in ConfigFromConventionProvider.get_extension_config(root_pkg):
            self._install_pycharm_configuration(info["module"])

    def _install_pycharm_configuration(self, module):
        """
        Installs a pycharm configuration for the extension.

        This is a no-op if there is no pycharm (.idea) directory in the current directory.
        """
        logger.info("Installing pycharm configuration for '{}'".format(module))
        module_split = module.split(".")

        # For brevity, we ignore the first name in the module if it contains more than 2 elements:
        module_split_shorter = module_split[1:] if len(module_split) > 2 else module_split
        root_path = os.getcwd()
        pycharm_path = os.path.join(root_path, ".idea")
        if not os.path.exists(pycharm_path):
            logger.info("No pyCharm installation found.")
            return

        pycharm_shared_path = os.path.join(pycharm_path, "runConfigurations")
        if not os.path.exists(pycharm_shared_path):
            logger.info("Creating directory for shared run configurations '{}'".format(pycharm_shared_path))
            os.makedirs(pycharm_shared_path)

        configuration_name = ".".join(module_split_shorter)
        file_name = "{}.xml".format("_".join(module_split_shorter))
        run_configuration_path = os.path.join(pycharm_shared_path, file_name)
        base_template = self.find_by_name("_base")
        template_config = os.path.join(base_template.template_dir, "pycharm_config.xml.j2")

        with open(template_config, "r") as from_fs:
            text = codecs.decode(from_fs.read(), "utf-8")
            template = Template(text)
            rendered = template.render(folder_name=module_split_shorter[0],
                                       extension_module=module,
                                       module_name=os.path.basename(root_path),
                                       configuration_name=configuration_name,
                                       debug_level="INFO",
                                       use_cache="True")

        with open(run_configuration_path, "w") as to_fs:
            to_fs.write(rendered)
        logger.debug("Updated the PyCharm configuration file")

    def find_by_name(self, template_name):
        try:
            template = [template for template in self.list_templates()
                        if template.name == template_name][0]
            return template
        except IndexError:
            raise TemplateNotFoundException(
                "Can't find a template called '{}'".format(template_name))

    @staticmethod
    def _iterate_directories(root):
        """Enumerates the full path of all directories from root"""
        for directory in os.listdir(root):
            full_path = os.path.join(root, directory)
            if os.path.isdir(full_path):
                yield full_path

    def _create_py_package(self, path):
        """Creates a py package at the path, creating the directory if it doesn't exist"""
        if os.path.exists(path):
            logger.info("Directory '{}' already exists".format(path))
        else:
            logger.info("Directory '{}' doesn't exist, creating it...".format(path))
            os.mkdir(path)

        init_file = "__init__.py"
        init_file_path = os.path.join(path, init_file)
        if os.path.exists(init_file_path):
            logger.info("Init file already exists in directory")
        else:
            logger.info("Creating {} in directory".format(init_file))
            with open(init_file_path, 'a'):
                pass


class ExtensionTemplate(object):
    """Describes an extension template"""
    def __init__(self, template_dir):
        self.template_dir = template_dir
        self.name = os.path.basename(self.template_dir)
        try:
            with open(os.path.join(self.template_dir, "description.txt")) as fs:
                self.description = fs.read()
        except IOError:
            self.description = "<Description missing>"

    def __repr__(self):
        return "{}: {}".format(self.name, self.description)


class TemplateNotFoundException(Exception):
    pass
