import os
import yaml


class IntegrationConfig:
    def __init__(self):
        dir_path = os.path.dirname(os.path.realpath(__file__))
        path = os.path.join(dir_path, "clarity_steps.yml")
        with open(path, 'r') as fs:
            self.config_dict = yaml.load(fs)

    def get_by_name(self, name):
        for step in self.config_dict["steps"]:
            if step["name"] == name:
                return AttrDict(step)
        raise KeyError()


class AttrDict(dict):
    def __init__(self, *args, **kwargs):
        super(AttrDict, self).__init__(*args, **kwargs)
        self.__dict__ = self

config = IntegrationConfig()
