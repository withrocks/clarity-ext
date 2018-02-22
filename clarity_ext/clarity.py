from genologics.lims import Lims
from genologics.config import BASEURI, USERNAME, PASSWORD
import genologics.entities
from clarity_ext.domain.process import Process
import os
import yaml
from getpass import getpass
import requests
from requests.auth import _basic_auth_str


class ClaritySession(object):
    """A wrapper around connections to Clarity."""

    # TODO: use default server by default, then set up default servers on dev, staging and prod. This
    # means we don't have to change the configured extensions (as they don't have to specify env)
    # TODO: discontinue genologicsrc in favor of this (probably needs to be available though)
    # TODO: first search in .clarity-ext.config then in /etc/clarity-ext.config

    def __init__(self, config):
        self.config = config
        self.current_step = None
        self.current_step_id = None
        self.environment = None
        self._api = None  # TODO: Error when the user tries to access api when it hasn't been set

    @property
    def api(self):
        if self._api is None:
            raise Exception("The underlying api hasn't been initialized, you must call one of the login methods")
        return self._api

    def login_with_user_config(self, environment=None, current_step_id=None):
        environment = environment or self.config.default
        token = self.config.auth_token(environment)["token"]
        username = self.config.user_config["environments"][environment]["username"]
        baseuri = self.config.global_config["environments"][environment]["server"]
        self._api = Lims(baseuri, username, None, auth_token=token)
        self.set_current_step(current_step_id)
        self.current_step_id = None
        self.environment = environment

    # def login_with_genologics_config(self, current_step_id=None):
    #     """Uses the default password file configured for pip/genologics"""
    #     self._api = Lims(BASEURI, USERNAME, PASSWORD)
    #     self.set_current_step(current_step_id)

    def set_current_step(self, current_step_id):
        self.current_step_id = current_step_id  # TODO: refactor
        if self.current_step_id:
            process_api_resource = genologics.entities.Process(self.api, id=self.current_step_id)
            self.current_step = Process.create_from_rest_resource(process_api_resource)

    def get(self, endpoint):
        """
        Executes a GET via the REST interface. One should rather use the api attribute instead.
        The endpoint is the part after /api/<version>/ in the API URI.
        """
        url = "{}/api/v2/{}".format(BASEURI, endpoint)
        return requests.get(url, auth=(USERNAME, PASSWORD))

    def login(self, environment, username, password):
        """Logs the user in to the specified environment and saves the credentials in the file system.

        Interactively asks for username and password if they are not provided"""
        env = self.config.global_config[environment]

        if not username:
            # TODO: py3!
            username = raw_input("username: ")
        if not password:
            password = getpass("password: ")

        auth = _basic_auth_str(username, password)
        headers = {"Authorization": auth}
        resp = requests.get("{}/api/v2/".format(env["server"]), headers=headers)

        if resp.status_code == 200:
            self.config.user_config["environments"][environment] = {"token": auth, "username": username}
            self.config.save()


class Configuration(object):
    """Configuration for clarity-ext. Wraps the user specific configuration as well as server configuration."""
    def __init__(self, user_config_path="~/.clarity-ext.user.config", server_config_path=".clarity-ext.config"):
        self.global_config_path = os.path.expanduser(server_config_path)
        self.global_config = self._fetch_config(self.global_config_path)
        self.user_config_path = os.path.expanduser(user_config_path)
        self.user_config = self._fetch_config(self.user_config_path)

        default = [k for k, v in self.global_config["environments"].items() if v["default"]]
        if len(default) > 1:
            raise Exception("More than one environment configured as default: {}".format(default))
        self.default = default[0]

    def set_environment(self, name, server, default, role):
        self.global_config["environments"][name] = {"server": server, "default": default, "role": role}

    def _fetch_config(self, fpath):
        """Fetches the user or env config, returning a default config skeleton if it's not available in the
        file system"""
        if os.path.exists(fpath):
            with open(fpath, "r") as fs:
                return yaml.load(fs)
        else:
            return dict(environments=dict())

    def get_environment_config(self, environment):
        if environment not in self.user_config:
            raise EnvironmentNotConfiguredException(environment)
        return self.user_config[environment]

    def auth_token(self, environment):
        try:
            return self.user_config["environments"][environment]
        except KeyError:
            raise NoAuthTokenConfigured("No auth token available for {}".format(environment))

    def save(self):
        self._save(self.global_config, self.global_config_path)
        self._save(self.user_config, self.user_config_path, mode=0600)

    def _save(self, config_obj, path, mode=None):
        fpath = os.path.expanduser(path)
        with open(fpath, "w") as fs:
            yaml.safe_dump(config_obj, fs, default_flow_style=False)
        if mode:
            os.chmod(fpath, mode)


class EnvironmentNotConfiguredException(Exception):
    pass

class NoAuthTokenConfigured(Exception):
    pass


class SessionException(Exception):
    pass
