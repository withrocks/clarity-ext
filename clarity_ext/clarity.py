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
    """
    A wrapper around connections to Clarity.

    :param api: A proxy for the REST API, looking like Lims from the genologics package.
    :param current_step_id: The step we're currently in.
    """
    def __init__(self, user_config_path="~/.clarity-ext.user.config", server_config_path=".clarity-ext.config"):
        self.server_config_path = server_config_path
        self.server_config = self.fetch_server_config()
        self.user_config_path = user_config_path
        self.user_config = self.fetch_user_config()
        self.current_step = None 

    # TODO: test all of these, mostly broken 
    def login_with_user_config(self, environment, current_step_id=None):
        token = self.user_config[environment]["token"]
        username = self.user_config[environment]["username"]
        baseuri = self.server_config["environments"][environment]["server"]
        return ClaritySession(Lims(baseuri, username, None, auth_token=token), current_step_id)

    def login_with_genologics_config(current_step_id=None):
        """Uses the default password file configured for pip/genologics"""
        self.api = Lims(BASEURI, USERNAME, PASSWORD)

    def set_current_step(current_step_id):
        self.current_step_id = current_step_id  # TODO: refactor
        process_api_resource = genologics.entities.Process(self.api, id=self.current_step_id)
        self.current_step = Process.create_from_rest_resource(process_api_resource)

    def get(self, endpoint):
        """
        Executes a GET via the REST interface. One should rather use the api attribute instead.
        The endpoint is the part after /api/<version>/ in the API URI.
        """
        url = "{}/api/v2/{}".format(BASEURI, endpoint)
        return requests.get(url, auth=(USERNAME, PASSWORD))

    def fetch_server_config(self):
        print(self.server_config_path)
        if os.path.exists(self.server_config_path):
            with open(self.server_config_path, "r") as f:
                return yaml.load(f)
        else:
            return dict()

    def login(self, environment, username, password):
        """Logs the user in to the specified environment and saves the credentials in the file system.

        Interactively asks for username and password if they are not provided"""
        env = self.server_config["environments"][environment]

        if not username:
            # TODO: py3!
            username = raw_input("username: ")
        if not password:
            password = getpass("password: ")

        auth = _basic_auth_str(username, password)
        headers = {"Authorization": auth}
        resp = requests.get("{}/api/v2/".format(env["server"]), headers=headers)

        if resp.status_code == 200:
            self.user_config[environment] = {"token": auth, "username": username}
            self.save_user_config(self.user_config)
            print("Wrote user config to {}".format(self.user_config_path))
        else:
            print("Login unsuccessful")

    def fetch_user_config(self):
        fpath = os.path.expanduser(self.user_config_path)
        if os.path.exists(fpath):
            with open(fpath, "r") as fs:
                return yaml.load(fs)
        else:
            return dict()

    def save_user_config(self, user_config):
        fpath = os.path.expanduser(self.user_config_path)
        with open(fpath, "w") as fs:
            yaml.safe_dump(user_config, fs, default_flow_style=False)
        os.chmod(fpath, 0600)


class SessionException(Exception):
    pass
