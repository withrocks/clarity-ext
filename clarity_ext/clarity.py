from genologics.lims import Lims
from genologics.config import BASEURI, USERNAME, PASSWORD
import genologics.entities
import requests
from clarity_ext.domain.process import Process


class ClaritySession(object):
    """
    A wrapper around connections to Clarity.

    :param api: A proxy for the REST API, looking like Lims from the genologics package.
    :param current_step_id: The step we're currently in.
    """
    def __init__(self, api, current_step_id):
        self.api = api
        api.check_version()
        self.current_step_id = current_step_id
        if current_step_id:
            process_api_resource = genologics.entities.Process(self.api, id=current_step_id)
            self.current_step = Process.create_from_rest_resource(process_api_resource)

    @staticmethod
    def create(current_step_id):
        return ClaritySession(Lims(BASEURI, USERNAME, PASSWORD), current_step_id)

    def get(self, endpoint):
        """
        Executes a GET via the REST interface. One should rather use the api attribute instead.
        The endpoint is the part after /api/<version>/ in the API URI.
        """
        url = "{}/api/v2/{}".format(BASEURI, endpoint)
        return requests.get(url, auth=(USERNAME, PASSWORD))
