from genologics.lims import Lims
from genologics.config import BASEURI, USERNAME, PASSWORD
from genologics.entities import Process


class ClaritySession(object):
    """
    A wrapper around connections to Clarity.
    """
    def __init__(self, api, current_step):
        self.api = api
        api.check_version()
        self.current_step = Process(self.api, id=current_step)

    @staticmethod
    def create(current_step):
        return ClaritySession(Lims(BASEURI, USERNAME, PASSWORD), current_step)

