import requests_cache
import clarity_ext
import xml.etree.ElementTree as ET
import logging
import re


class ProcessService(object):
    """Provides access to information about processes and process types"""
    def __init__(self, logger=None, cache=None, session=None):
        self.logger = logger or logging.getLogger(__name__)

        if not session:
            session = clarity_ext.ClaritySession.create(None)
        self.session = session
        self.cache = cache

    def list_process_types(self, refresh=False):
        # TODO: Don't fetch at all if the flag "fetched" is set
        for process_type in self.session.api.get_process_types():
            # Fetch from the entity cache if available
            from clarity_ext.service.cache import Entity
            cached = self.cache.query(Entity).filter(Entity.uri == process_type.uri).one_or_none()
            if not cached or refresh:
                from xml.etree import ElementTree
                process_type.get()
                xml = ElementTree.tostring(process_type.root, encoding="utf-8")
                cached = Entity(uri=process_type.uri,
                                key=process_type.name,
                                environment=self.session.environment,
                                xml=xml)
                self.cache.add(cached)
                self.cache.commit()
                raise
            yield process_type

    def list_processes_by_process_type(self, process_type):
        return self.session.api.get_processes(type=process_type.name)

    def ui_link_process(self, process):
        """
        Returns the UI link to the process rather than the API uri. The link will only be available if the
        process step is active
        """
        return "{}/clarity/work-details/{}".format(process.uri.split("/api")[0], process.id.split("-")[1])
