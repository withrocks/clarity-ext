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
        from xml.etree import ElementTree
        # TODO: Don't fetch at all if the flag "fetched" is set, this saves the one call to the
        # overview page
        for process_type in self.session.api.get_process_types():
            # Fetch from the entity cache if available
            from clarity_ext.service.cache import Entity
            cached = self.cache.query(Entity).filter(Entity.uri == process_type.uri).one_or_none()
            if not cached or refresh:
                process_type.get()
                s = ElementTree.tostring(process_type.root, encoding="UTF-8")
                xml = unicode(s, encoding="UTF-8")
                if not cached:
                    cached = Entity(uri=process_type.uri, environment=self.session.environment)
                cached.key = process_type.name
                cached.xml = xml
                self.cache.add(cached)
                self.cache.commit()

        for entity in self.cache.query(Entity).all():
            from genologics.entities import Processtype
            pt = Processtype(lims=self.session.api, _create_new=True)  # TODO: this logic should be moved to the cache service
            pt.root = ElementTree.fromstring(entity.xml.encode('utf-8'))
            pt._uri = entity.uri
            yield pt

    def list_processes_by_process_type(self, process_type):
        return self.session.api.get_processes(type=process_type.name)

    def ui_link_process(self, process):
        """
        Returns the UI link to the process rather than the API uri. The link will only be available if the
        process step is active
        """
        return "{}/clarity/work-details/{}".format(process.uri.split("/api")[0], process.id.split("-")[1])
