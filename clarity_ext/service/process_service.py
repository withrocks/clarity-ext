import clarity_ext
import xml.etree.ElementTree as ET
import logging
import re


class ProcessService(object):
    """Provides access to information about processes and process types"""
    def __init__(self, logger=None, cache_svc=None, session=None):
        self.logger = logger or logging.getLogger(__name__)
        self.session = session
        assert self.session
        self.cache = cache_svc.cache

    def list_process_types(self, filter=None, refresh=False):
        from xml.etree import ElementTree
        from clarity_ext.service.cache import Entity, Environment
        env = self.cache.query(Environment).filter(Environment.name == self.session.environment).one()

        if not env.last_fetched_process_types or refresh:
            for process_type in self.session.api.get_process_types():
                # Fetch from the entity cache if available
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
            import datetime
            env.last_fetched_process_types = datetime.datetime.now()
            self.cache.add(env)
            self.cache.commit()

        q = self.cache.query(Entity).filter(Entity.environment == self.session.environment)
        if filter:
            q = q.filter(Entity.key.like(filter))

        for entity in q.all():
            from genologics.entities import Processtype
            pt = Processtype(lims=self.session.api, _create_new=True)
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
