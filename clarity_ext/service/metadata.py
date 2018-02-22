import os
import re


# class Extension(Base):
#     __tablename__ = 'extension'
#
#     id = Column(Integer, primary_key=True)
#     name = Column(String(250))
#     command = Column(String(250))
#     process_type_id = Column(Integer, ForeignKey('process_type.id'))
#     process_type = relationship(ProcessType)


class ExtensionMetadataService(object):
    """Maintains the state of metadata about extensions. This metadata comes partly from Clarity and partly from
    the user's config files and code."""

    def __init__(self, config=None, session=None, cache_path=".cache.sqlite3"):
        # TODO: cache_path into config
        from clarity_ext.service.cache import CacheService
        cache_svc = CacheService(cache_path)
        self.config = config
        self.session = session
        self.cache_path = cache_path
        self.cache = cache_svc.fetch_cache(True)

    def ls(self, path, refresh=False):
        m = re.match(r"^/(?P<environment>[^/]+)(/(?P<processtype>[^/]+))?$", path)
        if m:
            environment = m.groupdict().get("environment", None)
            processtype = m.groupdict().get("processtype", None)
        else:
            raise PathException("Can't parse path {}".format(path))

        if not environment:
            raise PathException("No environment specified in path {}".format(path))

        # We should always fetch all process types, even if one is specified in the path. We
        # will usually fetch them only once (if refresh is not specified). Refresh does only apply to the
        # leaf in the path though.
        from clarity_ext.clarity import ClaritySession
        session = ClaritySession(self.config)
        session.login_with_user_config(environment)
        for process_type in self.fetch_process_types(session):
            from xml.etree import ElementTree
            # xml = ElementTree.tostring(process_type.root, encoding="utf-8")
            # entity = Entity(uri=process_type.uri, key=process_type.name, environment=environment, xml=xml)
            print(process_type)
            break
        pass

    def fetch_process_types(self, session=None):
        """Fetches the process types from the cache if available, otherwise from the service."""
        from clarity_ext.service import ProcessService
        session = session or self.session
        process_svc = ProcessService(cache=self.cache, session=session)  # TODO: Skip use_cache when done testing!
        for process_type in process_svc.list_process_types():
            yield process_type
        return

        for process_type in process_svc.list_process_types(None):
            key = process_type.name
            entry = dict()
            ext = entry["extensions"] = list()
            #cached[key] = entry

            entity = ProcessType(name=process_type.name, environment=environment)
            cache.add(entity)
            cache.commit()

            for p in process_type.parameters:
                ext = Extension(name=p.name, command=p.string, process_type=entity)
                cache.add(ext)
                cache.commit()

                import re
                matches = re.findall(r"clarity-ext extension --args 'pid={processLuid}' ([\w.]+)", p.string)
                if matches:
                    #extension["scripts"].append(m.group(1))
                    #extension["scripts"] = matches
                    pass
                    #clarity-ext extension --args 'pid={processLuid}' clarity_ext_scripts.general.rename_pools_rml

            if len(process_type.parameters) > 0:
                break


class PathException(Exception):
    pass
