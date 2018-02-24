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
        self.cache_svc = CacheService(cache_path, config)
        self.config = config
        self.session = session
        self.cache_path = cache_path
        self.directory_structure = {
            "environment": {
                "processtypes": {
                    "extensions": None,
                    "processes": None,
                }
            }
        }



        # This service may need to connect to one or more systems, so we have one session per environment:
        # It might make sense to have one "app" object which has all sessions as well as the current session
        self.session_dict = dict()

    def get_environment_session(self, environment):
        from clarity_ext.clarity import ClaritySession
        if environment not in self.session_dict:
            session = ClaritySession(self.config)
            session.login_with_user_config(environment)
            self.session_dict[environment] = session
        return self.session_dict[environment]

    def list_environments(self):
        pass

    def _ls_processtypes(self, env, process_type_pattern):
        # Find the process types matching pattern in this environment

        if process_type_pattern is None:
            process_type_pattern = "*"

        session = self.get_environment_session(env)

        from clarity_ext.service import ProcessService
        process_svc = ProcessService(cache_svc=self.cache_svc, session=session)
        # TODO: What if there is a '%' in the column?
        for process_type in process_svc.list_process_types(process_type_pattern.replace("*", "%")):
            yield process_type

    def _ls_environment(self, env_pattern):
        # The semantics of ls are that if you specify a directory (e.g. ls /) it means "show everything under this, i.e.
        # /* (slash glob)"
        import fnmatch
        if env_pattern is None:
            env_pattern = "*"

        all_environments = [key for key in self.config.global_config["environments"]]
        for env in all_environments:
            if fnmatch.fnmatch(env, env_pattern):
                yield env

    def _get_providers(self, env):
        return ["processtypes"]

    def _get_property(self, provider):
        pass

    def cat(self, path):
        pass


    def ls(self, path, refresh=False):
        """
        Lists entities in Clarity with clarity-ext specific extensions. The listing is always from a local cache
        which can be overwritten by setting refresh to True.
        """

        # Save entities in a process cache if they might be needed later in this method
        proc_cache = dict()

        if not path.startswith("/"):
            raise Exception("Paths must be absolute, e.g. `/dev`")

        # TODO: Allow escaping / with \
        parts = path.split("/")[1:]

        def get_part(pos):
            """Given a split path, returns the text/pattern at that position and None if it's empty or not available"""
            if len(parts) > pos:
                part = parts[pos]
                return part if len(part) > 0 else None

        env_pattern = get_part(0)
        environments = self._ls_environment(env_pattern)

        if not env_pattern:
            return [(env,) for env in environments]

        # If we haven't exited, we should list providers too:
        provider_pattern = get_part(1)

        providers = list()
        for env in environments:
            for provider in self._get_providers(env):
                providers.append((env, provider))

        if not provider_pattern:
            return providers

        entity_pattern = get_part(2)

        entities = list()
        # If we haven't exited, we should list all entities in each environment and provider:
        for env, provider in providers:
            if provider == "processtypes":
                for entity in self._ls_processtypes(env, entity_pattern):
                    entities.append((env, provider, entity.name))
            else:
                raise Exception()

        if not entity_pattern:
            return entities

        files_pattern = get_part(3)
        files = list()
        for env, provider, entity in entities:
            for f in self._ls_files(env, provider, entity, files_pattern):
                files.append((env, provider, entity, f))

        # TODO: ls'ing something that doesn't exist returns nothing without printing an error msg
        return files

    def _ls_files(self, env, provider, entity, files_pattern):
        return ["entity", "ext"]

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
