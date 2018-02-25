from clarity_ext.service.cache import CacheService
from clarity_ext.clarity import ClaritySession
from clarity_ext.service import ProcessService
import fnmatch
from clarity_ext.service.cache import ExtensionPoint, ExtensionInfo
from xml.etree import ElementTree


# The command for executing clarity_ext extensions via Clarity (TODO: This is available somewhere else)
CLARITY_EXT_COMMAND = "clarity-ext extension --args 'pid={processLuid}' (.+) exec"

class ExtensionMetadataService(object):
    """Maintains the state of metadata about extensions. This metadata comes partly from Clarity and partly from
    the user's config files and code."""

    def __init__(self, config=None, session=None, cache_path=".cache.sqlite3"):
        # TODO: cache_path into config
        self.cache_svc = CacheService(cache_path, config)
        self.config = config
        self.session = session
        self.cache_path = cache_path
        # This service may need to connect to one or more systems, so we have one session per environment:
        # It might make sense to have one "app" object which has all sessions as well as the current session
        self.session_dict = dict()

    def get_environment_session(self, environment):
        if environment not in self.session_dict:
            session = ClaritySession(self.config)
            session.login_with_user_config(environment)
            self.session_dict[environment] = session
        return self.session_dict[environment]

    def _ls_processtypes(self, env, process_type_pattern):
        # Find the process types matching pattern in this environment
        if process_type_pattern is None:
            process_type_pattern = "*"

        session = self.get_environment_session(env)

        process_svc = ProcessService(cache_svc=self.cache_svc, session=session)
        # TODO: What if there is a '%' in the column?
        for process_type in process_svc.list_process_types(process_type_pattern.replace("*", "%")):
            yield process_type

    def _ls_environment(self, env_pattern):
        if env_pattern is None:
            env_pattern = "*"
        all_environments = [key for key in self.config.global_config["environments"]]
        for env in all_environments:
            if fnmatch.fnmatch(env, env_pattern):
                yield env

    def _get_providers(self, env):
        return ["processtypes"]

    def cat(self, path):
        results = self._get_path(path)
        for path, file in results:
            if not file:
                pass
            else:
                print(file)

    def _get_path(self, path, synch_local_file=True):
        """Fetches the path info of a virtual directory or file, as well as the files. Used by both cat and ls"""
        # TODO: synch_local_file should be set to False - it's for development only
        # Save entities in a process cache if they might be needed later in this method
        proc_cache = dict()

        if not path.startswith("/"):
            raise Exception("Paths must be absolute, e.g. `/dev`. Got {}".format(path))

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
            return [((env,), None) for env in environments]

        # If we haven't exited, we should list providers too:
        provider_pattern = get_part(1)

        providers = list()
        for env in environments:
            for provider in self._get_providers(env):
                path = (env, provider)
                providers.append((path, None))

        if not provider_pattern:
            return providers

        entity_pattern = get_part(2)

        entities = list()
        # If we haven't exited, we should list all entities in each environment and provider:
        for path, file in providers:
            env, provider = path
            if provider == "processtypes":
                for entity in self._ls_processtypes(env, entity_pattern):
                    path = (env, provider, entity.name)
                    if entity.name in proc_cache:
                        raise Exception("More than one entity with the name/key {}".format(entity.name))
                    proc_cache[entity.name] = entity
                    entities.append((path, None))
            else:
                raise Exception()

        if not entity_pattern:
            return entities

        files_pattern = get_part(3)
        files = list()
        for path, _ in entities:
            env, provider, entity = path
            for fname in self._ls_files(env, provider, entity, files_pattern):
                path = (env, provider, entity, fname)
                entity_obj = proc_cache[entity]
                if fname == "entity":
                    f = ElementTree.tostring(entity_obj.root, encoding="UTF-8")
                elif fname == "ext":
                    if synch_local_file:
                        self.synch_extension_data(entity_obj)
                    f = None
                else:
                    raise Exception("Unexpected fname {}".format(fname))
                files.append((path, f))

        # TODO: ls'ing something that doesn't exist returns nothing without printing an error msg
        return files

    def ls(self, path, refresh=False):
        """
        Lists entities in Clarity with clarity-ext specific extensions. The listing is always from a local cache
        which can be overwritten by setting refresh to True.
        """
        return [path for path, f in self._get_path(path)]

    def _ls_files(self, env, provider, entity, files_pattern):
        # TODO: processes is actually a directory
        return [f for f in ["entity", "ext", "processes"] if fnmatch.fnmatch(f, files_pattern)]

    def _extensions_from_remote(self, remote):
        """Returns one or more extensions based on the remote data. Note that if there is more data in the string, it's
        ignored (e.g. if the user currently is executing something else than clarity-ext extensions). But the user
        will be prompted before pushing this data over to the server"""
        import re
        pattern = CLARITY_EXT_COMMAND
        return re.findall(pattern, remote)

    def update_dirty_state(self, processtype):
        """Checks the local state of the scripts and generates a local bash command to be potentially updated
        in Clarity. This would be called every time the user updates the extension_infos.

        This does not commit the local settings to the remote, but can be used to report if there are things to be
        uploaded"""
        for p in processtype.parameters:
            ext_point = self.cache_svc.cache.query(ExtensionPoint) \
                .filter(ExtensionPoint.processtype_uri == processtype.uri, ExtensionPoint.name == p.name).one_or_none()
            extensions_in_db = self.cache_svc.cache.query(ExtensionInfo) \
                .filter(ExtensionInfo.extension_point_id == ext_point.id).order_by(ExtensionInfo.seq).all()
            new_script = " && ".join([CLARITY_EXT_COMMAND.replace("(.+)", e.script) for e in extensions_in_db])
            ext_point.local = new_script
            ext_point.is_dirty = ext_point.local != ext_point.remote
            self.cache_svc.cache.add(ext_point)
            self.cache_svc.cache.commit()

    def synch_extension_data(self, processtype):
        """Given an entity, refreshes all the data we have on each of the extension point it has, as well as
        each extension we've defined for it. It may automatically clean up the Extension table if an extension point
        doesn't exist anymore, otherwise, the extension table will remain unchanged."""
        # TODO: Call this method only when refreshing a process type entity from clarity or if a specific refresh-local
        # parameter is set

        # Ensures we have an ext file based on the entity. If we already have one in the db, it will not be refreshed
        # TODO: Ensure we refresh if we fetch latest (but don't overwrite...)

        # Ensure we have one parameter per each entity:
        for p in processtype.parameters:
            ext_point = self.cache_svc.cache.query(ExtensionPoint)\
                .filter(ExtensionPoint.processtype_uri == processtype.uri,
                        ExtensionPoint.name == p.name).one_or_none()
            # TODO: Cleanup extension points that don't exist anymore (if they've been removed from the entity)
            if not ext_point:
                ext_point = ExtensionPoint(processtype_uri=processtype.uri, name=p.name,
                                           remote=p.string, local=None, is_dirty=False)
                self.cache_svc.cache.add(ext_point)
                self.cache_svc.cache.commit()

            # Now auto generate clarity-ext extensions from this data, only if we have none already.
            # This is only for convenience in the cases where the user starts using clarity-ext on a clarity instance
            # where there are already extensions
            extensions_in_db = self.cache_svc.cache.query(ExtensionInfo) \
                .filter(ExtensionInfo.extension_point_id == ext_point.id).all()
            if len(extensions_in_db) == 0:
                for ix, script in enumerate(self._extensions_from_remote(p.string)):
                    ext_info = ExtensionInfo(extension_point_id=ext_point.id, script=script, seq=ix)
                    self.cache_svc.cache.add(ext_info)
                    self.cache_svc.cache.commit()
        self.update_dirty_state(processtype)
        return
        # return
        cached = self.cache_svc.cache.query(ExtensionPoint).filter(Expanded.uri == entity.uri).one_or_none()
        print ("got", entity)

        if cached is None:
            ext_file = dict(extensions=list())
            # TODO: Makes only sense for the processtype, cleanup
            for p in entity.parameters:
                extension = {
                    "name": p.name,
                    "source": p.string
                }
                ext_file["extensions"].append(extension)

            import pprint
            pprint.pprint(ext_file)
        return None

    def fetch_process_types(self, session=None):
        """Fetches the process types from the cache if available, otherwise from the service."""
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
