from clarity_ext.service.cache import CacheService
from clarity_ext.clarity import ClaritySession
from clarity_ext.service import ProcessService
import fnmatch
from xml.etree import ElementTree


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

    def _get_path(self, path):
        """Fetches the path info of a virtual directory or file, as well as the files. Used by both cat and ls"""
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
                    f = self.synch_ext_file(entity_obj)
                    print(f)
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

    def synch_ext_file(self, entity):
        # Ensures we have an ext file based on the entity. If we already have one in the db, it will not be refreshed
        # TODO: Ensure we refresh if we fetch latest (but don't overwrite...)
        from clarity_ext.service.cache import Expanded
        cached = self.cache_svc.cache.query(Expanded).filter(Expanded.uri == entity.uri).one_or_none()
        print ("got", entity)

        if cached is None:
            ext_file = dict(extensions=list())
            # TODO: Makes only sense for the processtype, cleanup
            for p in entity.parameters:
                extension = {
                    "name": p.name
                }
                ext_file["extensions"].append(extension)

            print("gonna save", ext_file)
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
