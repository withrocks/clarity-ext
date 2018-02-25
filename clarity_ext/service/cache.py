from sqlalchemy import Column, Integer, String, Boolean, DateTime, UniqueConstraint
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
import os

Base = declarative_base()


class CacheService(object):
    """Provides a file cache (sqlite) for entities fetched from the Clarity REST API"""
    def __init__(self, cache_path, config):
        self.cache_path = cache_path
        self.cache = self.fetch_cache(True)
        self.config = config
        self._synch_config_with_cache()

    def _synch_config_with_cache(self):
        """Makes sure that the cache file contains all the environments in the config file"""
        db_envs = self.cache.query(Environment).all()
        db_envs = {entity.name: entity for entity in db_envs}
        for conf_env in self.config.global_config["environments"]:
            if conf_env not in db_envs:
                current = Environment(name=conf_env, fetched_process_types=False)
                self.cache.add(current)
                self.cache.commit()

    def fetch_cache(self, force_create=False):
        if not os.path.exists(self.cache_path) or force_create:
            self.create_cache()
        engine = create_engine('sqlite:///{}'.format(self.cache_path))
        Base.metadata.bind = engine
        cache = sessionmaker(bind=engine)()
        return cache

    def create_cache(self):
        """Creates a local sqlite cache file"""
        engine = create_engine('sqlite:///{}'.format(self.cache_path))
        Base.metadata.create_all(engine)


class Environment(Base):
    __tablename__ = 'environment'

    id = Column(Integer, primary_key=True)
    name = Column(String(20), nullable=False)
    fetched_process_types = Column(Boolean, nullable=False)
    last_fetched_process_types = Column(DateTime, nullable=True)


class Entity(Base):
    """An entity from the Clarity REST API"""

    __tablename__ = 'entity'
    uri = Column(String(), primary_key=True)

    # A human readable key for the object. May change over the life of an object. This
    # can for example be the name of a process type
    key = Column(String(), nullable=False)
    environment = Column(String(20), nullable=False)
    xml = Column(String(), nullable=False)


class ExtensionPoint(Base):
    """
    Contains information about an extension point in Clarity. Clarity represents the action that should happen
    as zero or more lines of bash. In the processtype contract, this is called "string", here it's called "action".

    Examples:

    Here the user is usign the "lab logic toolkit". In clarity-ext, the idea is not to use these, as they are one-liners
    that are not easy to debug, so it's recommended to rewrite them as a clarity-ext extension.

        name:   'Populate pooling, instrument, special information',
        source: 'bash -c "/opt/gls/clarity/bin/java
                -jar /opt/gls/clarity/extensions/ngs-common/v5/EPP/ngs-extensions.jar -i {stepURI:v2:http}
                -u {username} -p {password} script:evaluateDynamicExpression -t true -h false -exp \'input.::Number
                of lanes::=submittedSample.::Number of lanes:: ; input.::Pooling::=submittedSample.::Pooling:: ;
                input.::conc FC::=submittedSample.::conc FC:: ;if (submittedSample.hasValue(::Special info seq::))
                {input.::Special info seq:: =submittedSample.::Special info seq:: } ;
                input.::Sequencing instrument::=submittedSample.::Sequencing instrument:: ;
                input.::PhiX %::=submittedSample.::PhiX %::\' -log {compoundOutputFileLuid1}"'
        state: 0 (clean)  # can be: 0 clean/ 1 dirty
    """
    __tablename__ = 'extension_point'
    __table_args__ = (
        UniqueConstraint('processtype_uri', 'name'),
    )

    id = Column(Integer, primary_key=True)
    processtype_uri = Column(String(), nullable=False)
    name = Column(String(), nullable=False)
    # The actual source in Clarity. We may have different source waiting to be uploaded locally:
    remote = Column(String(), nullable=True)
    # The local source code we've contructed by joining together all clarity-ext extensions we have. If this differs from
    # the remote source code, we should report that to the users so they can upload new source accordingly. In that case,
    # state is 1
    local = Column(String(), nullable=True)
    is_dirty = Column(Boolean, nullable=False)


class ExtensionInfo(Base):
    """
    Lists all extensions for an extension point. These don't have to be available remotely yet. From these
    we calculate the local field in the ExtensionPoint table and push it up to the remote when applicable.
    """
    __tablename__ = 'extension_info'

    extension_point_id = Column(Integer, primary_key=True)
    script = Column(String(), primary_key=True)
    seq = Column(Integer, nullable=False)  # Position of this script if more than one should run at the extension point

    def __repr__(self):
        return "<ExtensionInfo #{}: {}>".format(self.seq, self.script)

