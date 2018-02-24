from sqlalchemy import Column, Integer, String, Boolean, DateTime
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


class Expanded(Base):
    """Contains additional information about the entity that's particular to clarity-ext, e.g. a list of
    extensions that haven't been pushed to Clarity"""
    __tablename__ = 'expanded'

    # The uri of the entity
    uri = Column(String(), primary_key=True)
    doc = Column(String(), nullable=True)

