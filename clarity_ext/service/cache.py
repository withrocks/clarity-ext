from sqlalchemy import Column, ForeignKey, Integer, String, Boolean
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
import os

Base = declarative_base()

class CacheService():
    """Provides a file cache (sqlite) for entities fetched from the Clarity REST API"""
    def __init__(self, cache_path):
        self.cache_path = cache_path

    # def _synch_config_with_cache(self, config):
    #     """Makes sure that the cache file contains all the environments in the config file"""
    #     db_envs = self.cache.query(Environment).all()
    #     db_envs = {entity.name: entity for entity in db_envs}
    #     for conf_env in config.global_config["environments"]:
    #         if conf_env not in db_envs:
    #             current = Environment(name=conf_env, fetched_process_types=False)
    #             self.cache.add(current)
    #             self.cache.commit()

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


class Entity(Base):
    """An entity from the Clarity REST API"""

    __tablename__ = 'entity'

    #id = Column(Integer, primary_key=True)
    uri = Column(String(), primary_key=True)

    # A human readable key for the object. May change over the life of an object. This
    # can for example be the name of a process type
    key = Column(String(), nullable=False)
    environment = Column(String(20), nullable=False)
    xml = Column(String(), nullable=False)
