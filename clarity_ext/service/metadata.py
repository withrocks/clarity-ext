import yaml
import os
from sqlalchemy import Column, ForeignKey, Integer, String
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

Base = declarative_base()

class ProcessType(Base):
    __tablename__ = 'process_type'

    id = Column(Integer, primary_key=True)
    name = Column(String(250), nullable=False)
    environment = Column(String(20), nullable=False)

class Extension(Base):
    __tablename__ = 'extension'

    id = Column(Integer, primary_key=True)
    name = Column(String(250))
    command = Column(String(250))
    process_type_id = Column(Integer, ForeignKey('process_type.id'))
    process_type = relationship(ProcessType)

class ExtensionMetadataService(object):
    """Maintains the state of metadata about extensions. This metadata comes partly from Clarity and partly from
    the user's config files and code."""
    def __init__(self):
        return
        self.cache = self._fetch_cache()
        pass

    def _fetch_cache(self):
        if not os.path.exists(self.cache_path):
            self.create_cache()

        engine = create_engine('sqlite:///{}'.format(self.cache_path))
        Base.metadata.bind = engine

        DBSession = sessionmaker(bind=engine)
        session = DBSession()
        return session

    def _create_cache(self):
        """Creates a local sqlite cache file"""
        engine = create_engine('sqlite:///{}'.format(self.cache_path))
        Base.metadata.create_all(engine)


    def fetch_process_types(self, environment):
        pass


