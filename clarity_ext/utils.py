import requests_cache
import os
import shutil
import hashlib
import logging
from contextlib import contextmanager
import types


# http://stackoverflow.com/a/3013910/282024
def lazyprop(fn):
    attr_name = '_lazy_' + fn.__name__
    @property
    def _lazyprop(self):
        if not hasattr(self, attr_name):
            setattr(self, attr_name, fn(self))
        return getattr(self, attr_name)
    return _lazyprop

# Monkey patch the sqlite cache in requests_cache so that it doesn't save
# the AUTH_HEADER
default_dbdict_set_item = requests_cache.backends.storage.dbdict.DbPickleDict.__setitem__
default_dbdict_get_item = requests_cache.backends.storage.dbdict.DbPickleDict.__getitem__
AUTH_HEADER = 'Authorization'


def dbdict_set_item(self, key, item):
    """Updates the AUTH_HEADER before caching the response"""
    store = item[0]
    if AUTH_HEADER in store.request.headers:
        store.request.headers[AUTH_HEADER] = '***'
    default_dbdict_set_item(self, key, item)


def dbdict_get_item(self, key):
    """
    Fetches the AUTH_HEADER value, but asserts that the AUTH_HEADER hasn't been cached.

    The AUTH_HEADER should not be saved by default (see dbdict_set_item). This patch
    ensures that it will be detected early if that happens.
    """
    item = default_dbdict_get_item(self, key)
    store = item[0]
    if AUTH_HEADER in store.request.headers and \
                      store.request.headers[AUTH_HEADER] != '***':
        raise ValueError("Auth header was serialized")
    return item

requests_cache.backends.storage.dbdict.DbPickleDict.__setitem__ = dbdict_set_item
requests_cache.backends.storage.dbdict.DbPickleDict.__getitem__ = dbdict_get_item


def use_requests_cache(cache):
    """Turns on caching for the requests library"""
    requests_cache.install_cache(
        cache, allowable_methods=('GET', 'POST', 'DELETE', 'PUT'))


def clean_directory(path, skip=[]):
    """Helper method for cleaning a directory. Skips names in the skip list."""
    to_remove = (os.path.join(path, file_or_dir)
                 for file_or_dir in os.listdir(path)
                 if file_or_dir not in skip)
    for item in to_remove:
        if os.path.isdir(item):
            shutil.rmtree(item)
        else:
            os.remove(item)


def single(seq):
    """Returns the first element in a list, throwing an exception if there is an unexpected number of items"""
    if isinstance(seq, types.GeneratorType):
        seq = list(seq)
    if len(seq) != 1:
        raise ValueError(
            "Unexpected number of items in the list ({})".format(len(seq)))
    return seq[0]


def single_or_default(seq):
    """Returns the first element in a list or None if the list is empty, raising an exception if
    there are more than one elements in the list"""
    if len(seq) > 1:
        raise ValueError(
            "Expecting at most one item in the list. Got ({}).".format(len(seq)))
    elif len(seq) == 0:
        return None
    else:
        return seq[0]


def get_and_apply(dictionary, key, default, fn):
    """
    Fetches the value from the dictionary if it exists, applying the map function
    only if the result is not None (similar to the get method on `dict`)
    """
    ret = dictionary.get(key, default)
    if ret:
        ret = fn(ret)
    return ret


def unique(items, fn):
    """Returns unique items based on evaluation of `fn` on each item"""
    seen = set()
    for item in items:
        key = fn(item)
        if key not in seen:
            seen.add(key)
            yield item


def dir_tree(path):
    """
    Dumps the directory tree to the debug log, including a SHA1 hash of each file.
    No-op if debug log is off.
    """

    def file_hash(path):
        """Returns the SHA1 hash of the file"""
        sha1 = hashlib.sha1()
        with open(path, "rb") as fs:
            sha1.update(fs.read())
            return sha1.hexdigest()

    ret = list()
    ret.append("Contents of '{}':".format(path))
    for root, dirs, files in os.walk(path):
        for f in files:
            full_path = os.path.join(root, f)
            ret.append("\t{}: {}".format(file_hash(full_path)[0:7], full_path))

    return os.linesep.join(ret)


def get_default_log_formatter(use_timestamp):
    """Creates a formatter with the default format [time] [name] [level] [message]"""
    format = "%(name)s - %(levelname)s - %(message)s"
    if use_timestamp:
        format = "%(asctime)s - " + format
    return logging.Formatter(format)


@contextmanager
def add_log_file_handler(path, use_timestamp, filter=None, mode='w'):
    """
    Adds a file handler to the root handler. Creates a new file if one exists.
    Within a `with` statement, the handler will be removed when out of context.
    """
    root_logger = logging.getLogger('')
    file_handler = logging.FileHandler(path, mode=mode)
    file_handler.setFormatter(get_default_log_formatter(use_timestamp))
    file_handler.addFilter(filter)
    root_logger.addHandler(file_handler)
    yield
    root_logger.removeHandler(file_handler)

