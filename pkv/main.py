import os
import threading
from yaml import load, dump
try:
    from yaml import CLoader as Loader, CDumper as Dumper
except ImportError:
    from yaml import Loader, Dumper


__version__ = 1.0


class PKV(object):
    """A persistent key-value database.
    It stores the data in `yaml` format.

    Example:
        >>> from pkv import PKV
        >>> db = PKV('/path/to/mydatabase.db')
        >>> db.set('key1', 'value1')
        >>> db.get('key1')
        >>> db.erase('key1')
    """

    def __init__(self, db='pkv.db'):
        self._db = db
        self._lock = threading.Lock()
        self._init_db()

    def __getitem__(self, key):
        """Getter method to fetch from the
        DB like a dict.

        Example:
            >>> db = PKV()
            >>> print(db['name'])
        """
        return self.get(key)

    def __setitem__(self, key, value):
        """Setter method to insert key, value
        pairs in the DB like a dict.

        Example:
            >>> db = PKV()
            >>> db['name'] = 'John'
        """
        return self.set(key, value)

    def __delitem__(self, key):
        """Remove key, value pairse from the DB
        using the `del` keyword.

        Example:
            >>> db = PKV()
            >>> del db['name']
        """
        return self.erase(key)

    def _init_db(self):
        if not os.path.exists(self._db):
            open(self._db, 'w+').close()

    def _dump(self):
        f = open(self._db, 'w')
        dump(self._data, f, Dumper=Dumper)
        f.close()

    def _load(self):
        f = open(self._db, 'r')
        self._data = load(f.read(), Loader=Loader)
        if self._data is None:
            self._data = dict()
        f.close()

    def set(self, key, value):
        self._lock.acquire()
        self._load()
        self._data[key] = value
        self._dump()
        self._lock.release()

    def get(self, key):
        self._load()
        return self._data.get(key, None)

    def erase(self, key):
        self._lock.acquire()
        self._load()
        res = self._data.pop(key, None)
        self._dump()
        self._lock.release()
        return res
