# pkv
A minimalistic persistent key value store.

### Installing

```
$ pip install pkv
```

### Usage

```Python
>>> from pkv import PKV
>>> db = PKV('mydb.db')
>>> db.set('ping', 'pong')
>>> db.get('ping')
>>> db.erase('ping')
```
