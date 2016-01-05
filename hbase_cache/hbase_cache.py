from datetime import datetime, timedelta
from operator import itemgetter
from collections import OrderedDict

from happybase import Connection
from werkzeug.contrib.cache import BaseCache, _items

def from_iso(dt):
    return datetime.strptime(dt, '%Y-%m-%dT%H:%M:%S.%f')


class HBaseCache(BaseCache):
    def __init__(self, host='127.0.0.1', port=9090, prefix=None, table_name=None, default_timeout=300, **kwargs): 
        super(HBaseCache, self).__init__(default_timeout)
        
        if not table_name:
            raise TypeError('table_name is a required argument')
        self.table_name = table_name

        self._c = Connection(host=host, port=port, table_prefix=prefix, **kwargs)
        self._table = self._c.table(table_name) # Note: initialisation overwrites the existing rows of the Hbase table
        
        self.clear()

    def _put(self, key, value, timeout):
        timestamp = (datetime.now() + timedelta(0, timeout or self.default_timeout)).isoformat()
        return key, {'cf:value': value, 'cf:timestamp': timestamp}

    def _extract(self, value):
        if value:
            v = value.get('cf:value')
            ts = from_iso(value.get('cf:timestamp'))
            if ts > datetime.now():
                return v
            else:
                return None
        else:
            return None

    def add(self, key, value, timeout=None):
        table = self._table
        try:
            if not table.row(key):
                table.put(*self._put(key, value, timeout))
            else:
                return False
        except:
            return False
        return True

    def clear(self):
        try:
            self._c.delete_table(self.table_name, disable=True)
        except:
            pass
        self._c.create_table(self.table_name, {'cf': dict()})
        return super(HBaseCache, self).clear()

    def dec(self, key, delta=1):
        return self.inc(key, -delta)

    def delete(self, key):
        try:
            self._table.delete(key)
        except:
            return False
        return True

    def delete_many(self, *keys):
        batch = self._table.batch()
        try:
            for k in keys:
                batch.delete(k)
            batch.send()
        except:
            return False
        return True

    def get(self, key):
        value = self._table.row(key)
        return self._extract(value) or None

    def get_dict(self, *keys):
        table = self._table
        rows = OrderedDict(table.rows(keys))
        return {k: self._extract(rows.get(k, None)) for k in keys}  # Non-existing keys are not return by table.rows()

    def get_many(self, *keys):
        table = self._table
        rows = OrderedDict(table.rows(keys))
        return [self._extract(rows.get(k, None)) for k in keys]  # Non-existing keys are not return by table.rows()

    def has(self, key):
        return super(HBaseCache, self).has(key)

    def inc(self, key, delta=1):
        table = self._table
        new_value = table.counter_inc(key, 'cf:value', delta)
        return new_value

    def set(self, key, value, timeout=None):
        table = self._table
        try:
            table.delete(key)
            table.put(*self._put(key, value, timeout))
        except:
            return False
        return True

    def set_many(self, mapping, timeout=None):
        batch = self._table.batch()
        for key, value in _items(mapping):
            batch.put(*self._put(key, value, timeout))
        try:
            batch.send()
        except:
            return False
        return True


def hbase(app, config, args, kwargs):
    settings = {'host': config.get('CACHE_HBASE_HOST', '127.0.0.1'),
                'port': config.get('CACHE_HBASE_PORT', 9090),
                'table_name': config.get('CACHE_HBASE_TABLE'),
                'prefix': config.get('CACHE_HBASE_PREFIX'),
                'default_timeout': config.get('CACHE_DEFAULT_TIMEOUT', 300)}
    kwargs.update(settings) 
    return HBaseCache(*args, **kwargs)