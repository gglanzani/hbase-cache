from werkzeug.contrib.cache import BaseCache
from happybase import Connection

class HBaseCache(BaseCache):
    def __init__(self, host='127.0.0.1', port=9090, prefix=None, table_name=None, default_timeout=300, **kwargs):
        super(HBaseCache, self).__init__(default_timeout)
        
        if not table_name:
            raise TypeError('table_name is a required argument')

        self._c = Connection(host=host, port=port, table_prefix=table_prefix, **kwargs)
        self._table = self._c.table(table_name)

    def _put(self, key, value):
        return key, {'cf:value': value}

    def _extract(self, value):
        if value:
            return value.get('cf:value')
        else:
            return value

    def add(self, key, value, timeout=None):
        table = self._table
        try:
            if not table.row(key):
                table.put(*self._put(key, value))
            else:
                return False
        except:
            return False
        return True

    def dec(self, key, delta=1):
        table = self._table
        value = table.row(key)
        new_value = (self._extract(value) or 0) - delta
        table.put(*self._put(key, new_value))
        # TO-DO the above should in principle be guarded by some exception handling
        return new_value

    def delete(self, key):
        try:
            self._table.delete(key)
        except:
            return False
        return True

    def set(self, key, value, timeout=None):
        table = self._table
        try:
            table.delete(key)  # TO-DO Does this return an exception if it doesn't exist? Otherwise we need to put a table.row before that
            table.put(*self._put(key, value))
        except:
            return False
        return True

    def clear(self):
        self.c.delete_table(self.table_name, disable=True)
        self.c.create_table(self.table_name, {'cf': dict()})
        return super(HBaseCache, self).clear()

