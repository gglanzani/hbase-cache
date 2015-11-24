from werkzeug.contrib.cache import BaseCache, _items
from happybase import Connection

from operator import itemgetter


class HBaseCache(BaseCache):
    def __init__(self, host='127.0.0.1', port=9090, prefix=None, table_name=None, default_timeout=300, **kwargs): 
# Potential bug: table_prefix instead of prefix
        BaseCache.__init__(self, default_timeout)
        
        if not table_name:
            raise TypeError('table_name is a required argument')
        self.table_name = table_name

        self._c = Connection(host=host, port=port, table_prefix=prefix, **kwargs)
        self._table = self._c.table(table_name) # Note: initialisation overwrites the existing rows of the Hbase table
        
        self.clear()

    def _put(self, key, value):
        return key, {'cf:value': value}

    def _extract(self, value):
        if value:
            return value.get('cf:value')
        else:
            return value

    def add(self, key, value, timeout=None): # Note: timeout is not used in this method, but should be
        print "Adding stuff"
        table = self._table
        print table
        try:
            if not table.row(key):  # TO-DO: what does table.row returns for non existing keys? # Returns empty dict >> check for it and return None
                table.put(*self._put(key, value))
            else:
                return False
        except:
            return False
        return True

    def clear(self):
        print "Clearing stuff"
        try:
            self._c.delete_table(self.table_name, disable=True)
        except:
            pass
        self._c.create_table(self.table_name, {'cf': dict()})
        return super(HBaseCache, self).clear()

    def dec(self, key, delta=1):
        return self.inc(key, -delta)
#        table = self._table
#        new_value = table.counter_inc(key, 'cf:value', -delta)
#        value = table.row(key)
#        new_value = (self._extract(value) or 0) - delta
#        table.put(*self._put(key, new_value))
        # TO-DO the above should in principle be guarded by some exception handling
#        return new_value

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
        rows = table.rows(keys)
        if not rows:
            return {k: None for k in keys}
        return {k: self._extract(v) for k, v in rows}

    def get_many(self, *keys):
        table = self._table
        rows = table.rows(keys)
        if not rows:
            return [None for _ in keys]
        return map(self._extract, map(itemgetter(1), rows))

    def has(self, key):
        return super(HBaseCache, self).has(key)

    def inc(self, key, delta=1):
        table = self._table
        new_value = table.counter_inc(key, 'cf:value', delta)
        return new_value

    def set(self, key, value, timeout=None):
        table = self._table
        print "Setting stuff"
        print table
        try:
            table.delete(key)  # TO-DO Does this return an exception if it doesn't exist? Otherwise we need to put a table.row before that
            table.put(*self._put(key, value))
        except:
            return False
        return True

    def set_many(self, mapping, timeout=None):
        print "Set many"
        batch = self._table.batch()
        for key, value in _items(mapping):
            batch.put(*self._put(key, value))
        try:
            batch.send()
        except:
            return False
        return True


def hbase(app, config, args, kwargs):
#    args.append(app.config['HBASE_SERVER'])
    kwargs['table_name'] = 'test'
    return HBaseCache(*args, **kwargs)

