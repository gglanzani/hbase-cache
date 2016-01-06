from collections import OrderedDict
from datetime import datetime, timedelta
from unittest import TestCase

from mock import MagicMock
from nose.tools import (
    assert_dict_equal,
    assert_equal,
    assert_false,
    assert_in,
    assert_is_instance,
    assert_is_not_none,
    assert_list_equal,
    assert_not_in,
    assert_raises,
    assert_true,
)

from hbase_cache import HBaseCache

class MockHBaseCache(HBaseCache):
    def __init__(self):
        self._table = MagicMock(None)
        self.default_timeout = 300


def get_value(value=42, future=True, now=None):
    time = now or datetime.now()
    if future:
        time += timedelta(0, 300)

    return {'cf:value': value, 'cf:timestamp': time.isoformat()}

class TestHBaseCache(TestCase):
    def setUp(self):
        self.cache = MockHBaseCache()
        self.cache._table.row = MagicMock()
        self.cache._table.rows = MagicMock()
        self.cache._table.put = MagicMock()
        return super(TestHBaseCache, self).setUp()

    def test_add_when_present(self):
        self.cache._table.row.return_value = get_value(value=5)

        assert_equal(self.cache.add('5', 5), False)

    def test_add_when_absent(self):
        self.cache._table.row.return_value = None
        self.cache._table.put.return_value = None

        assert_equal(self.cache.add('5', 5), True)

    # TO-DO: how to properly test dec, delete(_many), and inc, set and set_many?

    def test_get_within_timeout(self):
        self.cache._table.row.return_value = get_value(value=5)

        assert_equal(self.cache.get('5'), 5)

    def test_get_outside_timeout(self):
        self.cache._table.row.return_value = get_value(value=5, future=False)

        assert_equal(self.cache.get('5'), None)

    def test_get_dict(self):
        now = datetime.now()
        data = [('5', get_value(value=5, now=now)),
                ('6', get_value(value=6, now=now)),
                ('42', get_value(value=42, now=now)),
                ('7', get_value(value=7, now=(now - timedelta(0, 800))))]
        data_dict = dict(data)
        keys = data_dict.keys()
        results = {'5': 5, '6': 6, '42': 42, '7': None}
        self.cache._table.rows.return_value = data
        assert_dict_equal(self.cache.get_dict(keys), results)

    def test_get_many(self):
        now = datetime.now()
        data = [('5', get_value(value=5, now=now)),
                ('6', get_value(value=6, now=now)),
                ('42', get_value(value=42, now=now)),
                ('7', get_value(value=7, now=(now - timedelta(0, 800))))]
        data_dict = OrderedDict(data)
        keys = data_dict.keys()
        results = [5, 6, 42, None]
        self.cache._table.rows.return_value = data
        assert_equal(self.cache.get_many(keys), results)
