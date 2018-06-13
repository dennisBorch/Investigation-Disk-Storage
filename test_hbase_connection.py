#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Jun 13 13:54:32 2018

@author: dennis.borchardt
"""


from collections import defaultdict
import unittest
import happybase_mock as happybase


class MockTable(object):

    def __init__(self, table_name):
        self.table_name = table_name
        self.data = defaultdict(dict)

    def put(self, row, data):
        self.data[row].update(data)

    def row(self, row, columns=None):
        return self.data[row]

    def scan(self, **options):
        ''' does not respect any options like start/stop row '''
        return self.data.items()


class MockConnection(object):
    ''' singleton object, so that multiple HBaseTables can collaborate '''

    _instance = None
    tables = dict()

    def __new__(cls, *args, **kwargs):
        if not cls._instance:
            cls._instance = super(MockConnection, cls).__new__(cls, *args, **kwargs)
        return cls._instance

    def __init__(self, *args, **kwargs):
        pass

    def is_table_enabled(self, name):
        return True

    def table(self, name):
        table = self.tables.get(name)
        if not table:
            table = MockTable(name)
            self.tables[name] = table
        return table

    def flush(self):
        self.tables = dict()


class HBaseTestCase(unittest.TestCase):
    ''' mock out calls to hbase
    if you over-ride setUp(), make sure to call super '''

    def setUp(self):
        happybase._Connection = happybase.Connection
        happybase.Connection = MockConnection
        MockConnection().flush()

    def tearDown(self):
        happybase.Connection = happybase._Connection


class HappybaseMockTests(HBaseTestCase):

    def setUp(self):
        connection = happybase.Connection()
        self.table = connection.table('table-name')
        super(HappybaseMockTests, self).setUp()

    def test_put(self):
        self.table.put('row1', {'cf1:col1': '1', 'cf1:col2': '2', 'cf2:col1': '3'})
        self.assertEquals(self.table.row('row1'), {'cf1:col1': '1', 'cf1:col2': '2', 'cf2:col1': '3'})

    def test_scan(self):
        self.table.put('row1', {'cf1:col1': '1'})
        self.table.put('row1', {'cf1:col2': '2'})
        self.table.put('row2', {'cf2:col1': '3'})
        self.assertEquals(self.table.scan(), [('row1', {'cf1:col1': '1', 'cf1:col2': '2'}), ('row2', {'cf2:col1': '3'})])