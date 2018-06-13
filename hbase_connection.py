# -*- coding: utf-8 -*-
"""
Spyder Editor

@author: dennis.borchardt

This script will be used to connect to the HBase Database and will be insert 
some rows and columns in the HBase database.
"""

import happybase as base

def connect_to_hbase(host, port):
    """
    can be used to connect with a HBase database
    It's import to use the command "hbase thrift start -p" and specify the port.
    
    Parameters
    --------------
    host : string
        should be the ip address
    port : int
        the open port
    
    Return
    --------------
    HappyBase Connection Object
    """
    try:
        conn = base.Connection(host=host, port=port, transport='buffered')
        conn.open()
        return conn
    except:
        print("The given value is not correct, the function couldn't connect to HBase")
        conn.close()

import random   
import string

def create_random_string(column_length):
    """
    generates a random String values with letters and digits
    
    Parameters
    --------------
    column_length : int
        defines the length of the returning String value
        
    Return
    --------------
    String value
    """
    chars = string.ascii_letters
    gen_random_string = ''.join(random.choice(chars + string.digits) for _ in range(column_length))
    if gen_random_string is not None:
        return gen_random_string


def create_rows(table, family_column, column_length, nRows, value_length):
    """
    creating a column with n-rows in an specific table of the HBase database
    
    Parameters
    --------------
    table : String
        should be the table of the HBase, where the data got pushed into
    family_column : String
        is the existing family column from the table
    column_length : int
        defines the random generated String value
    nRows: int
        this paramter defines, how much rows are getting created
    value_length : int
        used for the length of the value, which will be inserted in the database  
    """
    ba = table.batch()
    for row in range(nRows):
        row_value = 'row-key-' + str(row)
        byte_conv_row = row_value.encode()
        ran_column = create_random_string(column_length)
        byte_conv_column = (family_column + ran_column).encode()
        ran_value = create_random_string(value_length)
        byte_conv_value = ran_value.encode()
        
        ba.put(byte_conv_row, {byte_conv_column: byte_conv_value})
    ba.send()  
    print("Batched: " + str(row+1) + " rows")
    
from sys import path
path.append('/Users/dennis.borchardt/python_project_hbase/')
import config
try:
    host = config.HBASE_CONFIG['host']
    namespace = config.HBASE_CONFIG['family_column']
    table_name = config.HBASE_CONFIG['table_name']
    port = config.HBASE_CONFIG['port']

    conn = connect_to_hbase(host, port)
    table = conn.table(table_name)
    create_rows(table, namespace, 10, 3, 5)
finally:
    conn.close()
    