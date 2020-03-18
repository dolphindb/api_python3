import unittest
import dolphindb as ddb
import pandas as pd
import numpy as np
from setup import *
import dolphindb.settings as keys

class TestDatabase(unittest.TestCase):
    @classmethod
    def setUp(self):
        self.s=ddb.session()
        self.s.connect(HOST,PORT,"admin","123456")
        
    @classmethod
    def tearDownClass(cls):
        pass
    
    def test_create_dfs_database_value_partition(self):
        mydbPath="dfs://db1"
        if self.s.existsDatabase(mydbPath):
            self.s.dropDatabase(mydbPath)
        self.s.database('db',partitionType=keys.VALUE,partitions=[1,2,3],dbPath=mydbPath)
        self.assertEqual(self.s.existsDatabase(mydbPath),True)
    
    def test_create_dfs_database_range_partition(self):
        mydbPath="dfs://db1"
        if self.s.existsDatabase(mydbPath):
            self.s.dropDatabase(mydbPath)
        self.s.database('db',partitionType=keys.RANGE,partitions=[1,11,21],dbPath=mydbPath)
        self.assertEqual(self.s.existsDatabase(mydbPath),True)


    def test_create_dfs_database_hash_partition(self):
        mydbPath="dfs://db1"
        if self.s.existsDatabase(mydbPath):
            self.s.dropDatabase(mydbPath)
        self.s.database('db',partitionType=keys.HASH,partitions=[4,2],dbPath=mydbPath)
        self.assertEqual(self.s.existsDatabase(mydbPath),True)

    def test_create_dfs_database_list_partition(self):
        mydbPath="dfs://db1"
        if self.s.existsDatabase(mydbPath):
            self.s.dropDatabase(mydbPath)
        self.s.database('db',partitionType=keys.LIST,partitions=[['IBM','ORCL','MSFT'],['GOOG','FB']],dbPath=mydbPath)
        self.assertEqual(self.s.existsDatabase(mydbPath),True)

    def test_create_dfs_database_compo_partition(self):
        #mydbPath="dfs://db1"
        #if self.s.existsDatabase(mydbPath):
        #    self.s.dropDatabase(mydbPath)
        #self.s.database('db1',partitionType=keys.VALUE,partitions=[np.datetime64('2012-06-01'),np.datetime64('2012-06-02')],dbPath='')
        #self.assertEqual(self.s.existsDatabase(mydbPath),True)
        pass

    def test_dropDatabase(self):
        mydbPath="dfs://db1"
        if self.s.existsDatabase(mydbPath):
            self.s.dropDatabase(mydbPath)
        self.s.database('db',partitionType=keys.VALUE,partitions=[1,2,3],dbPath=mydbPath)
        self.s.dropDatabase(mydbPath)
        self.assertEqual(self.s.existsDatabase(mydbPath),False)



if __name__ == '__main__':
    unittest.main()
