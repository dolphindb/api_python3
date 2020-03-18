import unittest
import dolphindb as ddb
import pandas as pd
import numpy as np
from setup import *
import dolphindb.settings as keys

def create_dfs_value_db():
    s=ddb.session()
    s.connect(HOST,PORT,"admin","123456")
    ddb_script='''
    login('admin','123456')
    dbPath='dfs://db1'
    tbName='pt'
    if(existsDatabase(dbPath))
        dropDatabase(dbPath)
    db=database(dbPath,VALUE,1..10)
    n=100000
    t=table(take(1..10,n) as id,rand(100,n) as val)
    db.createPartitionedTable(t,tbName,'id').append!(t)
    '''
    s.run(ddb_script)
    s.close()

def create_disk_value_db(WORK_DIR):
    s=ddb.session()
    s.connect(HOST,PORT,"admin","123456")
    ddb_script='''
    login('admin','123456')
    dbPath='{WORK_DIR}/db1'
    tbName='pt'
    if(exists(dbPath))
        dropDatabase(dbPath)
    db=database(dbPath,VALUE,1..10)
    n=100000
    t=table(take(1..10,n) as id,rand(100,n) as val)
    pt=db.createPartitionedTable(t,tbName,'id')
    pt.append!(t)
    '''.format(WORK_DIR=WORK_DIR)
    s.run(ddb_script)
    s.close()

class TestLoadTable(unittest.TestCase):
    @classmethod
    def setUp(self):
        self.s=ddb.session()
        self.s.connect(HOST,PORT,"admin","123456")
        
    @classmethod
    def tearDownClass(cls):
        pass

    def test_load_dfs_partitioned_table(self):
        create_dfs_value_db()
        pt=self.s.loadTable(tableName='pt',dbPath='dfs://db1')
        self.assertEqual(pt.rows,100000)
    
    def test_load_disk_partitioned_table(self):
        create_disk_value_db(WORK_DIR)
        pt=self.s.loadTable(tableName='pt',dbPath=WORK_DIR+'/db1')
        self.assertEqual(pt.rows,100000)
    
    def test_load_disk_partitioned_table_specified_partitions(self):
        create_disk_value_db(WORK_DIR)
        tmp=self.s.loadTable(tableName='pt',dbPath=WORK_DIR+'/db1',partitions=[1,2,3,4])
        self.assertEqual(tmp.rows,40000)

    def test_load_disk_partitioned_table_memory_mode_true(self):
        create_disk_value_db(WORK_DIR)
        tmp=self.s.loadTable(tableName='pt',dbPath=WORK_DIR+'/db1',partitions=[1,2,3,4],memoryMode=True)
        self.assertEqual(tmp.rows,40000)

    def test_load_dfs_partitioned_by_sql(self):
        create_dfs_value_db()
        tmp=self.s.loadTableBySQL(tableName='pt',dbPath='dfs://db1',sql='select * from pt where id in [1,2,3,4]')
        self.assertEqual(tmp.rows,40000)
    
    def test_load_disk_partitioned_by_sql(self):
        create_disk_value_db(WORK_DIR)
        tmp=self.s.loadTableBySQL(tableName='pt',dbPath=WORK_DIR+'/db1',sql='select * from pt where id in [1,2,3,4]')
        self.assertEqual(tmp.rows,40000)
    
    def test_exist_dfs_table(self):
        create_dfs_value_db()
        self.assertEqual(self.s.existsTable(dbUrl='dfs://db1',tableName='pt'),True)
    
    def test_exist_disk_table(self):
        create_disk_value_db(WORK_DIR)
        self.assertEqual(self.s.existsTable(dbUrl=WORK_DIR+'/db1',tableName='pt'),True)

    def test_drop_dfs_partition_specified_tableName(self):
        #create_dfs_value_db()
        #self.s.dropPartition(dbPath='dfs://db1',partitionPaths=['/1','/2'],tableName='pt')
        #tmp=self.s.loadTable(tableName='pt',dbPath='dfs://db1')
        #self.assertEqual(tmp.rows,80000)
        pass
    
    def test_drop_dfs_partition(self):
        create_dfs_value_db()
        self.s.dropPartition(dbPath='dfs://db1',partitionPaths=['/1','/2'])
        tmp=self.s.loadTable(tableName='pt',dbPath='dfs://db1')
        self.assertEqual(tmp.rows,80000)
    
    def test_drop_dfs_partition_partitionPaths(self):
        #create_dfs_value_db()
        #self.s.dropPartition(dbPath='dfs://db1',partitionPaths=[1,2])
        #tmp=self.s.loadTable(tableName='pt',dbPath='dfs://db1')
        #self.assertEqual(tmp.rows,80000)
        pass

    def test_drop_dfs_table(self):
        create_dfs_value_db()
        self.s.dropTable(dbPath='dfs://db1',tableName='pt')
        self.assertEqual(self.s.existsTable(dbUrl='dfs://db1',tableName='pt'),False)
    
    def test_drop_disk_table(self):
        create_disk_value_db(WORK_DIR)
        self.s.dropTable(dbPath=WORK_DIR+'/db1',tableName='pt')
        self.assertEqual(self.s.existsTable(dbUrl=WORK_DIR+'/db1',tableName='pt'),False)

if __name__ == '__main__':
    unittest.main()



