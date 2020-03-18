import unittest
import dolphindb as ddb
import os
from setup import *


class DBInfo:
    dfsDBName = 'dfs://testDropTable'
    diskDBName = WORK_DIR + '/testDropTable'
    table1 = 'tb1'
    table2 = 'tb2'


def create_dfs_dimension_db():
    s = ddb.session()
    s.connect(HOST, PORT, "admin", "123456")
    ddb_script = '''
    login('admin','123456')
    dbPath='{db}'
    if(existsDatabase(dbPath))
        dropDatabase(dbPath)
    db=database(dbPath,RANGE,1..10)
    n=100000
    tdata=table(sort(take(2010.01.01..2010.12.31, n)) as date, 1..n as id,take(`AMD`QWE`CES`DOP`ASZ`FSD`BBVC`AWQ`DS, n) as sym,rand(100,n) as val)
    db.createTable(tdata,`{tb1}).append!(tdata)
    db.createTable(tdata,`{tb2}).append!(tdata)
    '''.format(db=DBInfo.dfsDBName, tb1=DBInfo.table1, tb2=DBInfo.table2)
    s.run(ddb_script)
    s.close()


def create_dfs_range_db():
    s = ddb.session()
    s.connect(HOST, PORT, "admin", "123456")
    ddb_script = '''
    login('admin','123456')
    dbPath='{db}'
    if(existsDatabase(dbPath))
        dropDatabase(dbPath)
    db=database(dbPath,RANGE,0..10*10000+1)
    n=100000
    tdata=table(sort(take(2010.01.01..2010.12.31, n)) as date, 1..n as id,take(`AMD`QWE`CES`DOP`ASZ`FSD`BBVC`AWQ`DS, n) as sym,rand(100,n) as val)
    db.createPartitionedTable(tdata,`{tb1},`id).append!(tdata)
    db.createPartitionedTable(tdata,`{tb2},`id).append!(tdata)
    '''.format(db=DBInfo.dfsDBName, tb1=DBInfo.table1, tb2=DBInfo.table2)
    s.run(ddb_script)
    s.close()


def create_dfs_hash_db():
    s = ddb.session()
    s.connect(HOST, PORT, "admin", "123456")
    ddb_script = '''
    login('admin','123456')
    dbPath='{db}'
    if(existsDatabase(dbPath))
        dropDatabase(dbPath)
    db=database(dbPath,HASH,[INT,10])
    n=100000
    tdata=table(sort(take(2010.01.01..2010.12.31, n)) as date, take(1..10,n) as id,take(`AMD`QWE`CES`DOP`ASZ`FSD`BBVC`AWQ`DS, n) as sym,rand(100,n) as val)
    db.createPartitionedTable(tdata,`{tb1},`id).append!(tdata)
    db.createPartitionedTable(tdata,`{tb2},`id).append!(tdata)
    '''.format(db=DBInfo.dfsDBName, tb1=DBInfo.table1, tb2=DBInfo.table2)
    s.run(ddb_script)
    s.close()


def create_dfs_value_db():
    s = ddb.session()
    s.connect(HOST, PORT, "admin", "123456")
    ddb_script = '''
    login('admin','123456')
    dbPath='{db}'
    if(existsDatabase(dbPath))
        dropDatabase(dbPath)
    db=database(dbPath,VALUE,2010.01.01..2010.01.30)
    n=100000
    tdata=table(sort(take(2010.01.01..2010.01.30, n)) as date, take(1..10,n) as id,take(`AMD`QWE`CES`DOP`ASZ`FSD`BBVC`AWQ`DS, n) as sym,rand(100,n) as val)
    db.createPartitionedTable(tdata,`{tb1},`date).append!(tdata)
    db.createPartitionedTable(tdata,`{tb2},`date).append!(tdata)
    '''.format(db=DBInfo.dfsDBName, tb1=DBInfo.table1, tb2=DBInfo.table2)
    s.run(ddb_script)
    s.close()


def create_dfs_list_db():
    s = ddb.session()
    s.connect(HOST, PORT, "admin", "123456")
    ddb_script = '''
    login('admin','123456')
    dbPath='{db}'
    if(existsDatabase(dbPath))
        dropDatabase(dbPath)
    db=database(dbPath,LIST,[`AMD`QWE`CES,`DOP`ASZ,`FSD`BBVC,`AWQ`DS])
    n=100000
    tdata=table(sort(take(2010.01.01..2010.12.31, n)) as date, take(1..10,n) as id,take(`AMD`QWE`CES`DOP`ASZ`FSD`BBVC`AWQ`DS, n) as sym,rand(100,n) as val)
    db.createPartitionedTable(tdata,`{tb1},`sym).append!(tdata)
    db.createPartitionedTable(tdata,`{tb2},`sym).append!(tdata)
    '''.format(db=DBInfo.dfsDBName, tb1=DBInfo.table1, tb2=DBInfo.table2)
    s.run(ddb_script)
    s.close()


def create_dfs_compo_range_range_db():
    s = ddb.session()
    s.connect(HOST, PORT, "admin", "123456")
    ddb_script = '''
    login('admin','123456')
    dbPath='{db}'
    if(existsDatabase(dbPath))
        dropDatabase(dbPath)
    db1=database('',RANGE,2010.01M+0..12)
    db2=database('',RANGE,1 3 5 7 9 11)
    db=database(dbPath,COMPO,[db1,db2])
    n=100000
    tdata=table(sort(take(2010.01.01..2010.12.31, n)) as date, take(1..10,n) as id,take(`AMD`QWE`CES`DOP`ASZ`FSD`BBVC`AWQ`DS, n) as sym,rand(100,n) as val)
    db.createPartitionedTable(tdata,`{tb1},`date`id).append!(tdata)
    db.createPartitionedTable(tdata,`{tb2},`date`id).append!(tdata)
    '''.format(db=DBInfo.dfsDBName, tb1=DBInfo.table1, tb2=DBInfo.table2)
    s.run(ddb_script)
    s.close()


def create_disk_unpartitioned_db():
    s = ddb.session()
    s.connect(HOST, PORT, "admin", "123456")
    ddb_script = '''
    login('admin','123456')
    dbPath='{db}'
    if(exists(dbPath))
        dropDatabase(dbPath)
    db=database(dbPath)
    n=100000
    tdata=table(sort(take(2010.01.01..2010.12.31, n)) as date, 1..n as id,take(`AMD`QWE`CES`DOP`ASZ`FSD`BBVC`AWQ`DS, n) as sym,rand(100,n) as val)
    saveTable(db,tdata,`{tb1})
    saveTable(db,tdata,`{tb2})
    '''.format(db=DBInfo.diskDBName, tb1=DBInfo.table1, tb2=DBInfo.table2)
    print(ddb_script)
    s.run(ddb_script)
    s.close()


def create_disk_range_db():
    s = ddb.session()
    s.connect(HOST, PORT, "admin", "123456")
    ddb_script = '''
    login('admin','123456')
    dbPath='{db}'
    if(existsDatabase(dbPath))
        dropDatabase(dbPath)
    db=database(dbPath,RANGE,0..10*10000+1)
    n=100000
    tdata=table(sort(take(2010.01.01..2010.12.31, n)) as date, 1..n as id,take(`AMD`QWE`CES`DOP`ASZ`FSD`BBVC`AWQ`DS, n) as sym,rand(100,n) as val)
    db.createPartitionedTable(tdata,`{tb1},`id).append!(tdata)
    db.createPartitionedTable(tdata,`{tb2},`id).append!(tdata)
    '''.format(db=DBInfo.diskDBName, tb1=DBInfo.table1, tb2=DBInfo.table2)
    s.run(ddb_script)
    s.close()


def create_disk_hash_db():
    s = ddb.session()
    s.connect(HOST, PORT, "admin", "123456")
    ddb_script = '''
    login('admin','123456')
    dbPath='{db}'
    if(existsDatabase(dbPath))
        dropDatabase(dbPath)
    db=database(dbPath,HASH,[INT,10])
    n=100000
    tdata=table(sort(take(2010.01.01..2010.12.31, n)) as date, take(1..10,n) as id,take(`AMD`QWE`CES`DOP`ASZ`FSD`BBVC`AWQ`DS, n) as sym,rand(100,n) as val)
    db.createPartitionedTable(tdata,`{tb1},`id).append!(tdata)
    db.createPartitionedTable(tdata,`{tb2},`id).append!(tdata)
    '''.format(db=DBInfo.diskDBName, tb1=DBInfo.table1, tb2=DBInfo.table2)
    s.run(ddb_script)
    s.close()


def create_disk_value_db():
    s = ddb.session()
    s.connect(HOST, PORT, "admin", "123456")
    ddb_script = '''
    login('admin','123456')
    dbPath='{db}'
    if(existsDatabase(dbPath))
        dropDatabase(dbPath)
    db=database(dbPath,VALUE,2010.01.01..2010.01.30)
    n=100000
    tdata=table(sort(take(2010.01.01..2010.01.30, n)) as date, take(1..10,n) as id,take(`AMD`QWE`CES`DOP`ASZ`FSD`BBVC`AWQ`DS, n) as sym,rand(100,n) as val)
    db.createPartitionedTable(tdata,`{tb1},`date).append!(tdata)
    db.createPartitionedTable(tdata,`{tb2},`date).append!(tdata)
    '''.format(db=DBInfo.diskDBName, tb1=DBInfo.table1, tb2=DBInfo.table2)
    s.run(ddb_script)
    s.close()


def create_disk_list_db():
    s = ddb.session()
    s.connect(HOST, PORT, "admin", "123456")
    ddb_script = '''
    login('admin','123456')
    dbPath='{db}'
    if(existsDatabase(dbPath))
        dropDatabase(dbPath)
    db=database(dbPath,LIST,[`AMD`QWE`CES,`DOP`ASZ,`FSD`BBVC,`AWQ`DS])
    n=100000
    tdata=table(sort(take(2010.01.01..2010.12.31, n)) as date, take(1..10,n) as id,take(`AMD`QWE`CES`DOP`ASZ`FSD`BBVC`AWQ`DS, n) as sym,rand(100,n) as val)
    db.createPartitionedTable(tdata,`{tb1},`sym).append!(tdata)
    db.createPartitionedTable(tdata,`{tb2},`sym).append!(tdata)
    '''.format(db=DBInfo.diskDBName, tb1=DBInfo.table1, tb2=DBInfo.table2)
    s.run(ddb_script)
    s.close()


def create_disk_compo_range_range_db():
    s = ddb.session()
    s.connect(HOST, PORT, "admin", "123456")
    ddb_script = '''
    login('admin','123456')
    dbPath='{db}'
    if(existsDatabase(dbPath))
        dropDatabase(dbPath)
    db1=database('',RANGE,2010.01M+0..12)
    db2=database('',RANGE,1 3 5 7 9 11)
    db=database(dbPath,COMPO,[db1,db2])
    n=100000
    tdata=table(sort(take(2010.01.01..2010.12.31, n)) as date, take(1..10,n) as id,take(`AMD`QWE`CES`DOP`ASZ`FSD`BBVC`AWQ`DS, n) as sym,rand(100,n) as val)
    db.createPartitionedTable(tdata,`{tb1},`date`id).append!(tdata)
    db.createPartitionedTable(tdata,`{tb2},`date`id).append!(tdata)
    '''.format(db=DBInfo.diskDBName, tb1=DBInfo.table1, tb2=DBInfo.table2)
    s.run(ddb_script)
    s.close()


class TestTable(unittest.TestCase):
    @classmethod
    def setUp(self):
        self.s = ddb.session()
        self.s.connect(HOST, PORT, "admin", "123456")
        dbPaths = [DBInfo.dfsDBName, DBInfo.diskDBName]
        for dbPath in dbPaths:
            if self.s.existsDatabase(dbPath):
                self.s.dropDatabase(dbPath)
            if os.path.exists(dbPath):
                script = """
                if(exists('{dbPath}'))
                    rmdir('{dbPath}', true)
                """.format(dbPath=dbPath)
                self.s.run(script)

    @classmethod
    def tearDown(self):
        self.s = ddb.session()
        self.s.connect(HOST, PORT, "admin", "123456")
        dbPaths = [DBInfo.dfsDBName, DBInfo.diskDBName]
        for dbPath in dbPaths:
            if self.s.existsDatabase(dbPath):
                self.s.dropDatabase(dbPath)
            if os.path.exists(dbPath):
                script = """
                if(exists('{dbPath}'))
                    rmdir('{dbPath}', true)
                """.format(dbPath=dbPath)
                self.s.run(script)

    def test_dropTable_dfs_dimension(self):
        dbName = DBInfo.dfsDBName
        tbName1 = DBInfo.table1
        tbName2 = DBInfo.table2
        self.assertEqual(self.s.existsTable(dbName, tbName1), False)
        self.assertEqual(self.s.existsTable(dbName, tbName2), False)
        create_dfs_dimension_db()
        self.assertEqual(self.s.existsTable(dbName, tbName1), True)
        self.assertEqual(self.s.existsTable(dbName, tbName2), True)
        self.s.dropTable(dbName, tbName1)
        self.assertEqual(self.s.existsTable(dbName, tbName1), False)
        self.assertEqual(self.s.existsTable(dbName, tbName2), True)
        self.s.dropTable(dbName, tbName2)
        self.assertEqual(self.s.existsTable(dbName, tbName1), False)
        self.assertEqual(self.s.existsTable(dbName, tbName2), False)

    def test_dropTable_dfs_range(self):
        dbName = DBInfo.dfsDBName
        tbName1 = DBInfo.table1
        tbName2 = DBInfo.table2
        self.assertEqual(self.s.existsTable(dbName, tbName1), False)
        self.assertEqual(self.s.existsTable(dbName, tbName2), False)
        create_dfs_range_db()
        self.assertEqual(self.s.existsTable(dbName, tbName1), True)
        self.assertEqual(self.s.existsTable(dbName, tbName2), True)
        self.s.dropTable(dbName, tbName1)
        self.assertEqual(self.s.existsTable(dbName, tbName1), False)
        self.assertEqual(self.s.existsTable(dbName, tbName2), True)
        self.s.dropTable(dbName, tbName2)
        self.assertEqual(self.s.existsTable(dbName, tbName1), False)
        self.assertEqual(self.s.existsTable(dbName, tbName2), False)

    def test_dropTable_dfs_hash(self):
        dbName = DBInfo.dfsDBName
        tbName1 = DBInfo.table1
        tbName2 = DBInfo.table2
        self.assertEqual(self.s.existsTable(dbName, tbName1), False)
        self.assertEqual(self.s.existsTable(dbName, tbName2), False)
        create_dfs_hash_db()
        self.assertEqual(self.s.existsTable(dbName, tbName1), True)
        self.assertEqual(self.s.existsTable(dbName, tbName2), True)
        self.s.dropTable(dbName, tbName1)
        self.assertEqual(self.s.existsTable(dbName, tbName1), False)
        self.assertEqual(self.s.existsTable(dbName, tbName2), True)
        self.s.dropTable(dbName, tbName2)
        self.assertEqual(self.s.existsTable(dbName, tbName1), False)
        self.assertEqual(self.s.existsTable(dbName, tbName2), False)

    def test_dropTable_dfs_value(self):
        dbName = DBInfo.dfsDBName
        tbName1 = DBInfo.table1
        tbName2 = DBInfo.table2
        self.assertEqual(self.s.existsTable(dbName, tbName1), False)
        self.assertEqual(self.s.existsTable(dbName, tbName2), False)
        create_dfs_value_db()
        self.assertEqual(self.s.existsTable(dbName, tbName1), True)
        self.assertEqual(self.s.existsTable(dbName, tbName2), True)
        self.s.dropTable(dbName, tbName1)
        self.assertEqual(self.s.existsTable(dbName, tbName1), False)
        self.assertEqual(self.s.existsTable(dbName, tbName2), True)
        self.s.dropTable(dbName, tbName2)
        self.assertEqual(self.s.existsTable(dbName, tbName1), False)
        self.assertEqual(self.s.existsTable(dbName, tbName2), False)

    def test_dropTable_dfs_list(self):
        dbName = DBInfo.dfsDBName
        tbName1 = DBInfo.table1
        tbName2 = DBInfo.table2
        self.assertEqual(self.s.existsTable(dbName, tbName1), False)
        self.assertEqual(self.s.existsTable(dbName, tbName2), False)
        create_dfs_list_db()
        self.assertEqual(self.s.existsTable(dbName, tbName1), True)
        self.assertEqual(self.s.existsTable(dbName, tbName2), True)
        self.s.dropTable(dbName, tbName1)
        self.assertEqual(self.s.existsTable(dbName, tbName1), False)
        self.assertEqual(self.s.existsTable(dbName, tbName2), True)
        self.s.dropTable(dbName, tbName2)
        self.assertEqual(self.s.existsTable(dbName, tbName1), False)
        self.assertEqual(self.s.existsTable(dbName, tbName2), False)

    def test_dropTable_dfs_compo_range_range(self):
        dbName = DBInfo.dfsDBName
        tbName1 = DBInfo.table1
        tbName2 = DBInfo.table2
        self.assertEqual(self.s.existsTable(dbName, tbName1), False)
        self.assertEqual(self.s.existsTable(dbName, tbName2), False)
        create_dfs_compo_range_range_db()
        self.assertEqual(self.s.existsTable(dbName, tbName1), True)
        self.assertEqual(self.s.existsTable(dbName, tbName2), True)
        self.s.dropTable(dbName, tbName1)
        self.assertEqual(self.s.existsTable(dbName, tbName1), False)
        self.assertEqual(self.s.existsTable(dbName, tbName2), True)
        self.s.dropTable(dbName, tbName2)
        self.assertEqual(self.s.existsTable(dbName, tbName1), False)
        self.assertEqual(self.s.existsTable(dbName, tbName2), False)

    def test_dropTable_disk_unpartitioned(self):
        dbName = DBInfo.diskDBName
        tbName1 = DBInfo.table1
        tbName2 = DBInfo.table2
        self.assertEqual(self.s.existsTable(dbName, tbName1), False)
        self.assertEqual(self.s.existsTable(dbName, tbName2), False)
        create_disk_unpartitioned_db()
        self.assertEqual(self.s.existsTable(dbName, tbName1), True)
        self.assertEqual(self.s.existsTable(dbName, tbName2), True)
        self.s.dropTable(dbName, tbName1)
        self.assertEqual(self.s.existsTable(dbName, tbName1), False)
        self.assertEqual(self.s.existsTable(dbName, tbName2), True)
        self.s.dropTable(dbName, tbName2)
        self.assertEqual(self.s.existsTable(dbName, tbName1), False)
        self.assertEqual(self.s.existsTable(dbName, tbName2), False)

    def test_dropTable_disk_range(self):
        dbName = DBInfo.diskDBName
        tbName1 = DBInfo.table1
        tbName2 = DBInfo.table2
        self.assertEqual(self.s.existsTable(dbName, tbName1), False)
        self.assertEqual(self.s.existsTable(dbName, tbName2), False)
        create_disk_range_db()
        self.assertEqual(self.s.existsTable(dbName, tbName1), True)
        self.assertEqual(self.s.existsTable(dbName, tbName2), True)
        self.s.dropTable(dbName, tbName1)
        self.assertEqual(self.s.existsTable(dbName, tbName1), False)
        self.assertEqual(self.s.existsTable(dbName, tbName2), True)
        self.s.dropTable(dbName, tbName2)
        self.assertEqual(self.s.existsTable(dbName, tbName1), False)
        self.assertEqual(self.s.existsTable(dbName, tbName2), False)

    def test_dropTable_disk_hash(self):
        dbName = DBInfo.diskDBName
        tbName1 = DBInfo.table1
        tbName2 = DBInfo.table2
        self.assertEqual(self.s.existsTable(dbName, tbName1), False)
        self.assertEqual(self.s.existsTable(dbName, tbName2), False)
        create_disk_hash_db()
        self.assertEqual(self.s.existsTable(dbName, tbName1), True)
        self.assertEqual(self.s.existsTable(dbName, tbName2), True)
        self.s.dropTable(dbName, tbName1)
        self.assertEqual(self.s.existsTable(dbName, tbName1), False)
        self.assertEqual(self.s.existsTable(dbName, tbName2), True)
        self.s.dropTable(dbName, tbName2)
        self.assertEqual(self.s.existsTable(dbName, tbName1), False)
        self.assertEqual(self.s.existsTable(dbName, tbName2), False)

    def test_dropTable_disk_value(self):
        dbName = DBInfo.diskDBName
        tbName1 = DBInfo.table1
        tbName2 = DBInfo.table2
        self.assertEqual(self.s.existsTable(dbName, tbName1), False)
        self.assertEqual(self.s.existsTable(dbName, tbName2), False)
        create_disk_value_db()
        self.assertEqual(self.s.existsTable(dbName, tbName1), True)
        self.assertEqual(self.s.existsTable(dbName, tbName2), True)
        self.s.dropTable(dbName, tbName1)
        self.assertEqual(self.s.existsTable(dbName, tbName1), False)
        self.assertEqual(self.s.existsTable(dbName, tbName2), True)
        self.s.dropTable(dbName, tbName2)
        self.assertEqual(self.s.existsTable(dbName, tbName1), False)
        self.assertEqual(self.s.existsTable(dbName, tbName2), False)

    def test_dropTable_disk_list(self):
        dbName = DBInfo.diskDBName
        tbName1 = DBInfo.table1
        tbName2 = DBInfo.table2
        self.assertEqual(self.s.existsTable(dbName, tbName1), False)
        self.assertEqual(self.s.existsTable(dbName, tbName2), False)
        create_disk_list_db()
        self.assertEqual(self.s.existsTable(dbName, tbName1), True)
        self.assertEqual(self.s.existsTable(dbName, tbName2), True)
        self.s.dropTable(dbName, tbName1)
        self.assertEqual(self.s.existsTable(dbName, tbName1), False)
        self.assertEqual(self.s.existsTable(dbName, tbName2), True)
        self.s.dropTable(dbName, tbName2)
        self.assertEqual(self.s.existsTable(dbName, tbName1), False)
        self.assertEqual(self.s.existsTable(dbName, tbName2), False)

    def test_dropTable_disk_compo_range_range(self):
        dbName = DBInfo.diskDBName
        tbName1 = DBInfo.table1
        tbName2 = DBInfo.table2
        self.assertEqual(self.s.existsTable(dbName, tbName1), False)
        self.assertEqual(self.s.existsTable(dbName, tbName2), False)
        create_disk_compo_range_range_db()
        self.assertEqual(self.s.existsTable(dbName, tbName1), True)
        self.assertEqual(self.s.existsTable(dbName, tbName2), True)
        self.s.dropTable(dbName, tbName1)
        self.assertEqual(self.s.existsTable(dbName, tbName1), False)
        self.assertEqual(self.s.existsTable(dbName, tbName2), True)
        self.s.dropTable(dbName, tbName2)
        self.assertEqual(self.s.existsTable(dbName, tbName1), False)
        self.assertEqual(self.s.existsTable(dbName, tbName2), False)


if __name__ == '__main__':
    unittest.main()
