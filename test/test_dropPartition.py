import unittest
import dolphindb as ddb
from setup import *


class DBInfo:
    dfsDBName = 'dfs://testDropPartition'
    table1 = 'tb1'
    table2 = 'tb2'


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


def create_dfs_compo_range_hash_db():
    s = ddb.session()
    s.connect(HOST, PORT, "admin", "123456")
    ddb_script = '''
    login('admin','123456')
    dbPath='{db}'
    if(existsDatabase(dbPath))
        dropDatabase(dbPath)
    db1=database('',RANGE,2010.01M+0..12)
    db2=database('',HASH,[INT,10])
    db=database(dbPath,COMPO,[db1,db2])
    n=100000
    tdata=table(sort(take(2010.01.01..2010.12.31, n)) as date, take(1..10,n) as id,take(`AMD`QWE`CES`DOP`ASZ`FSD`BBVC`AWQ`DS, n) as sym,rand(100,n) as val)
    db.createPartitionedTable(tdata,`{tb1},`date`id).append!(tdata)
    db.createPartitionedTable(tdata,`{tb2},`date`id).append!(tdata)
    '''.format(db=DBInfo.dfsDBName, tb1=DBInfo.table1, tb2=DBInfo.table2)
    s.run(ddb_script)
    s.close()


def create_dfs_compo_range_value_db():
    s = ddb.session()
    s.connect(HOST, PORT, "admin", "123456")
    ddb_script = '''
    login('admin','123456')
    dbPath='{db}'
    if(existsDatabase(dbPath))
        dropDatabase(dbPath)
    db1=database('',RANGE,2010.01M+0..12)
    db2=database('',VALUE,1..10)
    db=database(dbPath,COMPO,[db1,db2])
    n=100000
    tdata=table(sort(take(2010.01.01..2010.12.31, n)) as date, take(1..10,n) as id,take(`AMD`QWE`CES`DOP`ASZ`FSD`BBVC`AWQ`DS, n) as sym,rand(100,n) as val)
    db.createPartitionedTable(tdata,`{tb1},`date`id).append!(tdata)
    db.createPartitionedTable(tdata,`{tb2},`date`id).append!(tdata)
    '''.format(db=DBInfo.dfsDBName, tb1=DBInfo.table1, tb2=DBInfo.table2)
    s.run(ddb_script)
    s.close()


def create_dfs_compo_range_list_db():
    s = ddb.session()
    s.connect(HOST, PORT, "admin", "123456")
    ddb_script = '''
    login('admin','123456')
    dbPath='{db}'
    if(existsDatabase(dbPath))
        dropDatabase(dbPath)
    db1=database('',RANGE,2010.01M+0..12)
    db2=database('',LIST,[`AMD`QWE`CES,`DOP`ASZ,`FSD`BBVC,`AWQ`DS])
    db=database(dbPath,COMPO,[db1,db2])
    n=100000
    tdata=table(sort(take(2010.01.01..2010.12.31, n)) as date, take(1..10,n) as id,take(`AMD`QWE`CES`DOP`ASZ`FSD`BBVC`AWQ`DS, n) as sym,rand(100,n) as val)
    db.createPartitionedTable(tdata,`{tb1},`date`sym).append!(tdata)
    db.createPartitionedTable(tdata,`{tb2},`date`sym).append!(tdata)
    '''.format(db=DBInfo.dfsDBName, tb1=DBInfo.table1, tb2=DBInfo.table2)
    s.run(ddb_script)
    s.close()


def create_dfs_compo_range_hash_list_db():
    s = ddb.session()
    s.connect(HOST, PORT, "admin", "123456")
    ddb_script = '''
    login('admin','123456')
    dbPath='{db}'
    if(existsDatabase(dbPath))
        dropDatabase(dbPath)
    db1=database('',RANGE,2010.01M+0..12)
    db2=database('',HASH,[INT,10])
    db3=database('',LIST,[`AMD`QWE`CES,`DOP`ASZ,`FSD`BBVC,`AWQ`DS])
    db=database(dbPath,COMPO,[db1,db2,db3])
    n=100000
    tdata=table(sort(take(2010.01.01..2010.12.31, n)) as date, take(1..10,n) as id,take(`AMD`QWE`CES`DOP`ASZ`FSD`BBVC`AWQ`DS, n) as sym,rand(100,n) as val)
    db.createPartitionedTable(tdata,`{tb1},`date`id`sym).append!(tdata)
    db.createPartitionedTable(tdata,`{tb2},`date`id`sym).append!(tdata)
    '''.format(db=DBInfo.dfsDBName, tb1=DBInfo.table1, tb2=DBInfo.table2)
    s.run(ddb_script)
    s.close()


def create_dfs_compo_range_value_list_db():
    s = ddb.session()
    s.connect(HOST, PORT, "admin", "123456")
    ddb_script = '''
    login('admin','123456')
    dbPath='{db}'
    if(existsDatabase(dbPath))
        dropDatabase(dbPath)
    db1=database('',RANGE,2010.01M+0..12)
    db2=database('',VALUE,1..10)
    db3=database('',LIST,[`AMD`QWE`CES,`DOP`ASZ,`FSD`BBVC,`AWQ`DS])
    db=database(dbPath,COMPO,[db1,db2,db3])
    n=100000
    tdata=table(sort(take(2010.01.01..2010.12.31, n)) as date, take(1..10,n) as id,take(`AMD`QWE`CES`DOP`ASZ`FSD`BBVC`AWQ`DS, n) as sym,rand(100,n) as val)
    db.createPartitionedTable(tdata,`{tb1},`date`id`sym).append!(tdata)
    db.createPartitionedTable(tdata,`{tb2},`date`id`sym).append!(tdata)
    '''.format(db=DBInfo.dfsDBName, tb1=DBInfo.table1, tb2=DBInfo.table2)
    s.run(ddb_script)
    s.close()


class TestDropPartition(unittest.TestCase):
    @classmethod
    def setUp(self):
        self.s = ddb.session()
        self.s.connect(HOST, PORT, "admin", "123456")

    @classmethod
    def tearDown(self):
        self.s = ddb.session()
        self.s.connect(HOST, PORT, "admin", "123456")
        dbPath = DBInfo.dfsDBName
        if self.s.existsDatabase(dbPath):
            self.s.dropDatabase(dbPath)

    def test_dropPartition_dfs_range_drop_single(self):
        dbPath = DBInfo.dfsDBName
        tbName1 = DBInfo.table1
        tbName2 = DBInfo.table2
        create_dfs_range_db()

        origin = self.s.loadTable(tbName1, dbPath)
        total = origin.rows

        drop = self.s.loadTableBySQL(tableName=tbName1, dbPath=dbPath,
                                     sql="select * from {tb} where id<10001".format(tb=tbName1))
        rs = total - drop.rows

        # tb1 dropPartition
        tmp = self.s.loadTable(tbName1, dbPath)
        self.assertEqual(tmp.rows, total)
        self.s.dropPartition(dbPath, ["'/1_10001'"], tbName1)
        tmp = self.s.loadTable(tbName1, dbPath)
        self.assertEqual(tmp.rows, rs)

        # tb2 dropPartition
        tmp = self.s.loadTable(tbName2, dbPath)
        self.assertEqual(tmp.rows, total)
        self.s.dropPartition(dbPath, ["'/1_10001'"], tbName2)
        tmp = self.s.loadTable(tbName2, dbPath)
        self.assertEqual(tmp.rows, rs)

        # both dropPartition
        create_dfs_range_db()
        tmp = self.s.loadTable(tbName1, dbPath)
        self.assertEqual(tmp.rows, total)
        tmp = self.s.loadTable(tbName2, dbPath)
        self.assertEqual(tmp.rows, total)
        self.s.dropPartition(dbPath, ["'/1_10001'"])
        tmp = self.s.loadTable(tbName1, dbPath)
        self.assertEqual(tmp.rows, rs)
        tmp = self.s.loadTable(tbName2, dbPath)
        self.assertEqual(tmp.rows, rs)

    def test_dropPartition_dfs_range_drop_multiple(self):
        dbPath = DBInfo.dfsDBName
        tbName1 = DBInfo.table1
        tbName2 = DBInfo.table2
        create_dfs_range_db()

        origin = self.s.loadTable(tbName1, dbPath)
        total = origin.rows

        drop = self.s.loadTableBySQL(tableName=tbName1, dbPath=dbPath,
                                     sql="select * from {tb} where id<40001".format(tb=tbName1))
        rs = total - drop.rows

        # tb1 dropPartition
        tmp = self.s.loadTable(tbName1, dbPath)
        self.assertEqual(tmp.rows, total)
        self.s.dropPartition(dbPath, ["'/1_10001'", "'/10001_20001'", "'/20001_30001'", "'/30001_40001'"], tbName1)
        tmp = self.s.loadTable(tbName1, dbPath)
        self.assertEqual(tmp.rows, rs)

        # tb2 dropPartition
        tmp = self.s.loadTable(tbName2, dbPath)
        self.assertEqual(tmp.rows, total)
        self.s.dropPartition(dbPath, ["'/1_10001'", "'/10001_20001'", "'/20001_30001'", "'/30001_40001'"], tbName2)
        tmp = self.s.loadTable(tbName2, dbPath)
        self.assertEqual(tmp.rows, rs)

        # both dropPartition
        create_dfs_range_db()
        tmp = self.s.loadTable(tbName1, dbPath)
        self.assertEqual(tmp.rows, total)
        tmp = self.s.loadTable(tbName2, dbPath)
        self.assertEqual(tmp.rows, total)
        self.s.dropPartition(dbPath, ["'/1_10001'", "'/10001_20001'", "'/20001_30001'", "'/30001_40001'"])
        tmp = self.s.loadTable(tbName1, dbPath)
        self.assertEqual(tmp.rows, rs)
        tmp = self.s.loadTable(tbName2, dbPath)
        self.assertEqual(tmp.rows, rs)

    def test_dropPartition_dfs_hash_drop_single(self):
        dbPath = DBInfo.dfsDBName
        tbName1 = DBInfo.table1
        tbName2 = DBInfo.table2
        create_dfs_hash_db()

        origin = self.s.loadTable(tbName1, dbPath)
        total = origin.rows

        drop = self.s.loadTableBySQL(tableName=tbName1, dbPath=dbPath,
                                     sql="select * from {tb} where id=10".format(tb=tbName1))
        rs = total - drop.rows

        # tb1 dropPartition
        tmp = self.s.loadTable(tbName1, dbPath)
        self.assertEqual(tmp.rows, total)
        self.s.dropPartition(dbPath, ['0'], tbName1)
        tmp = self.s.loadTable(tbName1, dbPath)
        self.assertEqual(tmp.rows, rs)

        # tb2 dropPartition
        tmp = self.s.loadTable(tbName2, dbPath)
        self.assertEqual(tmp.rows, total)
        self.s.dropPartition(dbPath, ['0'], tbName2)
        tmp = self.s.loadTable(tbName2, dbPath)
        self.assertEqual(tmp.rows, rs)

        # both dropPartition
        create_dfs_hash_db()
        tmp = self.s.loadTable(tbName1, dbPath)
        self.assertEqual(tmp.rows, total)
        tmp = self.s.loadTable(tbName2, dbPath)
        self.assertEqual(tmp.rows, total)
        self.s.dropPartition(dbPath, ['0'])
        tmp = self.s.loadTable(tbName1, dbPath)
        self.assertEqual(tmp.rows, rs)
        tmp = self.s.loadTable(tbName2, dbPath)
        self.assertEqual(tmp.rows, rs)

    def test_dropPartition_dfs_hash_drop_multiple(self):
        dbPath = DBInfo.dfsDBName
        tbName1 = DBInfo.table1
        tbName2 = DBInfo.table2
        create_dfs_hash_db()

        origin = self.s.loadTable(tbName1, dbPath)
        total = origin.rows

        drop = self.s.loadTableBySQL(tableName=tbName1, dbPath=dbPath,
                                     sql="select * from {tb} where id in [1,10]".format(tb=tbName1))
        rs = total - drop.rows

        # tb1 dropPartition
        tmp = self.s.loadTable(tbName1, dbPath)
        self.assertEqual(tmp.rows, total)
        self.s.dropPartition(dbPath, ['0', '1'], tbName1)
        tmp = self.s.loadTable(tbName1, dbPath)
        self.assertEqual(tmp.rows, rs)

        # tb2 dropPartition
        tmp = self.s.loadTable(tbName2, dbPath)
        self.assertEqual(tmp.rows, total)
        self.s.dropPartition(dbPath, ['0', '1'], tbName2)
        tmp = self.s.loadTable(tbName2, dbPath)
        self.assertEqual(tmp.rows, rs)

        # both dropPartition
        create_dfs_hash_db()
        tmp = self.s.loadTable(tbName1, dbPath)
        self.assertEqual(tmp.rows, total)
        tmp = self.s.loadTable(tbName2, dbPath)
        self.assertEqual(tmp.rows, total)
        self.s.dropPartition(dbPath, ['0', '1'])
        tmp = self.s.loadTable(tbName1, dbPath)
        self.assertEqual(tmp.rows, rs)
        tmp = self.s.loadTable(tbName2, dbPath)
        self.assertEqual(tmp.rows, rs)

    def test_dropPartition_dfs_value_drop_single(self):
        dbPath = DBInfo.dfsDBName
        tbName1 = DBInfo.table1
        tbName2 = DBInfo.table2
        create_dfs_value_db()

        origin = self.s.loadTable(tbName1, dbPath)
        total = origin.rows

        drop = self.s.loadTableBySQL(tableName=tbName1, dbPath=dbPath,
                                     sql="select * from {tb} where date=2010.01.01".format(tb=tbName1))
        rs = total - drop.rows

        # tb1 dropPartition
        tmp = self.s.loadTable(tbName1, dbPath)
        self.assertEqual(tmp.rows, total)
        self.s.dropPartition(dbPath, ['2010.01.01'], tbName1)
        tmp = self.s.loadTable(tbName1, dbPath)
        self.assertEqual(tmp.rows, rs)

        # tb2 dropPartition
        tmp = self.s.loadTable(tbName2, dbPath)
        self.assertEqual(tmp.rows, total)
        self.s.dropPartition(dbPath, ['2010.01.01'], tbName2)
        tmp = self.s.loadTable(tbName2, dbPath)
        self.assertEqual(tmp.rows, rs)

        # both dropPartition
        create_dfs_value_db()
        tmp = self.s.loadTable(tbName1, dbPath)
        self.assertEqual(tmp.rows, total)
        tmp = self.s.loadTable(tbName2, dbPath)
        self.assertEqual(tmp.rows, total)
        self.s.dropPartition(dbPath, ['2010.01.01'])
        tmp = self.s.loadTable(tbName1, dbPath)
        self.assertEqual(tmp.rows, rs)
        tmp = self.s.loadTable(tbName2, dbPath)
        self.assertEqual(tmp.rows, rs)

    def test_dropPartition_dfs_value_drop_multiple(self):
        dbPath = DBInfo.dfsDBName
        tbName1 = DBInfo.table1
        tbName2 = DBInfo.table2
        create_dfs_value_db()

        origin = self.s.loadTable(tbName1, dbPath)
        total = origin.rows

        drop = self.s.loadTableBySQL(tableName=tbName1, dbPath=dbPath,
                                     sql="select * from {tb} where date in 2010.01.01+[0,7,14,21]".format(tb=tbName1))
        rs = total - drop.rows

        # tb1 dropPartition
        tmp = self.s.loadTable(tbName1, dbPath)
        self.assertEqual(tmp.rows, total)
        self.s.dropPartition(dbPath, ['2010.01.01', '2010.01.08', '2010.01.15', '2010.01.22'], tbName1)
        tmp = self.s.loadTable(tbName1, dbPath)
        self.assertEqual(tmp.rows, rs)

        # tb2 dropPartition
        tmp = self.s.loadTable(tbName2, dbPath)
        self.assertEqual(tmp.rows, total)
        self.s.dropPartition(dbPath, ['2010.01.01', '2010.01.08', '2010.01.15', '2010.01.22'], tbName2)
        tmp = self.s.loadTable(tbName2, dbPath)
        self.assertEqual(tmp.rows, rs)

        # both dropPartition
        create_dfs_value_db()
        tmp = self.s.loadTable(tbName1, dbPath)
        self.assertEqual(tmp.rows, total)
        tmp = self.s.loadTable(tbName2, dbPath)
        self.assertEqual(tmp.rows, total)
        self.s.dropPartition(dbPath, ['2010.01.01', '2010.01.08', '2010.01.15', '2010.01.22'])
        tmp = self.s.loadTable(tbName1, dbPath)
        self.assertEqual(tmp.rows, rs)
        tmp = self.s.loadTable(tbName2, dbPath)
        self.assertEqual(tmp.rows, rs)

    def test_dropPartition_dfs_list_drop_single(self):
        dbPath = DBInfo.dfsDBName
        tbName1 = DBInfo.table1
        tbName2 = DBInfo.table2
        create_dfs_list_db()

        origin = self.s.loadTable(tbName1, dbPath)
        total = origin.rows

        drop = self.s.loadTableBySQL(tableName=tbName1, dbPath=dbPath,
                                     sql="select * from {tb} where sym in `AMD`QWE`CES".format(tb=tbName1))
        rs = total - drop.rows

        # tb1 dropPartition
        tmp = self.s.loadTable(tbName1, dbPath)
        self.assertEqual(tmp.rows, total)
        self.s.dropPartition(dbPath, ["'/List0'"], tbName1)
        tmp = self.s.loadTable(tbName1, dbPath)
        self.assertEqual(tmp.rows, rs)

        # tb2 dropPartition
        tmp = self.s.loadTable(tbName2, dbPath)
        self.assertEqual(tmp.rows, total)
        self.s.dropPartition(dbPath, ["'/List0'"], tbName2)
        tmp = self.s.loadTable(tbName2, dbPath)
        self.assertEqual(tmp.rows, rs)

        # both dropPartition
        create_dfs_list_db()
        tmp = self.s.loadTable(tbName1, dbPath)
        self.assertEqual(tmp.rows, total)
        tmp = self.s.loadTable(tbName2, dbPath)
        self.assertEqual(tmp.rows, total)
        self.s.dropPartition(dbPath, ["'/List0'"])
        tmp = self.s.loadTable(tbName1, dbPath)
        self.assertEqual(tmp.rows, rs)
        tmp = self.s.loadTable(tbName2, dbPath)
        self.assertEqual(tmp.rows, rs)

    def test_dropPartition_dfs_list_drop_multiple(self):
        dbPath = DBInfo.dfsDBName
        tbName1 = DBInfo.table1
        tbName2 = DBInfo.table2
        create_dfs_list_db()

        origin = self.s.loadTable(tbName1, dbPath)
        total = origin.rows
        # [`AMD`QWE`CES, `DOP`ASZ, `FSD`BBVC, `AWQ`DS]
        drop = self.s.loadTableBySQL(tableName=tbName1, dbPath=dbPath,
                                     sql="select * from {tb} where sym in `DOP`ASZ`FSD`BBVC".format(tb=tbName1))
        rs = total - drop.rows

        # tb1 dropPartition
        tmp = self.s.loadTable(tbName1, dbPath)
        self.assertEqual(tmp.rows, total)
        self.s.dropPartition(dbPath, ["'/List1'", "'/List2'"], tbName1)
        tmp = self.s.loadTable(tbName1, dbPath)
        self.assertEqual(tmp.rows, rs)

        # tb2 dropPartition
        tmp = self.s.loadTable(tbName2, dbPath)
        self.assertEqual(tmp.rows, total)
        self.s.dropPartition(dbPath, ["'/List1'", "'/List2'"], tbName2)
        tmp = self.s.loadTable(tbName2, dbPath)
        self.assertEqual(tmp.rows, rs)

        # both dropPartition
        create_dfs_list_db()
        tmp = self.s.loadTable(tbName1, dbPath)
        self.assertEqual(tmp.rows, total)
        tmp = self.s.loadTable(tbName2, dbPath)
        self.assertEqual(tmp.rows, total)
        self.s.dropPartition(dbPath, ["'/List1'", "'/List2'"])
        tmp = self.s.loadTable(tbName1, dbPath)
        self.assertEqual(tmp.rows, rs)
        tmp = self.s.loadTable(tbName2, dbPath)
        self.assertEqual(tmp.rows, rs)

    def test_dropPartition_dfs_compo2_range_range_drop_level1_single(self):
        dbPath = DBInfo.dfsDBName
        tbName1 = DBInfo.table1
        tbName2 = DBInfo.table2
        create_dfs_compo_range_range_db()

        origin = self.s.loadTable(tbName1, dbPath)
        total = origin.rows

        drop = self.s.loadTableBySQL(tableName=tbName1, dbPath=dbPath,
                                     sql="select * from {tb} where date<2010.02.01".format(tb=tbName1))
        rs = total - drop.rows

        # tb1 dropPartition
        tmp = self.s.loadTable(tbName1, dbPath)
        self.assertEqual(tmp.rows, total)
        self.s.dropPartition(dbPath, ["2010.01.01"], tbName1)

        tmp = self.s.loadTable(tbName1, dbPath)
        self.assertEqual(tmp.rows, rs)

        # tb2 dropPartition
        tmp = self.s.loadTable(tbName2, dbPath)
        self.assertEqual(tmp.rows, total)
        self.s.dropPartition(dbPath, ["2010.01.01"], tbName2)
        tmp = self.s.loadTable(tbName2, dbPath)
        self.assertEqual(tmp.rows, rs)

        # both dropPartition
        create_dfs_compo_range_range_db()
        tmp = self.s.loadTable(tbName1, dbPath)
        self.assertEqual(tmp.rows, total)
        tmp = self.s.loadTable(tbName2, dbPath)
        self.assertEqual(tmp.rows, total)
        self.s.dropPartition(dbPath, ["2010.01.01"])
        tmp = self.s.loadTable(tbName1, dbPath)
        self.assertEqual(tmp.rows, rs)
        tmp = self.s.loadTable(tbName2, dbPath)
        self.assertEqual(tmp.rows, rs)

    def test_dropPartition_dfs_compo2_range_range_drop_level1_multiple(self):
        dbPath = DBInfo.dfsDBName
        tbName1 = DBInfo.table1
        tbName2 = DBInfo.table2
        create_dfs_compo_range_range_db()

        origin = self.s.loadTable(tbName1, dbPath)
        total = origin.rows

        drop = self.s.loadTableBySQL(tableName=tbName1, dbPath=dbPath,
                                     sql="select * from {tb} where date<2010.03.01".format(tb=tbName1))
        rs = total - drop.rows

        # tb1 dropPartition
        tmp = self.s.loadTable(tbName1, dbPath)
        self.assertEqual(tmp.rows, total)
        self.s.dropPartition(dbPath, ["2010.01.01", "2010.02.01"], tbName1)

        tmp = self.s.loadTable(tbName1, dbPath)
        self.assertEqual(tmp.rows, rs)

        # tb2 dropPartition
        tmp = self.s.loadTable(tbName2, dbPath)
        self.assertEqual(tmp.rows, total)
        self.s.dropPartition(dbPath, ["2010.01.01", "2010.02.01"], tbName2)
        tmp = self.s.loadTable(tbName2, dbPath)
        self.assertEqual(tmp.rows, rs)

        # both dropPartition
        create_dfs_compo_range_range_db()
        tmp = self.s.loadTable(tbName1, dbPath)
        self.assertEqual(tmp.rows, total)
        tmp = self.s.loadTable(tbName2, dbPath)
        self.assertEqual(tmp.rows, total)
        self.s.dropPartition(dbPath, ["2010.01.01", "2010.02.01"])
        tmp = self.s.loadTable(tbName1, dbPath)
        self.assertEqual(tmp.rows, rs)
        tmp = self.s.loadTable(tbName2, dbPath)
        self.assertEqual(tmp.rows, rs)

    def test_dropPartition_dfs_compo2_range_range_drop_level2_single(self):
        dbPath = DBInfo.dfsDBName
        tbName1 = DBInfo.table1
        tbName2 = DBInfo.table2
        create_dfs_compo_range_range_db()

        origin = self.s.loadTable(tbName1, dbPath)
        total = origin.rows

        drop = self.s.loadTableBySQL(tableName=tbName1, dbPath=dbPath,
                                     sql="select * from {tb} where date<2010.02.01 and id<3".format(tb=tbName1))
        rs = total - drop.rows

        # tb1 dropPartition
        tmp = self.s.loadTable(tbName1, dbPath)
        self.assertEqual(tmp.rows, total)
        self.s.dropPartition(dbPath, ["2010.01.01", "1"], tbName1)
        tmp = self.s.loadTable(tbName1, dbPath)
        self.assertEqual(tmp.rows, rs)

        # tb2 dropPartition
        tmp = self.s.loadTable(tbName2, dbPath)
        self.assertEqual(tmp.rows, total)
        self.s.dropPartition(dbPath, ["2010.01.01", "1"], tbName2)
        tmp = self.s.loadTable(tbName2, dbPath)
        self.assertEqual(tmp.rows, rs)

        # both dropPartition
        create_dfs_compo_range_range_db()
        tmp = self.s.loadTable(tbName1, dbPath)
        self.assertEqual(tmp.rows, total)
        tmp = self.s.loadTable(tbName2, dbPath)
        self.assertEqual(tmp.rows, total)
        self.s.dropPartition(dbPath, ["2010.01.01", "1"])
        tmp = self.s.loadTable(tbName1, dbPath)
        self.assertEqual(tmp.rows, rs)
        tmp = self.s.loadTable(tbName2, dbPath)
        self.assertEqual(tmp.rows, rs)

    def test_dropPartition_dfs_compo2_range_range_drop_level2_multiple(self):
        dbPath = DBInfo.dfsDBName
        tbName1 = DBInfo.table1
        tbName2 = DBInfo.table2
        create_dfs_compo_range_range_db()

        origin = self.s.loadTable(tbName1, dbPath)
        total = origin.rows

        drop = self.s.loadTableBySQL(tableName=tbName1, dbPath=dbPath,
                                     sql="select * from {tb} where date<2010.02.01 and id between 3:6".format(tb=tbName1))
        rs = total - drop.rows

        # tb1 dropPartition
        tmp = self.s.loadTable(tbName1, dbPath)
        self.assertEqual(tmp.rows, total)
        self.s.dropPartition(dbPath, ["2010.01.01", "[3,5]"], tbName1)  # 1
        tmp = self.s.loadTable(tbName1, dbPath)
        self.assertEqual(tmp.rows, rs)

        # tb2 dropPartition
        tmp = self.s.loadTable(tbName2, dbPath)
        self.assertEqual(tmp.rows, total)
        self.s.dropPartition(dbPath, ["2010.01.01", "[3,5]"], tbName2)
        tmp = self.s.loadTable(tbName2, dbPath)
        self.assertEqual(tmp.rows, rs)

        # both dropPartition
        create_dfs_compo_range_range_db()
        tmp = self.s.loadTable(tbName1, dbPath)
        self.assertEqual(tmp.rows, total)
        tmp = self.s.loadTable(tbName2, dbPath)
        self.assertEqual(tmp.rows, total)
        self.s.dropPartition(dbPath, ["2010.01.01", "[3,5]"])
        tmp = self.s.loadTable(tbName1, dbPath)
        self.assertEqual(tmp.rows, rs)
        tmp = self.s.loadTable(tbName2, dbPath)
        self.assertEqual(tmp.rows, rs)

    def test_dropPartition_dfs_compo2_range_range_drop_multiple(self):
        dbPath = DBInfo.dfsDBName
        tbName1 = DBInfo.table1
        tbName2 = DBInfo.table2
        create_dfs_compo_range_range_db()

        origin = self.s.loadTable(tbName1, dbPath)
        total = origin.rows

        drop = self.s.loadTableBySQL(tableName=tbName1, dbPath=dbPath,
                                     sql="select * from {tb} where date < 2010.03.01 and id between 3:6".format(
                                         tb=tbName1))
        rs = total - drop.rows

        # tb1 dropPartition
        tmp = self.s.loadTable(tbName1, dbPath)
        self.assertEqual(tmp.rows, total)
        self.s.dropPartition(dbPath, ["[2010.01.01,2010.02.01]", "[3,5]"], tbName1)  # 1
        tmp = self.s.loadTable(tbName1, dbPath)
        self.assertEqual(tmp.rows, rs)

        # tb2 dropPartition
        tmp = self.s.loadTable(tbName2, dbPath)
        self.assertEqual(tmp.rows, total)
        self.s.dropPartition(dbPath, ["[2010.01.01,2010.02.01]", "[3,5]"], tbName2)
        tmp = self.s.loadTable(tbName2, dbPath)
        self.assertEqual(tmp.rows, rs)

        # both dropPartition
        create_dfs_compo_range_range_db()
        tmp = self.s.loadTable(tbName1, dbPath)
        self.assertEqual(tmp.rows, total)
        tmp = self.s.loadTable(tbName2, dbPath)
        self.assertEqual(tmp.rows, total)
        self.s.dropPartition(dbPath, ["[2010.01.01,2010.02.01]", "[3,5]"])
        tmp = self.s.loadTable(tbName1, dbPath)
        self.assertEqual(tmp.rows, rs)
        tmp = self.s.loadTable(tbName2, dbPath)
        self.assertEqual(tmp.rows, rs)

    def test_dropPartition_dfs_compo2_range_hash_drop_level1_single(self):
        dbPath = DBInfo.dfsDBName
        tbName1 = DBInfo.table1
        tbName2 = DBInfo.table2
        create_dfs_compo_range_hash_db()

        origin = self.s.loadTable(tbName1, dbPath)
        total = origin.rows

        drop = self.s.loadTableBySQL(tableName=tbName1, dbPath=dbPath,
                                     sql="select * from {tb} where date<2010.02.01".format(tb=tbName1))
        rs = total - drop.rows

        # tb1 dropPartition
        tmp = self.s.loadTable(tbName1, dbPath)
        self.assertEqual(tmp.rows, total)
        self.s.dropPartition(dbPath, ["2010.01.01"], tbName1)

        tmp = self.s.loadTable(tbName1, dbPath)
        self.assertEqual(tmp.rows, rs)

        # tb2 dropPartition
        tmp = self.s.loadTable(tbName2, dbPath)
        self.assertEqual(tmp.rows, total)
        self.s.dropPartition(dbPath, ["2010.01.01"], tbName2)
        tmp = self.s.loadTable(tbName2, dbPath)
        self.assertEqual(tmp.rows, rs)

        # both dropPartition
        create_dfs_compo_range_hash_db()
        tmp = self.s.loadTable(tbName1, dbPath)
        self.assertEqual(tmp.rows, total)
        tmp = self.s.loadTable(tbName2, dbPath)
        self.assertEqual(tmp.rows, total)
        self.s.dropPartition(dbPath, ["2010.01.01"])
        tmp = self.s.loadTable(tbName1, dbPath)
        self.assertEqual(tmp.rows, rs)
        tmp = self.s.loadTable(tbName2, dbPath)
        self.assertEqual(tmp.rows, rs)

    def test_dropPartition_dfs_compo2_range_hash_drop_level1_multiple(self):
        dbPath = DBInfo.dfsDBName
        tbName1 = DBInfo.table1
        tbName2 = DBInfo.table2
        create_dfs_compo_range_hash_db()

        origin = self.s.loadTable(tbName1, dbPath)
        total = origin.rows

        drop = self.s.loadTableBySQL(tableName=tbName1, dbPath=dbPath,
                                     sql="select * from {tb} where date<2010.03.01".format(tb=tbName1))
        rs = total - drop.rows

        # tb1 dropPartition
        tmp = self.s.loadTable(tbName1, dbPath)
        self.assertEqual(tmp.rows, total)
        self.s.dropPartition(dbPath, ["2010.01.01", "2010.02.01"], tbName1)

        tmp = self.s.loadTable(tbName1, dbPath)
        self.assertEqual(tmp.rows, rs)

        # tb2 dropPartition
        tmp = self.s.loadTable(tbName2, dbPath)
        self.assertEqual(tmp.rows, total)
        self.s.dropPartition(dbPath, ["2010.01.01", "2010.02.01"], tbName2)
        tmp = self.s.loadTable(tbName2, dbPath)
        self.assertEqual(tmp.rows, rs)

        # both dropPartition
        create_dfs_compo_range_hash_db()
        tmp = self.s.loadTable(tbName1, dbPath)
        self.assertEqual(tmp.rows, total)
        tmp = self.s.loadTable(tbName2, dbPath)
        self.assertEqual(tmp.rows, total)
        self.s.dropPartition(dbPath, ["2010.01.01", "2010.02.01"])
        tmp = self.s.loadTable(tbName1, dbPath)
        self.assertEqual(tmp.rows, rs)
        tmp = self.s.loadTable(tbName2, dbPath)
        self.assertEqual(tmp.rows, rs)

    def test_dropPartition_dfs_compo2_range_hash_drop_level2_single(self):
        dbPath = DBInfo.dfsDBName
        tbName1 = DBInfo.table1
        tbName2 = DBInfo.table2
        create_dfs_compo_range_hash_db()

        origin = self.s.loadTable(tbName1, dbPath)
        total = origin.rows

        drop = self.s.loadTableBySQL(tableName=tbName1, dbPath=dbPath,
                                     sql="select * from {tb} where date<2010.02.01 and id=1".format(tb=tbName1))
        rs = total - drop.rows

        # tb1 dropPartition
        tmp = self.s.loadTable(tbName1, dbPath)
        self.assertEqual(tmp.rows, total)
        self.s.dropPartition(dbPath, ["2010.01.01", "1"], tbName1)
        tmp = self.s.loadTable(tbName1, dbPath)
        self.assertEqual(tmp.rows, rs)

        # tb2 dropPartition
        tmp = self.s.loadTable(tbName2, dbPath)
        self.assertEqual(tmp.rows, total)
        self.s.dropPartition(dbPath, ["2010.01.01", "1"], tbName2)
        tmp = self.s.loadTable(tbName2, dbPath)
        self.assertEqual(tmp.rows, rs)

        # both dropPartition
        create_dfs_compo_range_hash_db()
        tmp = self.s.loadTable(tbName1, dbPath)
        self.assertEqual(tmp.rows, total)
        tmp = self.s.loadTable(tbName2, dbPath)
        self.assertEqual(tmp.rows, total)
        self.s.dropPartition(dbPath, ["2010.01.01", "1"])
        tmp = self.s.loadTable(tbName1, dbPath)
        self.assertEqual(tmp.rows, rs)
        tmp = self.s.loadTable(tbName2, dbPath)
        self.assertEqual(tmp.rows, rs)

    def test_dropPartition_dfs_compo2_range_hash_drop_level2_multiple(self):
        dbPath = DBInfo.dfsDBName
        tbName1 = DBInfo.table1
        tbName2 = DBInfo.table2
        create_dfs_compo_range_hash_db()

        origin = self.s.loadTable(tbName1, dbPath)
        total = origin.rows

        drop = self.s.loadTableBySQL(tableName=tbName1, dbPath=dbPath,
                                     sql="select * from {tb} where date<2010.02.01 and id in [3,5]".format(tb=tbName1))
        rs = total - drop.rows

        # tb1 dropPartition
        tmp = self.s.loadTable(tbName1, dbPath)
        self.assertEqual(tmp.rows, total)
        self.s.dropPartition(dbPath, ["2010.01.01", "[3,5]"], tbName1)  # 1
        tmp = self.s.loadTable(tbName1, dbPath)
        self.assertEqual(tmp.rows, rs)

        # tb2 dropPartition
        tmp = self.s.loadTable(tbName2, dbPath)
        self.assertEqual(tmp.rows, total)
        self.s.dropPartition(dbPath, ["2010.01.01", "[3,5]"], tbName2)
        tmp = self.s.loadTable(tbName2, dbPath)
        self.assertEqual(tmp.rows, rs)

        # both dropPartition
        create_dfs_compo_range_hash_db()
        tmp = self.s.loadTable(tbName1, dbPath)
        self.assertEqual(tmp.rows, total)
        tmp = self.s.loadTable(tbName2, dbPath)
        self.assertEqual(tmp.rows, total)
        self.s.dropPartition(dbPath, ["2010.01.01", "[3,5]"])
        tmp = self.s.loadTable(tbName1, dbPath)
        self.assertEqual(tmp.rows, rs)
        tmp = self.s.loadTable(tbName2, dbPath)
        self.assertEqual(tmp.rows, rs)

    def test_dropPartition_dfs_compo2_range_hash_drop_multiple(self):
        dbPath = DBInfo.dfsDBName
        tbName1 = DBInfo.table1
        tbName2 = DBInfo.table2
        create_dfs_compo_range_hash_db()

        origin = self.s.loadTable(tbName1, dbPath)
        total = origin.rows

        drop = self.s.loadTableBySQL(tableName=tbName1, dbPath=dbPath,
                                     sql="select * from {tb} where date < 2010.03.01 and id in [3,5]".format(
                                         tb=tbName1))
        rs = total - drop.rows

        # tb1 dropPartition
        tmp = self.s.loadTable(tbName1, dbPath)
        self.assertEqual(tmp.rows, total)
        self.s.dropPartition(dbPath, ["[2010.01.01,2010.02.01]", "[3,5]"], tbName1)  # 1
        tmp = self.s.loadTable(tbName1, dbPath)
        self.assertEqual(tmp.rows, rs)

        # tb2 dropPartition
        tmp = self.s.loadTable(tbName2, dbPath)
        self.assertEqual(tmp.rows, total)
        self.s.dropPartition(dbPath, ["[2010.01.01,2010.02.01]", "[3,5]"], tbName2)
        tmp = self.s.loadTable(tbName2, dbPath)
        self.assertEqual(tmp.rows, rs)

        # both dropPartition
        create_dfs_compo_range_hash_db()
        tmp = self.s.loadTable(tbName1, dbPath)
        self.assertEqual(tmp.rows, total)
        tmp = self.s.loadTable(tbName2, dbPath)
        self.assertEqual(tmp.rows, total)
        self.s.dropPartition(dbPath, ["[2010.01.01,2010.02.01]", "[3,5]"])
        tmp = self.s.loadTable(tbName1, dbPath)
        self.assertEqual(tmp.rows, rs)
        tmp = self.s.loadTable(tbName2, dbPath)
        self.assertEqual(tmp.rows, rs)

    def test_dropPartition_dfs_compo2_range_value_drop_level1_single(self):
        dbPath = DBInfo.dfsDBName
        tbName1 = DBInfo.table1
        tbName2 = DBInfo.table2
        create_dfs_compo_range_value_db()

        origin = self.s.loadTable(tbName1, dbPath)
        total = origin.rows

        drop = self.s.loadTableBySQL(tableName=tbName1, dbPath=dbPath,
                                     sql="select * from {tb} where date<2010.02.01".format(tb=tbName1))
        rs = total - drop.rows

        # tb1 dropPartition
        tmp = self.s.loadTable(tbName1, dbPath)
        self.assertEqual(tmp.rows, total)
        self.s.dropPartition(dbPath, ["2010.01.01"], tbName1)

        tmp = self.s.loadTable(tbName1, dbPath)
        self.assertEqual(tmp.rows, rs)

        # tb2 dropPartition
        tmp = self.s.loadTable(tbName2, dbPath)
        self.assertEqual(tmp.rows, total)
        self.s.dropPartition(dbPath, ["2010.01.01"], tbName2)
        tmp = self.s.loadTable(tbName2, dbPath)
        self.assertEqual(tmp.rows, rs)

        # both dropPartition
        create_dfs_compo_range_value_db()
        tmp = self.s.loadTable(tbName1, dbPath)
        self.assertEqual(tmp.rows, total)
        tmp = self.s.loadTable(tbName2, dbPath)
        self.assertEqual(tmp.rows, total)
        self.s.dropPartition(dbPath, ["2010.01.01"])
        tmp = self.s.loadTable(tbName1, dbPath)
        self.assertEqual(tmp.rows, rs)
        tmp = self.s.loadTable(tbName2, dbPath)
        self.assertEqual(tmp.rows, rs)

    def test_dropPartition_dfs_compo2_range_value_drop_level1_multiple(self):
        dbPath = DBInfo.dfsDBName
        tbName1 = DBInfo.table1
        tbName2 = DBInfo.table2
        create_dfs_compo_range_value_db()

        origin = self.s.loadTable(tbName1, dbPath)
        total = origin.rows

        drop = self.s.loadTableBySQL(tableName=tbName1, dbPath=dbPath,
                                     sql="select * from {tb} where date<2010.03.01".format(tb=tbName1))
        rs = total - drop.rows

        # tb1 dropPartition
        tmp = self.s.loadTable(tbName1, dbPath)
        self.assertEqual(tmp.rows, total)
        self.s.dropPartition(dbPath, ["2010.01.01", "2010.02.01"], tbName1)

        tmp = self.s.loadTable(tbName1, dbPath)
        self.assertEqual(tmp.rows, rs)

        # tb2 dropPartition
        tmp = self.s.loadTable(tbName2, dbPath)
        self.assertEqual(tmp.rows, total)
        self.s.dropPartition(dbPath, ["2010.01.01", "2010.02.01"], tbName2)
        tmp = self.s.loadTable(tbName2, dbPath)
        self.assertEqual(tmp.rows, rs)

        # both dropPartition
        create_dfs_compo_range_value_db()
        tmp = self.s.loadTable(tbName1, dbPath)
        self.assertEqual(tmp.rows, total)
        tmp = self.s.loadTable(tbName2, dbPath)
        self.assertEqual(tmp.rows, total)
        self.s.dropPartition(dbPath, ["2010.01.01", "2010.02.01"])
        tmp = self.s.loadTable(tbName1, dbPath)
        self.assertEqual(tmp.rows, rs)
        tmp = self.s.loadTable(tbName2, dbPath)
        self.assertEqual(tmp.rows, rs)

    def test_dropPartition_dfs_compo2_range_value_drop_level2_single(self):
        dbPath = DBInfo.dfsDBName
        tbName1 = DBInfo.table1
        tbName2 = DBInfo.table2
        create_dfs_compo_range_value_db()

        origin = self.s.loadTable(tbName1, dbPath)
        total = origin.rows

        drop = self.s.loadTableBySQL(tableName=tbName1, dbPath=dbPath,
                                     sql="select * from {tb} where date<2010.02.01 and id=1".format(tb=tbName1))
        rs = total - drop.rows

        # tb1 dropPartition
        tmp = self.s.loadTable(tbName1, dbPath)
        self.assertEqual(tmp.rows, total)
        self.s.dropPartition(dbPath, ["2010.01.01", "1"], tbName1)
        tmp = self.s.loadTable(tbName1, dbPath)
        self.assertEqual(tmp.rows, rs)

        # tb2 dropPartition
        tmp = self.s.loadTable(tbName2, dbPath)
        self.assertEqual(tmp.rows, total)
        self.s.dropPartition(dbPath, ["2010.01.01", "1"], tbName2)
        tmp = self.s.loadTable(tbName2, dbPath)
        self.assertEqual(tmp.rows, rs)

        # both dropPartition
        create_dfs_compo_range_value_db()
        tmp = self.s.loadTable(tbName1, dbPath)
        self.assertEqual(tmp.rows, total)
        tmp = self.s.loadTable(tbName2, dbPath)
        self.assertEqual(tmp.rows, total)
        self.s.dropPartition(dbPath, ["2010.01.01", "1"])
        tmp = self.s.loadTable(tbName1, dbPath)
        self.assertEqual(tmp.rows, rs)
        tmp = self.s.loadTable(tbName2, dbPath)
        self.assertEqual(tmp.rows, rs)

    def test_dropPartition_dfs_compo2_range_value_drop_level2_multiple(self):
        dbPath = DBInfo.dfsDBName
        tbName1 = DBInfo.table1
        tbName2 = DBInfo.table2
        create_dfs_compo_range_value_db()

        origin = self.s.loadTable(tbName1, dbPath)
        total = origin.rows

        drop = self.s.loadTableBySQL(tableName=tbName1, dbPath=dbPath,
                                     sql="select * from {tb} where date<2010.02.01 and id between 2:4".format(tb=tbName1))
        rs = total - drop.rows

        # tb1 dropPartition
        tmp = self.s.loadTable(tbName1, dbPath)
        self.assertEqual(tmp.rows, total)
        self.s.dropPartition(dbPath, ["2010.01.01", "[2,3,4]"], tbName1)  # 1
        tmp = self.s.loadTable(tbName1, dbPath)
        self.assertEqual(tmp.rows, rs)

        # tb2 dropPartition
        tmp = self.s.loadTable(tbName2, dbPath)
        self.assertEqual(tmp.rows, total)
        self.s.dropPartition(dbPath, ["2010.01.01", "[2,3,4]"], tbName2)
        tmp = self.s.loadTable(tbName2, dbPath)
        self.assertEqual(tmp.rows, rs)

        # both dropPartition
        create_dfs_compo_range_value_db()
        tmp = self.s.loadTable(tbName1, dbPath)
        self.assertEqual(tmp.rows, total)
        tmp = self.s.loadTable(tbName2, dbPath)
        self.assertEqual(tmp.rows, total)
        self.s.dropPartition(dbPath, ["2010.01.01", "[2,3,4]"])
        tmp = self.s.loadTable(tbName1, dbPath)
        self.assertEqual(tmp.rows, rs)
        tmp = self.s.loadTable(tbName2, dbPath)
        self.assertEqual(tmp.rows, rs)

    def test_dropPartition_dfs_compo2_range_value_drop_multiple(self):
        dbPath = DBInfo.dfsDBName
        tbName1 = DBInfo.table1
        tbName2 = DBInfo.table2
        create_dfs_compo_range_value_db()

        origin = self.s.loadTable(tbName1, dbPath)
        total = origin.rows

        drop = self.s.loadTableBySQL(tableName=tbName1, dbPath=dbPath,
                                     sql="select * from {tb} where date < 2010.03.01 and id between 2:4".format(
                                         tb=tbName1))
        rs = total - drop.rows

        # tb1 dropPartition
        tmp = self.s.loadTable(tbName1, dbPath)
        self.assertEqual(tmp.rows, total)
        self.s.dropPartition(dbPath, ["[2010.01.01,2010.02.01]", "[2,3,4]"], tbName1)  # 1
        tmp = self.s.loadTable(tbName1, dbPath)
        self.assertEqual(tmp.rows, rs)

        # tb2 dropPartition
        tmp = self.s.loadTable(tbName2, dbPath)
        self.assertEqual(tmp.rows, total)
        self.s.dropPartition(dbPath, ["[2010.01.01,2010.02.01]", "[2,3,4]"], tbName2)
        tmp = self.s.loadTable(tbName2, dbPath)
        self.assertEqual(tmp.rows, rs)

        # both dropPartition
        create_dfs_compo_range_value_db()
        tmp = self.s.loadTable(tbName1, dbPath)
        self.assertEqual(tmp.rows, total)
        tmp = self.s.loadTable(tbName2, dbPath)
        self.assertEqual(tmp.rows, total)
        self.s.dropPartition(dbPath, ["[2010.01.01,2010.02.01]", "[2,3,4]"])
        tmp = self.s.loadTable(tbName1, dbPath)
        self.assertEqual(tmp.rows, rs)
        tmp = self.s.loadTable(tbName2, dbPath)
        self.assertEqual(tmp.rows, rs)

    def test_dropPartition_dfs_compo2_range_list_drop_level1_single(self):
        dbPath = DBInfo.dfsDBName
        tbName1 = DBInfo.table1
        tbName2 = DBInfo.table2
        create_dfs_compo_range_list_db()

        origin = self.s.loadTable(tbName1, dbPath)
        total = origin.rows

        drop = self.s.loadTableBySQL(tableName=tbName1, dbPath=dbPath,
                                     sql="select * from {tb} where date<2010.02.01".format(tb=tbName1))
        rs = total - drop.rows

        # tb1 dropPartition
        tmp = self.s.loadTable(tbName1, dbPath)
        self.assertEqual(tmp.rows, total)
        self.s.dropPartition(dbPath, ["2010.01.01"], tbName1)

        tmp = self.s.loadTable(tbName1, dbPath)
        self.assertEqual(tmp.rows, rs)

        # tb2 dropPartition
        tmp = self.s.loadTable(tbName2, dbPath)
        self.assertEqual(tmp.rows, total)
        self.s.dropPartition(dbPath, ["2010.01.01"], tbName2)
        tmp = self.s.loadTable(tbName2, dbPath)
        self.assertEqual(tmp.rows, rs)

        # both dropPartition
        create_dfs_compo_range_list_db()
        tmp = self.s.loadTable(tbName1, dbPath)
        self.assertEqual(tmp.rows, total)
        tmp = self.s.loadTable(tbName2, dbPath)
        self.assertEqual(tmp.rows, total)
        self.s.dropPartition(dbPath, ["2010.01.01"])
        tmp = self.s.loadTable(tbName1, dbPath)
        self.assertEqual(tmp.rows, rs)
        tmp = self.s.loadTable(tbName2, dbPath)
        self.assertEqual(tmp.rows, rs)

    def test_dropPartition_dfs_compo2_range_list_drop_level1_multiple(self):
        dbPath = DBInfo.dfsDBName
        tbName1 = DBInfo.table1
        tbName2 = DBInfo.table2
        create_dfs_compo_range_list_db()

        origin = self.s.loadTable(tbName1, dbPath)
        total = origin.rows

        drop = self.s.loadTableBySQL(tableName=tbName1, dbPath=dbPath,
                                     sql="select * from {tb} where date<2010.03.01".format(tb=tbName1))
        rs = total - drop.rows

        # tb1 dropPartition
        tmp = self.s.loadTable(tbName1, dbPath)
        self.assertEqual(tmp.rows, total)
        self.s.dropPartition(dbPath, ["2010.01.01", "2010.02.01"], tbName1)

        tmp = self.s.loadTable(tbName1, dbPath)
        self.assertEqual(tmp.rows, rs)

        # tb2 dropPartition
        tmp = self.s.loadTable(tbName2, dbPath)
        self.assertEqual(tmp.rows, total)
        self.s.dropPartition(dbPath, ["2010.01.01", "2010.02.01"], tbName2)
        tmp = self.s.loadTable(tbName2, dbPath)
        self.assertEqual(tmp.rows, rs)

        # both dropPartition
        create_dfs_compo_range_list_db()
        tmp = self.s.loadTable(tbName1, dbPath)
        self.assertEqual(tmp.rows, total)
        tmp = self.s.loadTable(tbName2, dbPath)
        self.assertEqual(tmp.rows, total)
        self.s.dropPartition(dbPath, ["2010.01.01", "2010.02.01"])
        tmp = self.s.loadTable(tbName1, dbPath)
        self.assertEqual(tmp.rows, rs)
        tmp = self.s.loadTable(tbName2, dbPath)
        self.assertEqual(tmp.rows, rs)

    def test_dropPartition_dfs_compo2_range_list_drop_level2_single(self):
        dbPath = DBInfo.dfsDBName
        tbName1 = DBInfo.table1
        tbName2 = DBInfo.table2
        create_dfs_compo_range_list_db()

        origin = self.s.loadTable(tbName1, dbPath)
        total = origin.rows
        drop = self.s.loadTableBySQL(tableName=tbName1, dbPath=dbPath,
                                     sql="select * from {tb} where date<2010.02.01 and sym in `AMD`QWE`CES".format(tb=tbName1))
        rs = total - drop.rows

        # tb1 dropPartition
        tmp = self.s.loadTable(tbName1, dbPath)
        self.assertEqual(tmp.rows, total)
        self.s.dropPartition(dbPath, ["2010.01.01", "'AMD'"], tbName1)
        tmp = self.s.loadTable(tbName1, dbPath)
        self.assertEqual(tmp.rows, rs)

        # tb2 dropPartition
        tmp = self.s.loadTable(tbName2, dbPath)
        self.assertEqual(tmp.rows, total)
        self.s.dropPartition(dbPath, ["2010.01.01", "'AMD'"], tbName2)
        tmp = self.s.loadTable(tbName2, dbPath)
        self.assertEqual(tmp.rows, rs)

        # both dropPartition
        create_dfs_compo_range_list_db()
        tmp = self.s.loadTable(tbName1, dbPath)
        self.assertEqual(tmp.rows, total)
        tmp = self.s.loadTable(tbName2, dbPath)
        self.assertEqual(tmp.rows, total)
        self.s.dropPartition(dbPath, ["2010.01.01", "'AMD'"])
        tmp = self.s.loadTable(tbName1, dbPath)
        self.assertEqual(tmp.rows, rs)
        tmp = self.s.loadTable(tbName2, dbPath)
        self.assertEqual(tmp.rows, rs)

    def test_dropPartition_dfs_compo2_range_list_drop_level2_multiple(self):
        dbPath = DBInfo.dfsDBName
        tbName1 = DBInfo.table1
        tbName2 = DBInfo.table2
        create_dfs_compo_range_list_db()

        origin = self.s.loadTable(tbName1, dbPath)
        total = origin.rows

        drop = self.s.loadTableBySQL(tableName=tbName1, dbPath=dbPath,
                                     sql="select * from {tb} where date<2010.02.01 and sym in `DOP`ASZ`FSD`BBVC".format(tb=tbName1))
        rs = total - drop.rows

        # tb1 dropPartition
        tmp = self.s.loadTable(tbName1, dbPath)
        self.assertEqual(tmp.rows, total)
        self.s.dropPartition(dbPath, ["2010.01.01", "`DOP`FSD"], tbName1)  # 1
        tmp = self.s.loadTable(tbName1, dbPath)
        self.assertEqual(tmp.rows, rs)

        # tb2 dropPartition
        tmp = self.s.loadTable(tbName2, dbPath)
        self.assertEqual(tmp.rows, total)
        self.s.dropPartition(dbPath, ["2010.01.01", "['DOP','FSD']"], tbName2)
        tmp = self.s.loadTable(tbName2, dbPath)
        self.assertEqual(tmp.rows, rs)

        # both dropPartition
        create_dfs_compo_range_list_db()
        tmp = self.s.loadTable(tbName1, dbPath)
        self.assertEqual(tmp.rows, total)
        tmp = self.s.loadTable(tbName2, dbPath)
        self.assertEqual(tmp.rows, total)
        self.s.dropPartition(dbPath, ["2010.01.01", "`DOP`ASZ`FSD`BBVC"])
        tmp = self.s.loadTable(tbName1, dbPath)
        self.assertEqual(tmp.rows, rs)
        tmp = self.s.loadTable(tbName2, dbPath)
        self.assertEqual(tmp.rows, rs)

    def test_dropPartition_dfs_compo2_range_list_drop_multiple(self):
        dbPath = DBInfo.dfsDBName
        tbName1 = DBInfo.table1
        tbName2 = DBInfo.table2
        create_dfs_compo_range_list_db()

        origin = self.s.loadTable(tbName1, dbPath)
        total = origin.rows

        drop = self.s.loadTableBySQL(tableName=tbName1, dbPath=dbPath,
                                     sql="select * from {tb} where date < 2010.03.01 and sym in `DOP`ASZ`FSD`BBVC".format(
                                         tb=tbName1))
        rs = total - drop.rows

        # tb1 dropPartition
        tmp = self.s.loadTable(tbName1, dbPath)
        self.assertEqual(tmp.rows, total)
        self.s.dropPartition(dbPath, ["[2010.01.01,2010.02.01]", "['DOP','FSD']"], tbName1)  # 1
        tmp = self.s.loadTable(tbName1, dbPath)
        self.assertEqual(tmp.rows, rs)

        # tb2 dropPartition
        tmp = self.s.loadTable(tbName2, dbPath)
        self.assertEqual(tmp.rows, total)
        self.s.dropPartition(dbPath, ["[2010.01.01,2010.02.01]", "['DOP','FSD']"], tbName2)
        tmp = self.s.loadTable(tbName2, dbPath)
        self.assertEqual(tmp.rows, rs)

        # both dropPartition
        create_dfs_compo_range_list_db()
        tmp = self.s.loadTable(tbName1, dbPath)
        self.assertEqual(tmp.rows, total)
        tmp = self.s.loadTable(tbName2, dbPath)
        self.assertEqual(tmp.rows, total)
        self.s.dropPartition(dbPath, ["[2010.01.01,2010.02.01]", "['DOP','FSD']"])
        tmp = self.s.loadTable(tbName1, dbPath)
        self.assertEqual(tmp.rows, rs)
        tmp = self.s.loadTable(tbName2, dbPath)
        self.assertEqual(tmp.rows, rs)

    def test_dropPartition_dfs_compo3_range_hash_list_drop_level1_single(self):
        dbPath = DBInfo.dfsDBName
        tbName1 = DBInfo.table1
        tbName2 = DBInfo.table2
        create_dfs_compo_range_hash_list_db()

        origin = self.s.loadTable(tbName1, dbPath)
        total = origin.rows

        drop = self.s.loadTableBySQL(tableName=tbName1, dbPath=dbPath,
                                     sql="select * from {tb} where date<2010.02.01".format(tb=tbName1))
        rs = total - drop.rows

        # tb1 dropPartition
        tmp = self.s.loadTable(tbName1, dbPath)
        self.assertEqual(tmp.rows, total)
        self.s.dropPartition(dbPath, ["2010.01.01"], tbName1)

        tmp = self.s.loadTable(tbName1, dbPath)
        self.assertEqual(tmp.rows, rs)

        # tb2 dropPartition
        tmp = self.s.loadTable(tbName2, dbPath)
        self.assertEqual(tmp.rows, total)
        self.s.dropPartition(dbPath, ["2010.01.01"], tbName2)
        tmp = self.s.loadTable(tbName2, dbPath)
        self.assertEqual(tmp.rows, rs)

        # both dropPartition
        create_dfs_compo_range_hash_list_db()
        tmp = self.s.loadTable(tbName1, dbPath)
        self.assertEqual(tmp.rows, total)
        tmp = self.s.loadTable(tbName2, dbPath)
        self.assertEqual(tmp.rows, total)
        self.s.dropPartition(dbPath, ["2010.01.01"])
        tmp = self.s.loadTable(tbName1, dbPath)
        self.assertEqual(tmp.rows, rs)
        tmp = self.s.loadTable(tbName2, dbPath)
        self.assertEqual(tmp.rows, rs)

    def test_dropPartition_dfs_compo3_range_hash_list_drop_level1_multiple(self):
        dbPath = DBInfo.dfsDBName
        tbName1 = DBInfo.table1
        tbName2 = DBInfo.table2
        create_dfs_compo_range_hash_list_db()

        origin = self.s.loadTable(tbName1, dbPath)
        total = origin.rows

        drop = self.s.loadTableBySQL(tableName=tbName1, dbPath=dbPath,
                                     sql="select * from {tb} where date<2010.03.01".format(tb=tbName1))
        rs = total - drop.rows

        # tb1 dropPartition
        tmp = self.s.loadTable(tbName1, dbPath)
        self.assertEqual(tmp.rows, total)
        self.s.dropPartition(dbPath, ["2010.01.01", "2010.02.01"], tbName1)

        tmp = self.s.loadTable(tbName1, dbPath)
        self.assertEqual(tmp.rows, rs)

        # tb2 dropPartition
        tmp = self.s.loadTable(tbName2, dbPath)
        self.assertEqual(tmp.rows, total)
        self.s.dropPartition(dbPath, ["2010.01.01", "2010.02.01"], tbName2)
        tmp = self.s.loadTable(tbName2, dbPath)
        self.assertEqual(tmp.rows, rs)

        # both dropPartition
        create_dfs_compo_range_hash_list_db()
        tmp = self.s.loadTable(tbName1, dbPath)
        self.assertEqual(tmp.rows, total)
        tmp = self.s.loadTable(tbName2, dbPath)
        self.assertEqual(tmp.rows, total)
        self.s.dropPartition(dbPath, ["2010.01.01", "2010.02.01"])
        tmp = self.s.loadTable(tbName1, dbPath)
        self.assertEqual(tmp.rows, rs)
        tmp = self.s.loadTable(tbName2, dbPath)
        self.assertEqual(tmp.rows, rs)

    def test_dropPartition_dfs_compo3_range_hash_list_drop_level2_single(self):
        dbPath = DBInfo.dfsDBName
        tbName1 = DBInfo.table1
        tbName2 = DBInfo.table2
        create_dfs_compo_range_hash_list_db()

        origin = self.s.loadTable(tbName1, dbPath)
        total = origin.rows
        drop = self.s.loadTableBySQL(tableName=tbName1, dbPath=dbPath,
                                     sql="select * from {tb} where date<2010.02.01 and id=1".format(tb=tbName1))
        rs = total - drop.rows

        # tb1 dropPartition
        tmp = self.s.loadTable(tbName1, dbPath)
        self.assertEqual(tmp.rows, total)
        self.s.dropPartition(dbPath, ["2010.01.01", "1"], tbName1)
        tmp = self.s.loadTable(tbName1, dbPath)
        self.assertEqual(tmp.rows, rs)

        # tb2 dropPartition
        tmp = self.s.loadTable(tbName2, dbPath)
        self.assertEqual(tmp.rows, total)
        self.s.dropPartition(dbPath, ["2010.01.01", "1"], tbName2)
        tmp = self.s.loadTable(tbName2, dbPath)
        self.assertEqual(tmp.rows, rs)

        # both dropPartition
        create_dfs_compo_range_hash_list_db()
        tmp = self.s.loadTable(tbName1, dbPath)
        self.assertEqual(tmp.rows, total)
        tmp = self.s.loadTable(tbName2, dbPath)
        self.assertEqual(tmp.rows, total)
        self.s.dropPartition(dbPath, ["2010.01.01", "1"])
        tmp = self.s.loadTable(tbName1, dbPath)
        self.assertEqual(tmp.rows, rs)
        tmp = self.s.loadTable(tbName2, dbPath)
        self.assertEqual(tmp.rows, rs)

    def test_dropPartition_dfs_compo3_range_hash_list_drop_level2_multiple(self):
        dbPath = DBInfo.dfsDBName
        tbName1 = DBInfo.table1
        tbName2 = DBInfo.table2
        create_dfs_compo_range_hash_list_db()

        origin = self.s.loadTable(tbName1, dbPath)
        total = origin.rows

        drop = self.s.loadTableBySQL(tableName=tbName1, dbPath=dbPath,
                                     sql="select * from {tb} where date<2010.02.01 and id in 1..3".format(tb=tbName1))
        rs = total - drop.rows

        # tb1 dropPartition
        tmp = self.s.loadTable(tbName1, dbPath)
        self.assertEqual(tmp.rows, total)
        self.s.dropPartition(dbPath, ["2010.01.01", "1..3"], tbName1)  # 1
        tmp = self.s.loadTable(tbName1, dbPath)
        self.assertEqual(tmp.rows, rs)

        # tb2 dropPartition
        tmp = self.s.loadTable(tbName2, dbPath)
        self.assertEqual(tmp.rows, total)
        self.s.dropPartition(dbPath, ["2010.01.01", "1..3"], tbName2)
        tmp = self.s.loadTable(tbName2, dbPath)
        self.assertEqual(tmp.rows, rs)

        # both dropPartition
        create_dfs_compo_range_hash_list_db()
        tmp = self.s.loadTable(tbName1, dbPath)
        self.assertEqual(tmp.rows, total)
        tmp = self.s.loadTable(tbName2, dbPath)
        self.assertEqual(tmp.rows, total)
        self.s.dropPartition(dbPath, ["2010.01.01", "1..3"])
        tmp = self.s.loadTable(tbName1, dbPath)
        self.assertEqual(tmp.rows, rs)
        tmp = self.s.loadTable(tbName2, dbPath)
        self.assertEqual(tmp.rows, rs)

    def test_dropPartition_dfs_compo3_range_hash_list_drop_level3_single(self):
        dbPath = DBInfo.dfsDBName
        tbName1 = DBInfo.table1
        tbName2 = DBInfo.table2
        create_dfs_compo_range_hash_list_db()

        origin = self.s.loadTable(tbName1, dbPath)
        total = origin.rows
        drop = self.s.loadTableBySQL(tableName=tbName1, dbPath=dbPath,
                                     sql="select * from {tb} where date<2010.02.01 and id=1 and sym in `AMD`QWE`CES".format(tb=tbName1))
        rs = total - drop.rows

        # tb1 dropPartition
        tmp = self.s.loadTable(tbName1, dbPath)
        self.assertEqual(tmp.rows, total)
        self.s.dropPartition(dbPath, ["2010.01.01", "1", "'AMD'"], tbName1)
        tmp = self.s.loadTable(tbName1, dbPath)
        self.assertEqual(tmp.rows, rs)

        # tb2 dropPartition
        tmp = self.s.loadTable(tbName2, dbPath)
        self.assertEqual(tmp.rows, total)
        self.s.dropPartition(dbPath, ["2010.01.01", "1", "'AMD'"], tbName2)
        tmp = self.s.loadTable(tbName2, dbPath)
        self.assertEqual(tmp.rows, rs)

        # both dropPartition
        create_dfs_compo_range_hash_list_db()
        tmp = self.s.loadTable(tbName1, dbPath)
        self.assertEqual(tmp.rows, total)
        tmp = self.s.loadTable(tbName2, dbPath)
        self.assertEqual(tmp.rows, total)
        self.s.dropPartition(dbPath, ["2010.01.01", "1", "'AMD'"])
        tmp = self.s.loadTable(tbName1, dbPath)
        self.assertEqual(tmp.rows, rs)
        tmp = self.s.loadTable(tbName2, dbPath)
        self.assertEqual(tmp.rows, rs)

    def test_dropPartition_dfs_compo3_range_hash_list_drop_level3_multiple(self):
        dbPath = DBInfo.dfsDBName
        tbName1 = DBInfo.table1
        tbName2 = DBInfo.table2
        create_dfs_compo_range_hash_list_db()

        origin = self.s.loadTable(tbName1, dbPath)
        total = origin.rows

        drop = self.s.loadTableBySQL(tableName=tbName1, dbPath=dbPath,
                                     sql="select * from {tb} where date<2010.02.01 and id=1 and sym in `DOP`ASZ`FSD`BBVC".format(tb=tbName1))
        rs = total - drop.rows

        # tb1 dropPartition
        tmp = self.s.loadTable(tbName1, dbPath)
        self.assertEqual(tmp.rows, total)
        self.s.dropPartition(dbPath, ["2010.01.01", "1", "`DOP`FSD"], tbName1)  # 1
        tmp = self.s.loadTable(tbName1, dbPath)
        self.assertEqual(tmp.rows, rs)

        # tb2 dropPartition
        tmp = self.s.loadTable(tbName2, dbPath)
        self.assertEqual(tmp.rows, total)
        self.s.dropPartition(dbPath, ["2010.01.01", "1", "['DOP','FSD']"], tbName2)
        tmp = self.s.loadTable(tbName2, dbPath)
        self.assertEqual(tmp.rows, rs)

        # both dropPartition
        create_dfs_compo_range_hash_list_db()
        tmp = self.s.loadTable(tbName1, dbPath)
        self.assertEqual(tmp.rows, total)
        tmp = self.s.loadTable(tbName2, dbPath)
        self.assertEqual(tmp.rows, total)
        self.s.dropPartition(dbPath, ["2010.01.01", "1", "`DOP`ASZ`FSD`BBVC"])
        tmp = self.s.loadTable(tbName1, dbPath)
        self.assertEqual(tmp.rows, rs)
        tmp = self.s.loadTable(tbName2, dbPath)
        self.assertEqual(tmp.rows, rs)

    def test_dropPartition_dfs_compo3_range_hash_list_drop_multiple(self):
        dbPath = DBInfo.dfsDBName
        tbName1 = DBInfo.table1
        tbName2 = DBInfo.table2
        create_dfs_compo_range_hash_list_db()

        origin = self.s.loadTable(tbName1, dbPath)
        total = origin.rows

        drop = self.s.loadTableBySQL(tableName=tbName1, dbPath=dbPath,
                                     sql="select * from {tb} where date < 2010.03.01 and id in 1..3 and sym in `DOP`ASZ`FSD`BBVC".format(
                                         tb=tbName1))
        rs = total - drop.rows

        # tb1 dropPartition
        tmp = self.s.loadTable(tbName1, dbPath)
        self.assertEqual(tmp.rows, total)
        self.s.dropPartition(dbPath, ["[2010.01.01,2010.02.01]", "1..3", "['DOP','FSD']"], tbName1)  # 1
        tmp = self.s.loadTable(tbName1, dbPath)
        self.assertEqual(tmp.rows, rs)

        # tb2 dropPartition
        tmp = self.s.loadTable(tbName2, dbPath)
        self.assertEqual(tmp.rows, total)
        self.s.dropPartition(dbPath, ["[2010.01.01,2010.02.01]", "1..3", "['DOP','FSD']"], tbName2)
        tmp = self.s.loadTable(tbName2, dbPath)
        self.assertEqual(tmp.rows, rs)

        # both dropPartition
        create_dfs_compo_range_hash_list_db()
        tmp = self.s.loadTable(tbName1, dbPath)
        self.assertEqual(tmp.rows, total)
        tmp = self.s.loadTable(tbName2, dbPath)
        self.assertEqual(tmp.rows, total)
        self.s.dropPartition(dbPath, ["[2010.01.01,2010.02.01]", "1..3", "['DOP','FSD']"])
        tmp = self.s.loadTable(tbName1, dbPath)
        self.assertEqual(tmp.rows, rs)
        tmp = self.s.loadTable(tbName2, dbPath)
        self.assertEqual(tmp.rows, rs)

    def test_dropPartition_dfs_compo3_range_value_list_drop_level1_single(self):
        dbPath = DBInfo.dfsDBName
        tbName1 = DBInfo.table1
        tbName2 = DBInfo.table2
        create_dfs_compo_range_value_list_db()

        origin = self.s.loadTable(tbName1, dbPath)
        total = origin.rows

        drop = self.s.loadTableBySQL(tableName=tbName1, dbPath=dbPath,
                                     sql="select * from {tb} where date<2010.02.01".format(tb=tbName1))
        rs = total - drop.rows

        # tb1 dropPartition
        tmp = self.s.loadTable(tbName1, dbPath)
        self.assertEqual(tmp.rows, total)
        self.s.dropPartition(dbPath, ["2010.01.01"], tbName1)

        tmp = self.s.loadTable(tbName1, dbPath)
        self.assertEqual(tmp.rows, rs)

        # tb2 dropPartition
        tmp = self.s.loadTable(tbName2, dbPath)
        self.assertEqual(tmp.rows, total)
        self.s.dropPartition(dbPath, ["2010.01.01"], tbName2)
        tmp = self.s.loadTable(tbName2, dbPath)
        self.assertEqual(tmp.rows, rs)

        # both dropPartition
        create_dfs_compo_range_value_list_db()
        tmp = self.s.loadTable(tbName1, dbPath)
        self.assertEqual(tmp.rows, total)
        tmp = self.s.loadTable(tbName2, dbPath)
        self.assertEqual(tmp.rows, total)
        self.s.dropPartition(dbPath, ["2010.01.01"])
        tmp = self.s.loadTable(tbName1, dbPath)
        self.assertEqual(tmp.rows, rs)
        tmp = self.s.loadTable(tbName2, dbPath)
        self.assertEqual(tmp.rows, rs)

    def test_dropPartition_dfs_compo3_range_value_list_drop_level1_multiple(self):
        dbPath = DBInfo.dfsDBName
        tbName1 = DBInfo.table1
        tbName2 = DBInfo.table2
        create_dfs_compo_range_value_list_db()

        origin = self.s.loadTable(tbName1, dbPath)
        total = origin.rows

        drop = self.s.loadTableBySQL(tableName=tbName1, dbPath=dbPath,
                                     sql="select * from {tb} where date<2010.03.01".format(tb=tbName1))
        rs = total - drop.rows

        # tb1 dropPartition
        tmp = self.s.loadTable(tbName1, dbPath)
        self.assertEqual(tmp.rows, total)
        self.s.dropPartition(dbPath, ["2010.01.01", "2010.02.01"], tbName1)

        tmp = self.s.loadTable(tbName1, dbPath)
        self.assertEqual(tmp.rows, rs)

        # tb2 dropPartition
        tmp = self.s.loadTable(tbName2, dbPath)
        self.assertEqual(tmp.rows, total)
        self.s.dropPartition(dbPath, ["2010.01.01", "2010.02.01"], tbName2)
        tmp = self.s.loadTable(tbName2, dbPath)
        self.assertEqual(tmp.rows, rs)

        # both dropPartition
        create_dfs_compo_range_value_list_db()
        tmp = self.s.loadTable(tbName1, dbPath)
        self.assertEqual(tmp.rows, total)
        tmp = self.s.loadTable(tbName2, dbPath)
        self.assertEqual(tmp.rows, total)
        self.s.dropPartition(dbPath, ["2010.01.01", "2010.02.01"])
        tmp = self.s.loadTable(tbName1, dbPath)
        self.assertEqual(tmp.rows, rs)
        tmp = self.s.loadTable(tbName2, dbPath)
        self.assertEqual(tmp.rows, rs)

    def test_dropPartition_dfs_compo3_range_value_list_drop_level2_single(self):
        dbPath = DBInfo.dfsDBName
        tbName1 = DBInfo.table1
        tbName2 = DBInfo.table2
        create_dfs_compo_range_value_list_db()

        origin = self.s.loadTable(tbName1, dbPath)
        total = origin.rows
        drop = self.s.loadTableBySQL(tableName=tbName1, dbPath=dbPath,
                                     sql="select * from {tb} where date<2010.02.01 and id=1".format(tb=tbName1))
        rs = total - drop.rows

        # tb1 dropPartition
        tmp = self.s.loadTable(tbName1, dbPath)
        self.assertEqual(tmp.rows, total)
        self.s.dropPartition(dbPath, ["2010.01.01", "1"], tbName1)
        tmp = self.s.loadTable(tbName1, dbPath)
        self.assertEqual(tmp.rows, rs)

        # tb2 dropPartition
        tmp = self.s.loadTable(tbName2, dbPath)
        self.assertEqual(tmp.rows, total)
        self.s.dropPartition(dbPath, ["2010.01.01", "1"], tbName2)
        tmp = self.s.loadTable(tbName2, dbPath)
        self.assertEqual(tmp.rows, rs)

        # both dropPartition
        create_dfs_compo_range_value_list_db()
        tmp = self.s.loadTable(tbName1, dbPath)
        self.assertEqual(tmp.rows, total)
        tmp = self.s.loadTable(tbName2, dbPath)
        self.assertEqual(tmp.rows, total)
        self.s.dropPartition(dbPath, ["2010.01.01", "1"])
        tmp = self.s.loadTable(tbName1, dbPath)
        self.assertEqual(tmp.rows, rs)
        tmp = self.s.loadTable(tbName2, dbPath)
        self.assertEqual(tmp.rows, rs)

    def test_dropPartition_dfs_compo3_range_value_list_drop_level2_multiple(self):
        dbPath = DBInfo.dfsDBName
        tbName1 = DBInfo.table1
        tbName2 = DBInfo.table2
        create_dfs_compo_range_value_list_db()

        origin = self.s.loadTable(tbName1, dbPath)
        total = origin.rows

        drop = self.s.loadTableBySQL(tableName=tbName1, dbPath=dbPath,
                                     sql="select * from {tb} where date<2010.02.01 and id in 1..3".format(tb=tbName1))
        rs = total - drop.rows

        # tb1 dropPartition
        tmp = self.s.loadTable(tbName1, dbPath)
        self.assertEqual(tmp.rows, total)
        self.s.dropPartition(dbPath, ["2010.01.01", "1..3"], tbName1)  # 1
        tmp = self.s.loadTable(tbName1, dbPath)
        self.assertEqual(tmp.rows, rs)

        # tb2 dropPartition
        tmp = self.s.loadTable(tbName2, dbPath)
        self.assertEqual(tmp.rows, total)
        self.s.dropPartition(dbPath, ["2010.01.01", "1..3"], tbName2)
        tmp = self.s.loadTable(tbName2, dbPath)
        self.assertEqual(tmp.rows, rs)

        # both dropPartition
        create_dfs_compo_range_value_list_db()
        tmp = self.s.loadTable(tbName1, dbPath)
        self.assertEqual(tmp.rows, total)
        tmp = self.s.loadTable(tbName2, dbPath)
        self.assertEqual(tmp.rows, total)
        self.s.dropPartition(dbPath, ["2010.01.01", "1..3"])
        tmp = self.s.loadTable(tbName1, dbPath)
        self.assertEqual(tmp.rows, rs)
        tmp = self.s.loadTable(tbName2, dbPath)
        self.assertEqual(tmp.rows, rs)

    def test_dropPartition_dfs_compo3_range_value_list_drop_level3_single(self):
        dbPath = DBInfo.dfsDBName
        tbName1 = DBInfo.table1
        tbName2 = DBInfo.table2
        create_dfs_compo_range_value_list_db()

        origin = self.s.loadTable(tbName1, dbPath)
        total = origin.rows
        drop = self.s.loadTableBySQL(tableName=tbName1, dbPath=dbPath,
                                     sql="select * from {tb} where date<2010.02.01 and id=1 and sym in `AMD`QWE`CES".format(tb=tbName1))
        rs = total - drop.rows

        # tb1 dropPartition
        tmp = self.s.loadTable(tbName1, dbPath)
        self.assertEqual(tmp.rows, total)
        self.s.dropPartition(dbPath, ["2010.01.01", "1", "'AMD'"], tbName1)
        tmp = self.s.loadTable(tbName1, dbPath)
        self.assertEqual(tmp.rows, rs)

        # tb2 dropPartition
        tmp = self.s.loadTable(tbName2, dbPath)
        self.assertEqual(tmp.rows, total)
        self.s.dropPartition(dbPath, ["2010.01.01", "1", "'AMD'"], tbName2)
        tmp = self.s.loadTable(tbName2, dbPath)
        self.assertEqual(tmp.rows, rs)

        # both dropPartition
        create_dfs_compo_range_value_list_db()
        tmp = self.s.loadTable(tbName1, dbPath)
        self.assertEqual(tmp.rows, total)
        tmp = self.s.loadTable(tbName2, dbPath)
        self.assertEqual(tmp.rows, total)
        self.s.dropPartition(dbPath, ["2010.01.01", "1", "'AMD'"])
        tmp = self.s.loadTable(tbName1, dbPath)
        self.assertEqual(tmp.rows, rs)
        tmp = self.s.loadTable(tbName2, dbPath)
        self.assertEqual(tmp.rows, rs)

    def test_dropPartition_dfs_compo3_range_value_list_drop_level3_multiple(self):
        dbPath = DBInfo.dfsDBName
        tbName1 = DBInfo.table1
        tbName2 = DBInfo.table2
        create_dfs_compo_range_value_list_db()

        origin = self.s.loadTable(tbName1, dbPath)
        total = origin.rows

        drop = self.s.loadTableBySQL(tableName=tbName1, dbPath=dbPath,
                                     sql="select * from {tb} where date<2010.02.01 and id=1 and sym in `DOP`ASZ`FSD`BBVC".format(tb=tbName1))
        rs = total - drop.rows

        # tb1 dropPartition
        tmp = self.s.loadTable(tbName1, dbPath)
        self.assertEqual(tmp.rows, total)
        self.s.dropPartition(dbPath, ["2010.01.01", "1", "`DOP`FSD"], tbName1)  # 1
        tmp = self.s.loadTable(tbName1, dbPath)
        self.assertEqual(tmp.rows, rs)

        # tb2 dropPartition
        tmp = self.s.loadTable(tbName2, dbPath)
        self.assertEqual(tmp.rows, total)
        self.s.dropPartition(dbPath, ["2010.01.01", "1", "['DOP','FSD']"], tbName2)
        tmp = self.s.loadTable(tbName2, dbPath)
        self.assertEqual(tmp.rows, rs)

        # both dropPartition
        create_dfs_compo_range_value_list_db()
        tmp = self.s.loadTable(tbName1, dbPath)
        self.assertEqual(tmp.rows, total)
        tmp = self.s.loadTable(tbName2, dbPath)
        self.assertEqual(tmp.rows, total)
        self.s.dropPartition(dbPath, ["2010.01.01", "1", "`DOP`ASZ`FSD`BBVC"])
        tmp = self.s.loadTable(tbName1, dbPath)
        self.assertEqual(tmp.rows, rs)
        tmp = self.s.loadTable(tbName2, dbPath)
        self.assertEqual(tmp.rows, rs)

    def test_dropPartition_dfs_compo3_range_value_list_drop_multiple(self):
        dbPath = DBInfo.dfsDBName
        tbName1 = DBInfo.table1
        tbName2 = DBInfo.table2
        create_dfs_compo_range_value_list_db()

        origin = self.s.loadTable(tbName1, dbPath)
        total = origin.rows

        drop = self.s.loadTableBySQL(tableName=tbName1, dbPath=dbPath,
                                     sql="select * from {tb} where date < 2010.03.01 and id in 1..3 and sym in `DOP`ASZ`FSD`BBVC".format(
                                         tb=tbName1))
        rs = total - drop.rows

        # tb1 dropPartition
        tmp = self.s.loadTable(tbName1, dbPath)
        self.assertEqual(tmp.rows, total)
        self.s.dropPartition(dbPath, ["[2010.01.01,2010.02.01]", "1..3", "['DOP','FSD']"], tbName1)  # 1
        tmp = self.s.loadTable(tbName1, dbPath)
        self.assertEqual(tmp.rows, rs)

        # tb2 dropPartition
        tmp = self.s.loadTable(tbName2, dbPath)
        self.assertEqual(tmp.rows, total)
        self.s.dropPartition(dbPath, ["[2010.01.01,2010.02.01]", "1..3", "['DOP','FSD']"], tbName2)
        tmp = self.s.loadTable(tbName2, dbPath)
        self.assertEqual(tmp.rows, rs)

        # both dropPartition
        create_dfs_compo_range_value_list_db()
        tmp = self.s.loadTable(tbName1, dbPath)
        self.assertEqual(tmp.rows, total)
        tmp = self.s.loadTable(tbName2, dbPath)
        self.assertEqual(tmp.rows, total)
        self.s.dropPartition(dbPath, ["[2010.01.01,2010.02.01]", "1..3", "['DOP','FSD']"])
        tmp = self.s.loadTable(tbName1, dbPath)
        self.assertEqual(tmp.rows, rs)
        tmp = self.s.loadTable(tbName2, dbPath)
        self.assertEqual(tmp.rows, rs)