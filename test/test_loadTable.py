import unittest
import dolphindb as ddb
from numpy import repeat
from numpy.testing import assert_array_equal
from pandas.testing import assert_frame_equal
from setup import HOST, PORT, WORK_DIR


class DBInfo:
    dfsDBName = 'dfs://testLoadTable'
    diskDBName = WORK_DIR + '/testLoadTable'
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
    tdata=table(sort(take(2010.01.01..2010.12.31, n)) as date, take(1..10,n) as id,take(`AMD`QWE`CES`DOP`ASZ`FSD`BBVC`AWQ`DS, n) as sym,rand(100,n) as val)
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


def create_disk_compo_range_hash_db():
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
    '''.format(db=DBInfo.diskDBName, tb1=DBInfo.table1, tb2=DBInfo.table2)
    s.run(ddb_script)
    s.close()


def create_disk_compo_range_value_db():
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
    '''.format(db=DBInfo.diskDBName, tb1=DBInfo.table1, tb2=DBInfo.table2)
    s.run(ddb_script)
    s.close()


def create_disk_compo_range_list_db():
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
    '''.format(db=DBInfo.diskDBName, tb1=DBInfo.table1, tb2=DBInfo.table2)
    s.run(ddb_script)
    s.close()


def create_disk_compo_range_hash_list_db():
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
    '''.format(db=DBInfo.diskDBName, tb1=DBInfo.table1, tb2=DBInfo.table2)
    s.run(ddb_script)
    s.close()


def create_disk_compo_range_value_list_db():
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
    '''.format(db=DBInfo.diskDBName, tb1=DBInfo.table1, tb2=DBInfo.table2)
    s.run(ddb_script)
    s.close()


class LoadTableTest(unittest.TestCase):
    @classmethod
    def setUp(cls):
        cls.s = ddb.session()
        cls.s.connect(HOST, PORT, "admin", "123456")
        dbPaths = [DBInfo.dfsDBName, DBInfo.diskDBName]
        for dbPath in dbPaths:
            script = """
            if(existsDatabase('{dbPath}'))
                dropDatabase('{dbPath}')
            if(exists('{dbPath}'))
                rmdir('{dbPath}', true)
            """.format(dbPath=dbPath)
            cls.s.run(script)

    @classmethod
    def tearDown(cls):
        cls.s = ddb.session()
        cls.s.connect(HOST, PORT, "admin", "123456")
        dbPaths = [DBInfo.dfsDBName, DBInfo.diskDBName]
        for dbPath in dbPaths:
            script = """
            if(existsDatabase('{dbPath}'))
                dropDatabase('{dbPath}')
            if(exists('{dbPath}'))
                rmdir('{dbPath}', true)
            """.format(dbPath=dbPath)
            cls.s.run(script)

    def test_loadTable_dfs_dimension(self):
        dbPath = DBInfo.dfsDBName
        tbName1 = DBInfo.table1
        create_dfs_dimension_db()
        rs = self.s.run("select * from loadTable('{db}','{tb}')".format(db=dbPath, tb=tbName1))
        tmp = self.s.loadTable(tableName=tbName1, dbPath=dbPath)
        assert_frame_equal(tmp.toDF(), rs)

    def test_loadTable_dfs_range(self):
        dbPath = DBInfo.dfsDBName
        tbName1 = DBInfo.table1
        create_dfs_range_db()
        rs = self.s.run("select * from loadTable('{db}','{tb}')".format(db=dbPath, tb=tbName1))
        tmp = self.s.loadTable(tableName=tbName1, dbPath=dbPath)
        assert_frame_equal(tmp.toDF(), rs)

    def test_loadTable_dfs_range_param_partitions(self):
        dbPath = DBInfo.dfsDBName
        tbName1 = DBInfo.table1
        create_dfs_range_db()
        rs = self.s.run("select * from loadTable('{db}','{tb}')".format(db=dbPath, tb=tbName1))
        tmp = self.s.loadTable(tableName=tbName1, dbPath=dbPath, partitions=[5000, 15000])
        # dfs database does not support parameter "partitions" and loads all meta-data
        assert_frame_equal(tmp.toDF(), rs)

    def test_loadTable_dfs_range_param_memoryMode(self):
        dbPath = DBInfo.dfsDBName
        tbName1 = DBInfo.table1
        create_dfs_range_db()

        rs = self.s.run("select * from loadTable('{db}','{tb}')".format(db=dbPath, tb=tbName1))
        before = list(self.s.run("exec memSize from getSessionMemoryStat()"))
        tmp = self.s.loadTable(tableName=tbName1, dbPath=dbPath, memoryMode=True)
        after = list(self.s.run("exec memSize from getSessionMemoryStat()"))

        assert_frame_equal(tmp.toDF(), rs)
        # dfs databse doesn't support parameter "memoryMode" and never loads all data into memory without specification
        assert_array_equal(after == before, repeat(True, len(after)))

    def test_loadTable_dfs_hash(self):
        dbPath = DBInfo.dfsDBName
        tbName1 = DBInfo.table1
        create_dfs_hash_db()
        rs = self.s.run("select * from loadTable('{db}','{tb}')".format(db=dbPath, tb=tbName1))
        tmp = self.s.loadTable(tableName=tbName1, dbPath=dbPath)
        assert_frame_equal(tmp.toDF(), rs)

    def test_loadTable_dfs_hash_param_partitions(self):
        dbPath = DBInfo.dfsDBName
        tbName1 = DBInfo.table1
        create_dfs_hash_db()
        rs = self.s.run("select * from loadTable('{db}','{tb}')".format(db=dbPath, tb=tbName1))
        tmp = self.s.loadTable(tableName=tbName1, dbPath=dbPath, partitions=[1, 2])
        # dfs database does not support parameter "partitions" and loads all meta-data
        assert_frame_equal(tmp.toDF(), rs)

    def test_loadTable_dfs_hash_param_memoryMode(self):
        dbPath = DBInfo.dfsDBName
        tbName1 = DBInfo.table1
        create_dfs_hash_db()

        rs = self.s.run("select * from loadTable('{db}','{tb}')".format(db=dbPath, tb=tbName1))
        before = list(self.s.run("exec memSize from getSessionMemoryStat()"))
        tmp = self.s.loadTable(tableName=tbName1, dbPath=dbPath, memoryMode=True)
        after = list(self.s.run("exec memSize from getSessionMemoryStat()"))

        assert_frame_equal(tmp.toDF(), rs)
        # dfs databse doesn't support parameter "memoryMode" and never loads all data into memory without specification
        assert_array_equal(after == before, repeat(True, len(after)))

    def test_loadTable_dfs_value(self):
        dbPath = DBInfo.dfsDBName
        tbName1 = DBInfo.table1
        create_dfs_value_db()
        rs = self.s.run("select * from loadTable('{db}','{tb}')".format(db=dbPath, tb=tbName1))
        tmp = self.s.loadTable(tableName=tbName1, dbPath=dbPath)
        assert_frame_equal(tmp.toDF(), rs)

    def test_loadTable_dfs_value_param_partitions(self):
        dbPath = DBInfo.dfsDBName
        tbName1 = DBInfo.table1
        create_dfs_value_db()
        rs = self.s.run("select * from loadTable('{db}','{tb}')".format(db=dbPath, tb=tbName1))
        tmp = self.s.loadTable(tableName=tbName1, dbPath=dbPath, partitions=["2010.01.01", "2010.01.30"])
        # dfs database does not support parameter "partitions" and loads all meta-data
        assert_frame_equal(tmp.toDF(), rs)

    def test_loadTable_dfs_value_param_memoryMode(self):
        dbPath = DBInfo.dfsDBName
        tbName1 = DBInfo.table1
        create_dfs_value_db()

        rs = self.s.run("select * from loadTable('{db}','{tb}')".format(db=dbPath, tb=tbName1))
        before = list(self.s.run("exec memSize from getSessionMemoryStat()"))
        tmp = self.s.loadTable(tableName=tbName1, dbPath=dbPath, memoryMode=True)
        after = list(self.s.run("exec memSize from getSessionMemoryStat()"))

        assert_frame_equal(tmp.toDF(), rs)
        # dfs databse doesn't support parameter "memoryMode" and never loads all data into memory without specification
        assert_array_equal(after == before, repeat(True, len(after)))

    def test_loadTable_dfs_list(self):
        dbPath = DBInfo.dfsDBName
        tbName1 = DBInfo.table1
        create_dfs_list_db()
        rs = self.s.run("select * from loadTable('{db}','{tb}')".format(db=dbPath, tb=tbName1))
        tmp = self.s.loadTable(tableName=tbName1, dbPath=dbPath)
        assert_frame_equal(tmp.toDF(), rs)

    def test_loadTable_dfs_list_param_partitions(self):
        dbPath = DBInfo.dfsDBName
        tbName1 = DBInfo.table1
        create_dfs_list_db()
        rs = self.s.run("select * from loadTable('{db}','{tb}')".format(db=dbPath, tb=tbName1))
        tmp = self.s.loadTable(tableName=tbName1, dbPath=dbPath, partitions=["`DOP", "`BBVC"])
        # dfs database does not support parameter "partitions" and loads all meta-data
        assert_frame_equal(tmp.toDF(), rs)

    def test_loadTable_dfs_list_param_memoryMode(self):
        dbPath = DBInfo.dfsDBName
        tbName1 = DBInfo.table1
        create_dfs_list_db()

        rs = self.s.run("select * from loadTable('{db}','{tb}')".format(db=dbPath, tb=tbName1))
        before = list(self.s.run("exec memSize from getSessionMemoryStat()"))
        tmp = self.s.loadTable(tableName=tbName1, dbPath=dbPath, memoryMode=True)
        after = list(self.s.run("exec memSize from getSessionMemoryStat()"))

        assert_frame_equal(tmp.toDF(), rs)
        # dfs databse doesn't support parameter "memoryMode" and never loads all data into memory without specification
        assert_array_equal(after == before, repeat(True, len(after)))

    def test_loadTable_dfs_compo_range_range(self):
        dbPath = DBInfo.dfsDBName
        tbName1 = DBInfo.table1
        create_dfs_compo_range_range_db()
        rs = self.s.run("select * from loadTable('{db}','{tb}')".format(db=dbPath, tb=tbName1))
        tmp = self.s.loadTable(tableName=tbName1, dbPath=dbPath)
        assert_frame_equal(tmp.toDF(), rs)

    def test_loadTable_dfs_compo_range_range_param_partitions(self):
        dbPath = DBInfo.dfsDBName
        tbName1 = DBInfo.table1
        create_dfs_compo_range_range_db()
        rs = self.s.run("select * from loadTable('{db}','{tb}')".format(db=dbPath, tb=tbName1))
        tmp = self.s.loadTable(tableName=tbName1, dbPath=dbPath, partitions=["2010.01.01", "2010.01.30"])
        # dfs database does not support parameter "partitions" and loads all meta-data
        assert_frame_equal(tmp.toDF(), rs)

    def test_loadTable_dfs_compo_range_range_param_memoryMode(self):
        dbPath = DBInfo.dfsDBName
        tbName1 = DBInfo.table1
        create_dfs_compo_range_range_db()

        rs = self.s.run("select * from loadTable('{db}','{tb}')".format(db=dbPath, tb=tbName1))
        before = list(self.s.run("exec memSize from getSessionMemoryStat()"))
        tmp = self.s.loadTable(tableName=tbName1, dbPath=dbPath, memoryMode=True)
        after = list(self.s.run("exec memSize from getSessionMemoryStat()"))

        assert_frame_equal(tmp.toDF(), rs)
        # dfs databse doesn't support parameter "memoryMode" and never loads all data into memory without specification
        assert_array_equal(after == before, repeat(True, len(after)))

    def test_loadTable_dfs_compo_range_hash(self):
        dbPath = DBInfo.dfsDBName
        tbName1 = DBInfo.table1
        create_dfs_compo_range_hash_db()
        rs = self.s.run("select * from loadTable('{db}','{tb}')".format(db=dbPath, tb=tbName1))
        tmp = self.s.loadTable(tableName=tbName1, dbPath=dbPath)
        assert_frame_equal(tmp.toDF(), rs)

    def test_loadTable_dfs_compo_range_hash_param_partitions(self):
        dbPath = DBInfo.dfsDBName
        tbName1 = DBInfo.table1
        create_dfs_compo_range_hash_db()
        rs = self.s.run("select * from loadTable('{db}','{tb}')".format(db=dbPath, tb=tbName1))
        tmp = self.s.loadTable(tableName=tbName1, dbPath=dbPath, partitions=["2010.01.01", "2010.01.30"])
        # dfs database does not support parameter "partitions" and loads all meta-data
        assert_frame_equal(tmp.toDF(), rs)

    def test_loadTable_dfs_compo_range_hash_param_memoryMode(self):
        dbPath = DBInfo.dfsDBName
        tbName1 = DBInfo.table1
        create_dfs_compo_range_hash_db()

        rs = self.s.run("select * from loadTable('{db}','{tb}')".format(db=dbPath, tb=tbName1))
        before = list(self.s.run("exec memSize from getSessionMemoryStat()"))
        tmp = self.s.loadTable(tableName=tbName1, dbPath=dbPath, memoryMode=True)
        after = list(self.s.run("exec memSize from getSessionMemoryStat()"))

        assert_frame_equal(tmp.toDF(), rs)
        # dfs databse doesn't support parameter "memoryMode" and never loads all data into memory without specification
        assert_array_equal(after == before, repeat(True, len(after)))

    def test_loadTable_dfs_compo_range_value(self):
        dbPath = DBInfo.dfsDBName
        tbName1 = DBInfo.table1
        create_dfs_compo_range_value_db()
        rs = self.s.run("select * from loadTable('{db}','{tb}')".format(db=dbPath, tb=tbName1))
        tmp = self.s.loadTable(tableName=tbName1, dbPath=dbPath)
        assert_frame_equal(tmp.toDF(), rs)

    def test_loadTable_dfs_compo_range_value_param_partitions(self):
        dbPath = DBInfo.dfsDBName
        tbName1 = DBInfo.table1
        create_dfs_compo_range_value_db()
        rs = self.s.run("select * from loadTable('{db}','{tb}')".format(db=dbPath, tb=tbName1))
        tmp = self.s.loadTable(tableName=tbName1, dbPath=dbPath, partitions=["2010.01.01", "2010.01.30"])
        # dfs database does not support parameter "partitions" and loads all meta-data
        assert_frame_equal(tmp.toDF(), rs)

    def test_loadTable_dfs_compo_range_value_param_memoryMode(self):
        dbPath = DBInfo.dfsDBName
        tbName1 = DBInfo.table1
        create_dfs_compo_range_value_db()

        rs = self.s.run("select * from loadTable('{db}','{tb}')".format(db=dbPath, tb=tbName1))
        before = list(self.s.run("exec memSize from getSessionMemoryStat()"))
        tmp = self.s.loadTable(tableName=tbName1, dbPath=dbPath, memoryMode=True)
        after = list(self.s.run("exec memSize from getSessionMemoryStat()"))

        assert_frame_equal(tmp.toDF(), rs)
        # dfs databse doesn't support parameter "memoryMode" and never loads all data into memory without specification
        assert_array_equal(after == before, repeat(True, len(after)))

    def test_loadTable_dfs_compo_range_list(self):
        dbPath = DBInfo.dfsDBName
        tbName1 = DBInfo.table1
        create_dfs_compo_range_list_db()
        rs = self.s.run("select * from loadTable('{db}','{tb}')".format(db=dbPath, tb=tbName1))
        tmp = self.s.loadTable(tableName=tbName1, dbPath=dbPath)
        assert_frame_equal(tmp.toDF(), rs)

    def test_loadTable_dfs_compo_range_list_param_partitions(self):
        dbPath = DBInfo.dfsDBName
        tbName1 = DBInfo.table1
        create_dfs_compo_range_list_db()
        rs = self.s.run("select * from loadTable('{db}','{tb}')".format(db=dbPath, tb=tbName1))
        tmp = self.s.loadTable(tableName=tbName1, dbPath=dbPath, partitions=["2010.01.01", "2010.01.30"])
        # dfs database does not support parameter "partitions" and loads all meta-data
        assert_frame_equal(tmp.toDF(), rs)

    def test_loadTable_dfs_compo_range_list_param_memoryMode(self):
        dbPath = DBInfo.dfsDBName
        tbName1 = DBInfo.table1
        create_dfs_compo_range_list_db()

        rs = self.s.run("select * from loadTable('{db}','{tb}')".format(db=dbPath, tb=tbName1))
        before = list(self.s.run("exec memSize from getSessionMemoryStat()"))
        tmp = self.s.loadTable(tableName=tbName1, dbPath=dbPath, memoryMode=True)
        after = list(self.s.run("exec memSize from getSessionMemoryStat()"))

        assert_frame_equal(tmp.toDF(), rs)
        # dfs databse doesn't support parameter "memoryMode" and never loads all data into memory without specification
        assert_array_equal(after == before, repeat(True, len(after)))

    def test_loadTable_dfs_compo_range_hash_list(self):
        dbPath = DBInfo.dfsDBName
        tbName1 = DBInfo.table1
        create_dfs_compo_range_hash_list_db()
        rs = self.s.run("select * from loadTable('{db}','{tb}')".format(db=dbPath, tb=tbName1))
        tmp = self.s.loadTable(tableName=tbName1, dbPath=dbPath)
        assert_frame_equal(tmp.toDF(), rs)

    def test_loadTable_dfs_compo_range_hash_list_param_partitions(self):
        dbPath = DBInfo.dfsDBName
        tbName1 = DBInfo.table1
        create_dfs_compo_range_hash_list_db()
        rs = self.s.run("select * from loadTable('{db}','{tb}')".format(db=dbPath, tb=tbName1))
        tmp = self.s.loadTable(tableName=tbName1, dbPath=dbPath, partitions=["2010.01.01", "2010.01.30"])
        # dfs database does not support parameter "partitions" and loads all meta-data
        assert_frame_equal(tmp.toDF(), rs)

    def test_loadTable_dfs_compo_range_hash_list_param_memoryMode(self):
        dbPath = DBInfo.dfsDBName
        tbName1 = DBInfo.table1
        create_dfs_compo_range_hash_list_db()

        rs = self.s.run("select * from loadTable('{db}','{tb}')".format(db=dbPath, tb=tbName1))
        before = list(self.s.run("exec memSize from getSessionMemoryStat()"))
        tmp = self.s.loadTable(tableName=tbName1, dbPath=dbPath, memoryMode=True)
        after = list(self.s.run("exec memSize from getSessionMemoryStat()"))

        assert_frame_equal(tmp.toDF(), rs)
        # dfs databse doesn't support parameter "memoryMode" and never loads all data into memory without specification
        assert_array_equal(after == before, repeat(True, len(after)))

    def test_loadTable_dfs_compo_range_value_list(self):
        dbPath = DBInfo.dfsDBName
        tbName1 = DBInfo.table1
        create_dfs_compo_range_value_list_db()
        rs = self.s.run("select * from loadTable('{db}','{tb}')".format(db=dbPath, tb=tbName1))
        tmp = self.s.loadTable(tableName=tbName1, dbPath=dbPath)
        assert_frame_equal(tmp.toDF(), rs)

    def test_loadTable_dfs_compo_range_value_list_param_partitions(self):
        dbPath = DBInfo.dfsDBName
        tbName1 = DBInfo.table1
        create_dfs_compo_range_value_list_db()
        rs = self.s.run("select * from loadTable('{db}','{tb}')".format(db=dbPath, tb=tbName1))
        tmp = self.s.loadTable(tableName=tbName1, dbPath=dbPath, partitions=["2010.01.01", "2010.01.30"])
        # dfs database does not support parameter "partitions" and loads all meta-data
        assert_frame_equal(tmp.toDF(), rs)

    def test_loadTable_dfs_compo_range_value_list_param_memoryMode(self):
        dbPath = DBInfo.dfsDBName
        tbName1 = DBInfo.table1
        create_dfs_compo_range_value_list_db()

        rs = self.s.run("select * from loadTable('{db}','{tb}')".format(db=dbPath, tb=tbName1))
        before = list(self.s.run("exec memSize from getSessionMemoryStat()"))
        tmp = self.s.loadTable(tableName=tbName1, dbPath=dbPath, memoryMode=True)
        after = list(self.s.run("exec memSize from getSessionMemoryStat()"))

        assert_frame_equal(tmp.toDF(), rs)
        # dfs databse doesn't support parameter "memoryMode" and never loads all data into memory without specification
        assert_array_equal(after == before, repeat(True, len(after)))

    def test_loadTable_disk_unpartitioned(self):
        dbPath = DBInfo.diskDBName
        tbName1 = DBInfo.table1
        create_disk_unpartitioned_db()
        rs = self.s.run("select * from loadTable('{db}','{tb}')".format(db=dbPath, tb=tbName1))
        tmp = self.s.loadTable(tableName=tbName1, dbPath=dbPath)
        assert_frame_equal(tmp.toDF(), rs)

    def test_loadTable_disk_range(self):
        dbPath = DBInfo.diskDBName
        tbName1 = DBInfo.table1
        create_disk_range_db()
        rs = self.s.run("select * from loadTable('{db}','{tb}')".format(db=dbPath, tb=tbName1))
        tmp = self.s.loadTable(tableName=tbName1, dbPath=dbPath)
        assert_frame_equal(tmp.toDF(), rs)

    def test_loadTable_disk_range_param_partitions(self):
        dbPath = DBInfo.diskDBName
        tbName1 = DBInfo.table1
        create_disk_range_db()
        rs = self.s.run("select * from loadTable('{db}','{tb}') where id<20001".format(db=dbPath, tb=tbName1))
        tmp = self.s.loadTable(tableName=tbName1, dbPath=dbPath, partitions=[5000, 15000])
        assert_frame_equal(tmp.toDF(), rs)

    def test_loadTable_disk_range_param_memoryMode(self):
        dbPath = DBInfo.diskDBName
        tbName1 = DBInfo.table1
        create_disk_range_db()
        rs = self.s.run("select * from loadTable('{db}','{tb}')".format(db=dbPath, tb=tbName1))

        before = list(self.s.run("exec memSize from getSessionMemoryStat()"))
        tmp = self.s.loadTable(tableName=tbName1, dbPath=dbPath, memoryMode=True)
        after = list(self.s.run("exec memSize from getSessionMemoryStat()"))
        assert_frame_equal(tmp.toDF(), rs)
        assert_array_equal(after >= before, repeat(True, 4))

    def test_loadTable_disk_hash(self):
        dbPath = DBInfo.diskDBName
        tbName1 = DBInfo.table1
        create_disk_hash_db()
        rs = self.s.run("select * from loadTable('{db}','{tb}')".format(db=dbPath, tb=tbName1))
        tmp = self.s.loadTable(tableName=tbName1, dbPath=dbPath)
        assert_frame_equal(tmp.toDF(), rs)

    def test_loadTable_disk_hash_param_partitions(self):
        dbPath = DBInfo.diskDBName
        tbName1 = DBInfo.table1
        create_disk_hash_db()
        rs = self.s.run("select * from loadTable('{db}','{tb}') where id in [1,3,5]".format(db=dbPath, tb=tbName1))
        tmp = self.s.loadTable(tableName=tbName1, dbPath=dbPath, partitions=[1, 3, 5])
        assert_frame_equal(tmp.toDF(), rs)

    def test_loadTable_disk_hash_param_memoryMode(self):
        dbPath = DBInfo.diskDBName
        tbName1 = DBInfo.table1
        create_disk_hash_db()
        rs = self.s.run("select * from loadTable('{db}','{tb}')".format(db=dbPath, tb=tbName1))

        before = list(self.s.run("exec memSize from getSessionMemoryStat()"))
        tmp = self.s.loadTable(tableName=tbName1, dbPath=dbPath, memoryMode=True)
        after = list(self.s.run("exec memSize from getSessionMemoryStat()"))
        assert_frame_equal(tmp.toDF(), rs)
        assert_array_equal(after >= before, repeat(True, 4))

    def test_loadTable_disk_value(self):
        dbPath = DBInfo.diskDBName
        tbName1 = DBInfo.table1
        create_disk_value_db()
        rs = self.s.run("select * from loadTable('{db}','{tb}')".format(db=dbPath, tb=tbName1))
        tmp = self.s.loadTable(tableName=tbName1, dbPath=dbPath)
        assert_frame_equal(tmp.toDF(), rs)

    def test_loadTable_disk_value_param_partitions(self):
        dbPath = DBInfo.diskDBName
        tbName1 = DBInfo.table1
        create_disk_value_db()
        rs = self.s.run("select * from loadTable('{db}','{tb}') where date in [2010.01.01, 2010.01.30]".format(db=dbPath, tb=tbName1))
        tmp = self.s.loadTable(tableName=tbName1, dbPath=dbPath, partitions=["2010.01.01", "2010.01.30"])
        assert_frame_equal(tmp.toDF(), rs)

    def test_loadTable_disk_value_param_memoryMode(self):
        dbPath = DBInfo.diskDBName
        tbName1 = DBInfo.table1
        create_disk_value_db()
        rs = self.s.run("select * from loadTable('{db}','{tb}')".format(db=dbPath, tb=tbName1))

        before = list(self.s.run("exec memSize from getSessionMemoryStat()"))
        tmp = self.s.loadTable(tableName=tbName1, dbPath=dbPath, memoryMode=True)
        after = list(self.s.run("exec memSize from getSessionMemoryStat()"))
        assert_frame_equal(tmp.toDF(), rs)
        assert_array_equal(after >= before, repeat(True, 4))

    def test_loadTable_disk_list(self):
        dbPath = DBInfo.diskDBName
        tbName1 = DBInfo.table1
        create_disk_list_db()
        rs = self.s.run("select * from loadTable('{db}','{tb}')".format(db=dbPath, tb=tbName1))
        tmp = self.s.loadTable(tableName=tbName1, dbPath=dbPath)
        assert_frame_equal(tmp.toDF(), rs)

    def test_loadTable_disk_list_param_partitions(self):
        dbPath = DBInfo.diskDBName
        tbName1 = DBInfo.table1
        create_disk_list_db()
        rs = self.s.run("select * from loadTable('{db}','{tb}') where sym in `DOP`ASZ`FSD`BBVC`AWQ`DS".format(db=dbPath, tb=tbName1))
        tmp = self.s.loadTable(tableName=tbName1, dbPath=dbPath, partitions=["`DOP", "`FSD", "`AWQ"])
        assert_frame_equal(tmp.toDF(), rs)

    def test_loadTable_disk_list_param_memoryMode(self):
        dbPath = DBInfo.diskDBName
        tbName1 = DBInfo.table1
        create_disk_list_db()
        rs = self.s.run("select * from loadTable('{db}','{tb}')".format(db=dbPath, tb=tbName1))

        before = list(self.s.run("exec memSize from getSessionMemoryStat()"))
        tmp = self.s.loadTable(tableName=tbName1, dbPath=dbPath, memoryMode=True)
        after = list(self.s.run("exec memSize from getSessionMemoryStat()"))
        assert_frame_equal(tmp.toDF(), rs)
        assert_array_equal(after >= before, repeat(True, 4))

    def test_loadTable_disk_compo_range_range(self):
        dbPath = DBInfo.diskDBName
        tbName1 = DBInfo.table1
        create_disk_compo_range_range_db()
        rs = self.s.run("select * from loadTable('{db}','{tb}')".format(db=dbPath, tb=tbName1))
        tmp = self.s.loadTable(tableName=tbName1, dbPath=dbPath)
        assert_frame_equal(tmp.toDF(), rs)

    def test_loadTable_disk_compo_range_range_param_partitions(self):
        dbPath = DBInfo.diskDBName
        tbName1 = DBInfo.table1
        create_disk_compo_range_range_db()
        rs = self.s.run("select * from loadTable('{db}','{tb}') where "
                        "date between 2010.01.01:2010.01.31 "
                        "or date between 2010.04.01:2010.04.30".format(db=dbPath, tb=tbName1))
        tmp = self.s.loadTable(tableName=tbName1, dbPath=dbPath, partitions=["2010.01.01", "2010.04.25"])
        assert_frame_equal(tmp.toDF(), rs)

    def test_loadTable_disk_compo_range_range_param_memoryMode(self):
        dbPath = DBInfo.diskDBName
        tbName1 = DBInfo.table1
        create_disk_compo_range_range_db()
        rs = self.s.run("select * from loadTable('{db}','{tb}')".format(db=dbPath, tb=tbName1))

        before = list(self.s.run("exec memSize from getSessionMemoryStat()"))
        tmp = self.s.loadTable(tableName=tbName1, dbPath=dbPath, memoryMode=True)
        after = list(self.s.run("exec memSize from getSessionMemoryStat()"))
        assert_frame_equal(tmp.toDF(), rs)
        assert_array_equal(after >= before, repeat(True, 4))

    def test_loadTable_disk_compo_range_hash(self):
        dbPath = DBInfo.diskDBName
        tbName1 = DBInfo.table1
        create_disk_compo_range_hash_db()
        rs = self.s.run("select * from loadTable('{db}','{tb}')".format(db=dbPath, tb=tbName1))
        tmp = self.s.loadTable(tableName=tbName1, dbPath=dbPath)
        assert_frame_equal(tmp.toDF(), rs)

    def test_loadTable_disk_compo_range_hash_param_partitions(self):
        dbPath = DBInfo.diskDBName
        tbName1 = DBInfo.table1
        create_disk_compo_range_hash_db()
        rs = self.s.run("select * from loadTable('{db}','{tb}') where "
                        "date between 2010.01.01:2010.01.31 "
                        "or date between 2010.04.01:2010.04.30".format(db=dbPath, tb=tbName1))
        tmp = self.s.loadTable(tableName=tbName1, dbPath=dbPath, partitions=["2010.01.01", "2010.04.25"])
        assert_frame_equal(tmp.toDF(), rs)

    def test_loadTable_disk_compo_range_hash_param_memoryMode(self):
        dbPath = DBInfo.diskDBName
        tbName1 = DBInfo.table1
        create_disk_compo_range_hash_db()
        rs = self.s.run("select * from loadTable('{db}','{tb}')".format(db=dbPath, tb=tbName1))

        before = list(self.s.run("exec memSize from getSessionMemoryStat()"))
        tmp = self.s.loadTable(tableName=tbName1, dbPath=dbPath, memoryMode=True)
        after = list(self.s.run("exec memSize from getSessionMemoryStat()"))
        assert_frame_equal(tmp.toDF(), rs)
        assert_array_equal(after >= before, repeat(True, 4))

    def test_loadTable_disk_compo_range_value(self):
        dbPath = DBInfo.diskDBName
        tbName1 = DBInfo.table1
        create_disk_compo_range_value_db()
        rs = self.s.run("select * from loadTable('{db}','{tb}')".format(db=dbPath, tb=tbName1))
        tmp = self.s.loadTable(tableName=tbName1, dbPath=dbPath)
        assert_frame_equal(tmp.toDF(), rs)

    def test_loadTable_disk_compo_range_value_param_partitions(self):
        dbPath = DBInfo.diskDBName
        tbName1 = DBInfo.table1
        create_disk_compo_range_value_db()
        rs = self.s.run("select * from loadTable('{db}','{tb}') where "
                        "date between 2010.01.01:2010.01.31 "
                        "or date between 2010.04.01:2010.04.30".format(db=dbPath, tb=tbName1))
        tmp = self.s.loadTable(tableName=tbName1, dbPath=dbPath, partitions=["2010.01.01", "2010.04.25"])
        assert_frame_equal(tmp.toDF(), rs)

    def test_loadTable_disk_compo_range_value_param_memoryMode(self):
        dbPath = DBInfo.diskDBName
        tbName1 = DBInfo.table1
        create_disk_compo_range_value_db()
        rs = self.s.run("select * from loadTable('{db}','{tb}')".format(db=dbPath, tb=tbName1))

        before = list(self.s.run("exec memSize from getSessionMemoryStat()"))
        tmp = self.s.loadTable(tableName=tbName1, dbPath=dbPath, memoryMode=True)
        after = list(self.s.run("exec memSize from getSessionMemoryStat()"))
        assert_frame_equal(tmp.toDF(), rs)
        assert_array_equal(after >= before, repeat(True, 4))

    def test_loadTable_disk_compo_range_list(self):
        dbPath = DBInfo.diskDBName
        tbName1 = DBInfo.table1
        create_disk_compo_range_list_db()
        rs = self.s.run("select * from loadTable('{db}','{tb}')".format(db=dbPath, tb=tbName1))
        tmp = self.s.loadTable(tableName=tbName1, dbPath=dbPath)
        assert_frame_equal(tmp.toDF(), rs)

    def test_loadTable_disk_compo_range_list_param_partitions(self):
        dbPath = DBInfo.diskDBName
        tbName1 = DBInfo.table1
        create_disk_compo_range_list_db()
        rs = self.s.run("select * from loadTable('{db}','{tb}') where "
                        "date between 2010.01.01:2010.01.31 "
                        "or date between 2010.04.01:2010.04.30".format(db=dbPath, tb=tbName1))
        tmp = self.s.loadTable(tableName=tbName1, dbPath=dbPath, partitions=["2010.01.01", "2010.04.25"])
        assert_frame_equal(tmp.toDF(), rs)

    def test_loadTable_disk_compo_range_list_param_memoryMode(self):
        dbPath = DBInfo.diskDBName
        tbName1 = DBInfo.table1
        create_disk_compo_range_list_db()
        rs = self.s.run("select * from loadTable('{db}','{tb}')".format(db=dbPath, tb=tbName1))

        before = list(self.s.run("exec memSize from getSessionMemoryStat()"))
        tmp = self.s.loadTable(tableName=tbName1, dbPath=dbPath, memoryMode=True)
        after = list(self.s.run("exec memSize from getSessionMemoryStat()"))
        assert_frame_equal(tmp.toDF(), rs)
        assert_array_equal(after >= before, repeat(True, 4))

    def test_loadTable_disk_compo_range_hash_list(self):
        dbPath = DBInfo.diskDBName
        tbName1 = DBInfo.table1
        create_disk_compo_range_hash_list_db()
        rs = self.s.run("select * from loadTable('{db}','{tb}')".format(db=dbPath, tb=tbName1))
        tmp = self.s.loadTable(tableName=tbName1, dbPath=dbPath)
        assert_frame_equal(tmp.toDF(), rs)

    def test_loadTable_disk_compo_range_hash_list_param_partitions(self):
        dbPath = DBInfo.diskDBName
        tbName1 = DBInfo.table1
        create_disk_compo_range_hash_list_db()
        rs = self.s.run("select * from loadTable('{db}','{tb}') where "
                        "date between 2010.01.01:2010.01.31 "
                        "or date between 2010.04.01:2010.04.30".format(db=dbPath, tb=tbName1))
        tmp = self.s.loadTable(tableName=tbName1, dbPath=dbPath, partitions=["2010.01.01", "2010.04.25"])
        assert_frame_equal(tmp.toDF(), rs)

    def test_loadTable_disk_compo_range_hash_list_param_memoryMode(self):
        dbPath = DBInfo.diskDBName
        tbName1 = DBInfo.table1
        create_disk_compo_range_hash_list_db()
        rs = self.s.run("select * from loadTable('{db}','{tb}')".format(db=dbPath, tb=tbName1))

        before = list(self.s.run("exec memSize from getSessionMemoryStat()"))
        tmp = self.s.loadTable(tableName=tbName1, dbPath=dbPath, memoryMode=True)
        after = list(self.s.run("exec memSize from getSessionMemoryStat()"))
        assert_frame_equal(tmp.toDF(), rs)
        assert_array_equal(after >= before, repeat(True, 4))

    def test_loadTable_disk_compo_range_value_list(self):
        dbPath = DBInfo.diskDBName
        tbName1 = DBInfo.table1
        create_disk_compo_range_value_list_db()
        rs = self.s.run("select * from loadTable('{db}','{tb}')".format(db=dbPath, tb=tbName1))
        tmp = self.s.loadTable(tableName=tbName1, dbPath=dbPath)
        assert_frame_equal(tmp.toDF(), rs)

    def test_loadTable_disk_compo_range_value_list_param_partitions(self):
        dbPath = DBInfo.diskDBName
        tbName1 = DBInfo.table1
        create_disk_compo_range_value_list_db()
        rs = self.s.run("select * from loadTable('{db}','{tb}') where "
                        "date between 2010.01.01:2010.01.31 "
                        "or date between 2010.04.01:2010.04.30".format(db=dbPath, tb=tbName1))
        tmp = self.s.loadTable(tableName=tbName1, dbPath=dbPath, partitions=["2010.01.01", "2010.04.25"])
        assert_frame_equal(tmp.toDF(), rs)

    def test_loadTable_disk_compo_range_value_list_param_memoryMode(self):
        dbPath = DBInfo.diskDBName
        tbName1 = DBInfo.table1
        create_disk_compo_range_value_list_db()
        rs = self.s.run("select * from loadTable('{db}','{tb}')".format(db=dbPath, tb=tbName1))

        before = list(self.s.run("exec memSize from getSessionMemoryStat()"))
        tmp = self.s.loadTable(tableName=tbName1, dbPath=dbPath, memoryMode=True)
        after = list(self.s.run("exec memSize from getSessionMemoryStat()"))
        assert_frame_equal(tmp.toDF(), rs)
        assert_array_equal(after >= before, repeat(True, 4))


if __name__ == '__main__':
    unittest.main()
