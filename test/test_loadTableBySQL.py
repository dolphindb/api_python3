import unittest
import dolphindb as ddb
from pandas.testing import assert_frame_equal
from setup import HOST, PORT, WORK_DIR


class DBInfo:
    dfsDBName = 'dfs://testLoadTableBySQL'
    diskDBName = WORK_DIR + '/testLoadTableBySQL'
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


class LoadTableBySQLTest(unittest.TestCase):
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

    def test_loadTableBySQL_dfs_dimension(self):
        dbPath = DBInfo.diskDBName
        tbName1 = DBInfo.table1
        create_dfs_dimension_db()
        with self.assertRaises(RuntimeError):
            self.s.loadTableBySQL(tableName=tbName1, dbPath=dbPath,
                                    sql="select * from {tb} where date in [2010.01.05,2010.01.15,2010.01.19]".format(tb=tbName1))

    def test_loadTableBySQL_dfs_range(self):
        dbPath = DBInfo.dfsDBName
        tbName1 = DBInfo.table1
        create_dfs_range_db()
        rs = self.s.run("select * from loadTable('{db}','{tb}') where date in [2010.01.05,2010.01.15,2010.01.19]".format(db=dbPath, tb=tbName1))
        tmp = self.s.loadTableBySQL(tableName=tbName1, dbPath=dbPath,
                                    sql="select * from {tb} where date in [2010.01.05,2010.01.15,2010.01.19]".format(tb=tbName1))
        assert_frame_equal(rs, tmp.toDF())

    def test_loadTableBySQL_dfs_hash(self):
        dbPath = DBInfo.dfsDBName
        tbName1 = DBInfo.table1
        create_dfs_hash_db()
        rs = self.s.run("select * from loadTable('{db}','{tb}') where date in [2010.01.05,2010.01.15,2010.01.19]".format(db=dbPath, tb=tbName1))
        tmp = self.s.loadTableBySQL(tableName=tbName1, dbPath=dbPath,
                                    sql="select * from {tb} where date in [2010.01.05,2010.01.15,2010.01.19]".format(tb=tbName1))
        assert_frame_equal(rs, tmp.toDF())

    def test_loadTableBySQL_dfs_value(self):
        dbPath = DBInfo.dfsDBName
        tbName1 = DBInfo.table1
        create_dfs_value_db()
        rs = self.s.run("select * from loadTable('{db}','{tb}') where date in [2010.01.05,2010.01.15,2010.01.19]".format(db=dbPath, tb=tbName1))
        tmp = self.s.loadTableBySQL(tableName=tbName1, dbPath=dbPath,
                                    sql="select * from {tb} where date in [2010.01.05,2010.01.15,2010.01.19]".format(tb=tbName1))
        assert_frame_equal(rs, tmp.toDF())

    def test_loadTableBySQL_dfs_list(self):
        dbPath = DBInfo.dfsDBName
        tbName1 = DBInfo.table1
        create_dfs_list_db()
        rs = self.s.run("select * from loadTable('{db}','{tb}') where date in [2010.01.05,2010.01.15,2010.01.19]".format(db=dbPath, tb=tbName1))
        tmp = self.s.loadTableBySQL(tableName=tbName1, dbPath=dbPath,
                                    sql="select * from {tb} where date in [2010.01.05,2010.01.15,2010.01.19]".format(tb=tbName1))
        assert_frame_equal(rs, tmp.toDF())

    def test_loadTableBySQL_dfs_compo_range_range(self):
        dbPath = DBInfo.dfsDBName
        tbName1 = DBInfo.table1
        create_dfs_compo_range_range_db()
        rs = self.s.run("select * from loadTable('{db}','{tb}') where date in [2010.01.05,2010.01.15,2010.01.19]".format(db=dbPath, tb=tbName1))
        tmp = self.s.loadTableBySQL(tableName=tbName1, dbPath=dbPath,
                                    sql="select * from {tb} where date in [2010.01.05,2010.01.15,2010.01.19]".format(tb=tbName1))
        assert_frame_equal(rs, tmp.toDF())

    def test_loadTableBySQL_dfs_compo_range_hash(self):
        dbPath = DBInfo.dfsDBName
        tbName1 = DBInfo.table1
        create_dfs_compo_range_hash_db()
        rs = self.s.run("select * from loadTable('{db}','{tb}') where date in [2010.01.05,2010.01.15,2010.01.19]".format(db=dbPath, tb=tbName1))
        tmp = self.s.loadTableBySQL(tableName=tbName1, dbPath=dbPath,
                                    sql="select * from {tb} where date in [2010.01.05,2010.01.15,2010.01.19]".format(tb=tbName1))
        assert_frame_equal(rs, tmp.toDF())

    def test_loadTableBySQL_dfs_compo_range_value(self):
        dbPath = DBInfo.dfsDBName
        tbName1 = DBInfo.table1
        create_dfs_compo_range_value_db()
        rs = self.s.run("select * from loadTable('{db}','{tb}') where date in [2010.01.05,2010.01.15,2010.01.19]".format(db=dbPath, tb=tbName1))
        tmp = self.s.loadTableBySQL(tableName=tbName1, dbPath=dbPath,
                                    sql="select * from {tb} where date in [2010.01.05,2010.01.15,2010.01.19]".format(tb=tbName1))
        assert_frame_equal(rs, tmp.toDF())

    def test_loadTableBySQL_dfs_compo_range_list(self):
        dbPath = DBInfo.dfsDBName
        tbName1 = DBInfo.table1
        create_dfs_compo_range_list_db()
        rs = self.s.run("select * from loadTable('{db}','{tb}') where date in [2010.01.05,2010.01.15,2010.01.19]".format(db=dbPath, tb=tbName1))
        tmp = self.s.loadTableBySQL(tableName=tbName1, dbPath=dbPath,
                                    sql="select * from {tb} where date in [2010.01.05,2010.01.15,2010.01.19]".format(tb=tbName1))
        assert_frame_equal(rs, tmp.toDF())

    def test_loadTableBySQL_dfs_compo_range_hash_list(self):
        dbPath = DBInfo.dfsDBName
        tbName1 = DBInfo.table1
        create_dfs_compo_range_hash_list_db()
        rs = self.s.run("select * from loadTable('{db}','{tb}') where date in [2010.01.05,2010.01.15,2010.01.19]".format(db=dbPath, tb=tbName1))
        tmp = self.s.loadTableBySQL(tableName=tbName1, dbPath=dbPath,
                                    sql="select * from {tb} where date in [2010.01.05,2010.01.15,2010.01.19]".format(tb=tbName1))
        assert_frame_equal(rs, tmp.toDF())

    def test_loadTableBySQL_dfs_compo_range_value_list(self):
        dbPath = DBInfo.dfsDBName
        tbName1 = DBInfo.table1
        create_dfs_compo_range_value_list_db()
        rs = self.s.run("select * from loadTable('{db}','{tb}') where date in [2010.01.05,2010.01.15,2010.01.19]".format(db=dbPath, tb=tbName1))
        tmp = self.s.loadTableBySQL(tableName=tbName1, dbPath=dbPath,
                                    sql="select * from {tb} where date in [2010.01.05,2010.01.15,2010.01.19]".format(tb=tbName1))
        assert_frame_equal(rs, tmp.toDF())

    def test_loadTableBySQL_disk_unpartitioned(self):
        dbPath = DBInfo.diskDBName
        tbName1 = DBInfo.table1
        create_disk_unpartitioned_db()
        with self.assertRaises(RuntimeError):
            self.s.loadTableBySQL(tableName=tbName1, dbPath=dbPath,
                                    sql="select * from {tb} where date in [2010.01.05,2010.01.15,2010.01.19]".format(tb=tbName1))

    def test_loadTableBySQL_disk_range(self):
        dbPath = DBInfo.diskDBName
        tbName1 = DBInfo.table1
        create_disk_range_db()
        rs = self.s.run("select * from loadTable('{db}','{tb}') where date in [2010.01.05,2010.01.15,2010.01.19]".format(db=dbPath, tb=tbName1))
        tmp = self.s.loadTableBySQL(tableName=tbName1, dbPath=dbPath,
                                    sql="select * from {tb} where date in [2010.01.05,2010.01.15,2010.01.19]".format(tb=tbName1))
        assert_frame_equal(rs, tmp.toDF())

    def test_loadTableBySQL_disk_hash(self):
        dbPath = DBInfo.diskDBName
        tbName1 = DBInfo.table1
        create_disk_hash_db()
        rs = self.s.run("select * from loadTable('{db}','{tb}') where date in [2010.01.05,2010.01.15,2010.01.19]".format(db=dbPath, tb=tbName1))
        tmp = self.s.loadTableBySQL(tableName=tbName1, dbPath=dbPath,
                                    sql="select * from {tb} where date in [2010.01.05,2010.01.15,2010.01.19]".format(tb=tbName1))
        assert_frame_equal(rs, tmp.toDF())

    def test_loadTableBySQL_disk_value(self):
        dbPath = DBInfo.diskDBName
        tbName1 = DBInfo.table1
        create_disk_value_db()
        rs = self.s.run("select * from loadTable('{db}','{tb}') where date in [2010.01.05,2010.01.15,2010.01.19]".format(db=dbPath, tb=tbName1))
        tmp = self.s.loadTableBySQL(tableName=tbName1, dbPath=dbPath,
                                    sql="select * from {tb} where date in [2010.01.05,2010.01.15,2010.01.19]".format(tb=tbName1))
        assert_frame_equal(rs, tmp.toDF())

    def test_loadTableBySQL_disk_list(self):
        dbPath = DBInfo.diskDBName
        tbName1 = DBInfo.table1
        create_disk_list_db()
        rs = self.s.run("select * from loadTable('{db}','{tb}') where date in [2010.01.05,2010.01.15,2010.01.19]".format(db=dbPath, tb=tbName1))
        tmp = self.s.loadTableBySQL(tableName=tbName1, dbPath=dbPath,
                                    sql="select * from {tb} where date in [2010.01.05,2010.01.15,2010.01.19]".format(tb=tbName1))
        assert_frame_equal(rs, tmp.toDF())

    def test_loadTableBySQL_disk_compo_range_range(self):
        dbPath = DBInfo.diskDBName
        tbName1 = DBInfo.table1
        create_disk_compo_range_range_db()
        rs = self.s.run("select * from loadTable('{db}','{tb}') where date in [2010.01.05,2010.01.15,2010.01.19]".format(db=dbPath, tb=tbName1))
        tmp = self.s.loadTableBySQL(tableName=tbName1, dbPath=dbPath,
                                    sql="select * from {tb} where date in [2010.01.05,2010.01.15,2010.01.19]".format(tb=tbName1))
        assert_frame_equal(rs, tmp.toDF())

    def test_loadTableBySQL_disk_compo_range_hash(self):
        dbPath = DBInfo.diskDBName
        tbName1 = DBInfo.table1
        create_disk_compo_range_hash_db()
        rs = self.s.run("select * from loadTable('{db}','{tb}') where date in [2010.01.05,2010.01.15,2010.01.19]".format(db=dbPath, tb=tbName1))
        tmp = self.s.loadTableBySQL(tableName=tbName1, dbPath=dbPath,
                                    sql="select * from {tb} where date in [2010.01.05,2010.01.15,2010.01.19]".format(tb=tbName1))
        assert_frame_equal(rs, tmp.toDF())

    def test_loadTableBySQL_disk_compo_range_value(self):
        dbPath = DBInfo.diskDBName
        tbName1 = DBInfo.table1
        create_disk_compo_range_value_db()
        rs = self.s.run("select * from loadTable('{db}','{tb}') where date in [2010.01.05,2010.01.15,2010.01.19]".format(db=dbPath, tb=tbName1))
        tmp = self.s.loadTableBySQL(tableName=tbName1, dbPath=dbPath,
                                    sql="select * from {tb} where date in [2010.01.05,2010.01.15,2010.01.19]".format(tb=tbName1))
        assert_frame_equal(rs, tmp.toDF())

    def test_loadTableBySQL_disk_compo_range_list(self):
        dbPath = DBInfo.diskDBName
        tbName1 = DBInfo.table1
        create_disk_compo_range_list_db()
        rs = self.s.run("select * from loadTable('{db}','{tb}') where date in [2010.01.05,2010.01.15,2010.01.19]".format(db=dbPath, tb=tbName1))
        tmp = self.s.loadTableBySQL(tableName=tbName1, dbPath=dbPath,
                                    sql="select * from {tb} where date in [2010.01.05,2010.01.15,2010.01.19]".format(tb=tbName1))
        assert_frame_equal(rs, tmp.toDF())

    def test_loadTableBySQL_disk_compo_range_hash_list(self):
        dbPath = DBInfo.diskDBName
        tbName1 = DBInfo.table1
        create_disk_compo_range_hash_list_db()
        rs = self.s.run("select * from loadTable('{db}','{tb}') where date in [2010.01.05,2010.01.15,2010.01.19]".format(db=dbPath, tb=tbName1))
        tmp = self.s.loadTableBySQL(tableName=tbName1, dbPath=dbPath,
                                    sql="select * from {tb} where date in [2010.01.05,2010.01.15,2010.01.19]".format(tb=tbName1))
        assert_frame_equal(rs, tmp.toDF())

    def test_loadTableBySQL_disk_compo_range_value_list(self):
        dbPath = DBInfo.diskDBName
        tbName1 = DBInfo.table1
        create_disk_compo_range_value_list_db()
        rs = self.s.run("select * from loadTable('{db}','{tb}') where date in [2010.01.05,2010.01.15,2010.01.19]".format(db=dbPath, tb=tbName1))
        tmp = self.s.loadTableBySQL(tableName=tbName1, dbPath=dbPath,
                                    sql="select * from {tb} where date in [2010.01.05,2010.01.15,2010.01.19]".format(tb=tbName1))
        assert_frame_equal(rs, tmp.toDF())

    def test_loadTableBySQL_paramete(self):
        dbPath = DBInfo.dfsDBName
        tbName1 = DBInfo.table1
        create_dfs_value_db()
        with self.assertRaises(TypeError):
            tmp = self.s.loadTableBySQL(tableName_ERROR=tbName1, dbPath=dbPath,
                                    sql="select * from {tb} where date in [2010.01.05,2010.01.15,2010.01.19]".format(tb=tbName1))
        with self.assertRaises(TypeError):
            tmp = self.s.loadTableBySQL(tableName=tbName1, dbPath_ERROR=dbPath,
                                    sql="select * from {tb} where date in [2010.01.05,2010.01.15,2010.01.19]".format(tb=tbName1))
        with self.assertRaises(TypeError):
            tmp = self.s.loadTableBySQL(tableName=tbName1, dbPath=dbPath,
                                    sql_ERROR="select * from {tb} where date in [2010.01.05,2010.01.15,2010.01.19]".format(tb=tbName1))
        tmp = self.s.loadTableBySQL(tableName=tbName1, dbPath=dbPath,
                                    sql="select * from {tb} where date in [2010.01.05,2010.01.15,2010.01.19]".format(tb=tbName1))

if __name__ == '__main__':
    unittest.main()
