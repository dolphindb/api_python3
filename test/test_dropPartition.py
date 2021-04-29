import unittest
import dolphindb as ddb
from pandas.testing import assert_frame_equal
from setup import HOST, PORT


class DBInfo:
    dfsDBName = 'dfs://testDropPartition'
    table1 = 'tb1'
    table2 = 'tb2'
    tableRows = 10000


def create_dfs_range_db():
    s = ddb.session()
    s.connect(HOST, PORT, "admin", "123456")
    ddb_script = '''
    login('admin','123456')
    n={tbRows}
    dbPath='{db}'
    if(existsDatabase(dbPath))
        dropDatabase(dbPath)
    db=database(dbPath,RANGE,0..10*(n/10)+1)
    tdata=table(sort(take(2010.01.01..2010.12.31, n)) as date, 1..n as id,take(`AMD`QWE`CES`DOP`ASZ`FSD`BBVC`AWQ`DS, n) as sym,rand(100,n) as val)
    db.createPartitionedTable(tdata,`{tb1},`id).append!(tdata)
    db.createPartitionedTable(tdata,`{tb2},`id).append!(tdata)
    '''.format(db=DBInfo.dfsDBName, tb1=DBInfo.table1, tb2=DBInfo.table2, tbRows=DBInfo.tableRows)
    s.run(ddb_script)
    s.close()


def create_dfs_hash_db():
    s = ddb.session()
    s.connect(HOST, PORT, "admin", "123456")
    ddb_script = '''
    login('admin','123456')
    n={tbRows}
    dbPath='{db}'
    if(existsDatabase(dbPath))
        dropDatabase(dbPath)
    db=database(dbPath,HASH,[INT,10])
    tdata=table(sort(take(2010.01.01..2010.12.31, n)) as date, take(1..10,n) as id,take(`AMD`QWE`CES`DOP`ASZ`FSD`BBVC`AWQ`DS, n) as sym,rand(100,n) as val)
    db.createPartitionedTable(tdata,`{tb1},`id).append!(tdata)
    db.createPartitionedTable(tdata,`{tb2},`id).append!(tdata)
    '''.format(db=DBInfo.dfsDBName, tb1=DBInfo.table1, tb2=DBInfo.table2, tbRows=DBInfo.tableRows)
    s.run(ddb_script)
    s.close()


def create_dfs_value_db():
    s = ddb.session()
    s.connect(HOST, PORT, "admin", "123456")
    ddb_script = '''
    login('admin','123456')
    n={tbRows}
    dbPath='{db}'
    if(existsDatabase(dbPath))
        dropDatabase(dbPath)
    db=database(dbPath,VALUE,2010.01.01..2010.01.30)
    tdata=table(sort(take(2010.01.01..2010.01.30, n)) as date, take(1..10,n) as id,take(`AMD`QWE`CES`DOP`ASZ`FSD`BBVC`AWQ`DS, n) as sym,rand(100,n) as val)
    db.createPartitionedTable(tdata,`{tb1},`date).append!(tdata)
    db.createPartitionedTable(tdata,`{tb2},`date).append!(tdata)
    '''.format(db=DBInfo.dfsDBName, tb1=DBInfo.table1, tb2=DBInfo.table2, tbRows=DBInfo.tableRows)
    s.run(ddb_script)
    s.close()


def create_dfs_list_db():
    s = ddb.session()
    s.connect(HOST, PORT, "admin", "123456")
    ddb_script = '''
    login('admin','123456')
    n={tbRows}
    dbPath='{db}'
    if(existsDatabase(dbPath))
        dropDatabase(dbPath)
    db=database(dbPath,LIST,[`AMD`QWE`CES,`DOP`ASZ,`FSD`BBVC,`AWQ`DS])
    tdata=table(sort(take(2010.01.01..2010.12.31, n)) as date, take(1..10,n) as id,take(`AMD`QWE`CES`DOP`ASZ`FSD`BBVC`AWQ`DS, n) as sym,rand(100,n) as val)
    db.createPartitionedTable(tdata,`{tb1},`sym).append!(tdata)
    db.createPartitionedTable(tdata,`{tb2},`sym).append!(tdata)
    '''.format(db=DBInfo.dfsDBName, tb1=DBInfo.table1, tb2=DBInfo.table2, tbRows=DBInfo.tableRows)
    s.run(ddb_script)
    s.close()


def create_dfs_compo_range_range_db():
    s = ddb.session()
    s.connect(HOST, PORT, "admin", "123456")
    ddb_script = '''
    login('admin','123456')
    n={tbRows}
    dbPath='{db}'
    if(existsDatabase(dbPath))
        dropDatabase(dbPath)
    db1=database('',RANGE,2010.01M+0..12)
    db2=database('',RANGE,1 3 5 7 9 11)
    db=database(dbPath,COMPO,[db1,db2])
    tdata=table(sort(take(2010.01.01..2010.12.31, n)) as date, take(1..10,n) as id,take(`AMD`QWE`CES`DOP`ASZ`FSD`BBVC`AWQ`DS, n) as sym,rand(100,n) as val)
    db.createPartitionedTable(tdata,`{tb1},`date`id).append!(tdata)
    db.createPartitionedTable(tdata,`{tb2},`date`id).append!(tdata)
    '''.format(db=DBInfo.dfsDBName, tb1=DBInfo.table1, tb2=DBInfo.table2, tbRows=DBInfo.tableRows)
    s.run(ddb_script)
    s.close()


def create_dfs_compo_range_hash_db():
    s = ddb.session()
    s.connect(HOST, PORT, "admin", "123456")
    ddb_script = '''
    login('admin','123456')
    n={tbRows}
    dbPath='{db}'
    if(existsDatabase(dbPath))
        dropDatabase(dbPath)
    db1=database('',RANGE,2010.01M+0..12)
    db2=database('',HASH,[INT,10])
    db=database(dbPath,COMPO,[db1,db2])
    tdata=table(sort(take(2010.01.01..2010.12.31, n)) as date, take(1..10,n) as id,take(`AMD`QWE`CES`DOP`ASZ`FSD`BBVC`AWQ`DS, n) as sym,rand(100,n) as val)
    db.createPartitionedTable(tdata,`{tb1},`date`id).append!(tdata)
    db.createPartitionedTable(tdata,`{tb2},`date`id).append!(tdata)
    '''.format(db=DBInfo.dfsDBName, tb1=DBInfo.table1, tb2=DBInfo.table2, tbRows=DBInfo.tableRows)
    s.run(ddb_script)
    s.close()


def create_dfs_compo_range_value_db():
    s = ddb.session()
    s.connect(HOST, PORT, "admin", "123456")
    ddb_script = '''
    login('admin','123456')
    n={tbRows}
    dbPath='{db}'
    if(existsDatabase(dbPath))
        dropDatabase(dbPath)
    db1=database('',RANGE,2010.01M+0..12)
    db2=database('',VALUE,1..10)
    db=database(dbPath,COMPO,[db1,db2])
    tdata=table(sort(take(2010.01.01..2010.12.31, n)) as date, take(1..10,n) as id,take(`AMD`QWE`CES`DOP`ASZ`FSD`BBVC`AWQ`DS, n) as sym,rand(100,n) as val)
    db.createPartitionedTable(tdata,`{tb1},`date`id).append!(tdata)
    db.createPartitionedTable(tdata,`{tb2},`date`id).append!(tdata)
    '''.format(db=DBInfo.dfsDBName, tb1=DBInfo.table1, tb2=DBInfo.table2, tbRows=DBInfo.tableRows)
    s.run(ddb_script)
    s.close()


def create_dfs_compo_range_list_db():
    s = ddb.session()
    s.connect(HOST, PORT, "admin", "123456")
    ddb_script = '''
    login('admin','123456')
    n={tbRows}
    dbPath='{db}'
    if(existsDatabase(dbPath))
        dropDatabase(dbPath)
    db1=database('',RANGE,2010.01M+0..12)
    db2=database('',LIST,[`AMD`QWE`CES,`DOP`ASZ,`FSD`BBVC,`AWQ`DS])
    db=database(dbPath,COMPO,[db1,db2])
    tdata=table(sort(take(2010.01.01..2010.12.31, n)) as date, take(1..10,n) as id,take(`AMD`QWE`CES`DOP`ASZ`FSD`BBVC`AWQ`DS, n) as sym,rand(100,n) as val)
    db.createPartitionedTable(tdata,`{tb1},`date`sym).append!(tdata)
    db.createPartitionedTable(tdata,`{tb2},`date`sym).append!(tdata)
    '''.format(db=DBInfo.dfsDBName, tb1=DBInfo.table1, tb2=DBInfo.table2, tbRows=DBInfo.tableRows)
    s.run(ddb_script)
    s.close()


def create_dfs_compo_range_hash_list_db():
    s = ddb.session()
    s.connect(HOST, PORT, "admin", "123456")
    ddb_script = '''
    login('admin','123456')
    n={tbRows}
    dbPath='{db}'
    if(existsDatabase(dbPath))
        dropDatabase(dbPath)
    db1=database('',RANGE,2010.01M+0..12)
    db2=database('',HASH,[INT,10])
    db3=database('',LIST,[`AMD`QWE`CES,`DOP`ASZ,`FSD`BBVC,`AWQ`DS])
    db=database(dbPath,COMPO,[db1,db2,db3])
    tdata=table(sort(take(2010.01.01..2010.12.31, n)) as date, take(1..10,n) as id,take(`AMD`QWE`CES`DOP`ASZ`FSD`BBVC`AWQ`DS, n) as sym,rand(100,n) as val)
    db.createPartitionedTable(tdata,`{tb1},`date`id`sym).append!(tdata)
    db.createPartitionedTable(tdata,`{tb2},`date`id`sym).append!(tdata)
    '''.format(db=DBInfo.dfsDBName, tb1=DBInfo.table1, tb2=DBInfo.table2, tbRows=DBInfo.tableRows)
    s.run(ddb_script)
    s.close()


def create_dfs_compo_range_value_list_db():
    s = ddb.session()
    s.connect(HOST, PORT, "admin", "123456")
    ddb_script = '''
    login('admin','123456')
    n={tbRows}
    dbPath='{db}'
    if(existsDatabase(dbPath))
        dropDatabase(dbPath)
    db1=database('',RANGE,2010.01M+0..12)
    db2=database('',VALUE,1..10)
    db3=database('',LIST,[`AMD`QWE`CES,`DOP`ASZ,`FSD`BBVC,`AWQ`DS])
    db=database(dbPath,COMPO,[db1,db2,db3])
    tdata=table(sort(take(2010.01.01..2010.12.31, n)) as date, take(1..10,n) as id,take(`AMD`QWE`CES`DOP`ASZ`FSD`BBVC`AWQ`DS, n) as sym,rand(100,n) as val)
    db.createPartitionedTable(tdata,`{tb1},`date`id`sym).append!(tdata)
    db.createPartitionedTable(tdata,`{tb2},`date`id`sym).append!(tdata)
    '''.format(db=DBInfo.dfsDBName, tb1=DBInfo.table1, tb2=DBInfo.table2, tbRows=DBInfo.tableRows)
    s.run(ddb_script)
    s.close()


def loadTB(dbPath, tbName, where=""):
    s = ddb.session()
    s.connect(HOST, PORT, "admin", "123456")
    if where == "":
        return s.run("select * from loadTable('{db}', '{tb}') ".format(db=dbPath, tb=tbName))
    else:
        return s.run("select * from loadTable('{db}', '{tb}') ".format(db=dbPath, tb=tbName) + where)


class DropPartitionTest(unittest.TestCase):
    @classmethod
    def setUp(cls):
        cls.s = ddb.session()
        cls.s.connect(HOST, PORT, "admin", "123456")
        dbPath = DBInfo.dfsDBName
        script = """
        if(existsDatabase('{dbPath}'))
            dropDatabase('{dbPath}')
        """.format(dbPath=dbPath)
        cls.s.run(script)

    @classmethod
    def tearDown(cls):
        cls.s = ddb.session()
        cls.s.connect(HOST, PORT, "admin", "123456")
        dbPath = DBInfo.dfsDBName
        script = """
        if(existsDatabase('{dbPath}'))
            dropDatabase('{dbPath}')
        """.format(dbPath=dbPath)
        cls.s.run(script)

    def test_dropPartition_dfs_range_drop_single(self):
        dbPath = DBInfo.dfsDBName
        tbName1 = DBInfo.table1
        tbName2 = DBInfo.table2
        bound = str(DBInfo.tableRows//10 + 1)

        # tb1 dropPartition
        create_dfs_range_db()
        origin = loadTB(dbPath, tbName1)
        rs = loadTB(dbPath, tbName1, "where id>=" + bound)
        assert_frame_equal(loadTB(dbPath, tbName2), origin)
        assert_frame_equal(loadTB(dbPath, tbName1), origin)
        self.s.dropPartition(dbPath, ["'/1_" + bound + "'"], tbName1)
        assert_frame_equal(loadTB(dbPath, tbName1), rs)
        assert_frame_equal(loadTB(dbPath, tbName2), origin)

        # tb2 dropPartition
        create_dfs_range_db()
        origin = loadTB(dbPath, tbName1)
        rs = loadTB(dbPath, tbName1, "where id>=" + bound)
        assert_frame_equal(loadTB(dbPath, tbName1), origin)
        assert_frame_equal(loadTB(dbPath, tbName2), origin)
        self.s.dropPartition(dbPath, ["'/1_" + bound + "'"], tbName2)
        assert_frame_equal(loadTB(dbPath, tbName2), rs)
        assert_frame_equal(loadTB(dbPath, tbName1), origin)

        # both dropPartition
        create_dfs_range_db()
        origin = loadTB(dbPath, tbName1)
        rs = loadTB(dbPath, tbName1, "where id>=" + bound)
        assert_frame_equal(loadTB(dbPath, tbName1), origin)
        assert_frame_equal(loadTB(dbPath, tbName2), origin)
        self.s.dropPartition(dbPath, ["'/1_" + bound + "'"])
        assert_frame_equal(loadTB(dbPath, tbName1), rs)
        assert_frame_equal(loadTB(dbPath, tbName2), rs)

    def test_dropPartition_dfs_range_drop_multiple(self):
        dbPath = DBInfo.dfsDBName
        tbName1 = DBInfo.table1
        tbName2 = DBInfo.table2
        bound = str(DBInfo.tableRows*4 // 10 + 1)
        partitions = ["'/1_1001'", "'/1001_2001'", "'/2001_3001'", "'/3001_4001'"]
        # should be altered with DBInfo.tableRows

        # tb1 dropPartition
        create_dfs_range_db()
        origin = loadTB(dbPath, tbName1)
        rs = loadTB(dbPath, tbName1, "where id>=" + bound)
        assert_frame_equal(loadTB(dbPath, tbName2), origin)
        assert_frame_equal(loadTB(dbPath, tbName1), origin)
        self.s.dropPartition(dbPath, partitions, tbName1)
        assert_frame_equal(loadTB(dbPath, tbName1), rs)
        assert_frame_equal(loadTB(dbPath, tbName2), origin)

        # tb2 dropPartition
        create_dfs_range_db()
        origin = loadTB(dbPath, tbName1)
        rs = loadTB(dbPath, tbName1, "where id>=" + bound)
        assert_frame_equal(loadTB(dbPath, tbName1), origin)
        assert_frame_equal(loadTB(dbPath, tbName2), origin)
        self.s.dropPartition(dbPath, partitions, tbName2)
        assert_frame_equal(loadTB(dbPath, tbName2), rs)
        assert_frame_equal(loadTB(dbPath, tbName1), origin)

        # both dropPartition
        create_dfs_range_db()
        origin = loadTB(dbPath, tbName1)
        rs = loadTB(dbPath, tbName1, "where id>=" + bound)
        assert_frame_equal(loadTB(dbPath, tbName1), origin)
        assert_frame_equal(loadTB(dbPath, tbName2), origin)
        self.s.dropPartition(dbPath, partitions)
        assert_frame_equal(loadTB(dbPath, tbName1), rs)
        assert_frame_equal(loadTB(dbPath, tbName2), rs)

    def test_dropPartition_dfs_hash_drop_single(self):
        dbPath = DBInfo.dfsDBName
        tbName1 = DBInfo.table1
        tbName2 = DBInfo.table2

        # tb1 dropPartition
        create_dfs_hash_db()
        origin = loadTB(dbPath, tbName1)
        rs = loadTB(dbPath, tbName1, "where id!=10")
        assert_frame_equal(loadTB(dbPath, tbName2), origin)
        assert_frame_equal(loadTB(dbPath, tbName1), origin)
        self.s.dropPartition(dbPath, ['0'], tbName1)
        assert_frame_equal(loadTB(dbPath, tbName1), rs)
        assert_frame_equal(loadTB(dbPath, tbName2), origin)

        # tb2 dropPartition
        create_dfs_hash_db()
        origin = loadTB(dbPath, tbName1)
        rs = loadTB(dbPath, tbName1, "where id!=10")
        assert_frame_equal(loadTB(dbPath, tbName1), origin)
        assert_frame_equal(loadTB(dbPath, tbName2), origin)
        self.s.dropPartition(dbPath, ['0'], tbName2)
        assert_frame_equal(loadTB(dbPath, tbName2), rs)
        assert_frame_equal(loadTB(dbPath, tbName1), origin)

        # both dropPartition
        create_dfs_hash_db()
        origin = loadTB(dbPath, tbName1)
        rs = loadTB(dbPath, tbName1, "where id!=10")
        assert_frame_equal(loadTB(dbPath, tbName1), origin)
        assert_frame_equal(loadTB(dbPath, tbName2), origin)
        self.s.dropPartition(dbPath, ['0'])
        assert_frame_equal(loadTB(dbPath, tbName1), rs)
        assert_frame_equal(loadTB(dbPath, tbName2), rs)

    def test_dropPartition_dfs_hash_drop_multiple(self):
        dbPath = DBInfo.dfsDBName
        tbName1 = DBInfo.table1
        tbName2 = DBInfo.table2

        # tb1 dropPartition
        create_dfs_hash_db()
        origin = loadTB(dbPath, tbName1)
        rs = loadTB(dbPath, tbName1, "where id in 2..9")
        assert_frame_equal(loadTB(dbPath, tbName2), origin)
        assert_frame_equal(loadTB(dbPath, tbName1), origin)
        self.s.dropPartition(dbPath, ['0', '1'], tbName1)
        assert_frame_equal(loadTB(dbPath, tbName1), rs)
        assert_frame_equal(loadTB(dbPath, tbName2), origin)

        # tb2 dropPartition
        create_dfs_hash_db()
        origin = loadTB(dbPath, tbName1)
        rs = loadTB(dbPath, tbName1, "where id in 2..9")
        assert_frame_equal(loadTB(dbPath, tbName1), origin)
        assert_frame_equal(loadTB(dbPath, tbName2), origin)
        self.s.dropPartition(dbPath, ['0', '1'], tbName2)
        assert_frame_equal(loadTB(dbPath, tbName2), rs)
        assert_frame_equal(loadTB(dbPath, tbName1), origin)

        # both dropPartition
        create_dfs_hash_db()
        origin = loadTB(dbPath, tbName1)
        rs = loadTB(dbPath, tbName1, "where id in 2..9")
        assert_frame_equal(loadTB(dbPath, tbName1), origin)
        assert_frame_equal(loadTB(dbPath, tbName2), origin)
        self.s.dropPartition(dbPath, ['0', '1'])
        assert_frame_equal(loadTB(dbPath, tbName1), rs)
        assert_frame_equal(loadTB(dbPath, tbName2), rs)

    def test_dropPartition_dfs_value_drop_single(self):
        dbPath = DBInfo.dfsDBName
        tbName1 = DBInfo.table1
        tbName2 = DBInfo.table2

        # tb1 dropPartition
        create_dfs_value_db()
        origin = loadTB(dbPath, tbName1)
        rs = loadTB(dbPath, tbName1, "where date!=2010.01.01")
        assert_frame_equal(loadTB(dbPath, tbName2), origin)
        assert_frame_equal(loadTB(dbPath, tbName1), origin)
        self.s.dropPartition(dbPath, ['2010.01.01'], tbName1)
        assert_frame_equal(loadTB(dbPath, tbName1), rs)
        assert_frame_equal(loadTB(dbPath, tbName2), origin)

        # tb2 dropPartition
        create_dfs_value_db()
        origin = loadTB(dbPath, tbName1)
        rs = loadTB(dbPath, tbName1, "where date!=2010.01.01")
        assert_frame_equal(loadTB(dbPath, tbName1), origin)
        assert_frame_equal(loadTB(dbPath, tbName2), origin)
        self.s.dropPartition(dbPath, ['2010.01.01'], tbName2)
        assert_frame_equal(loadTB(dbPath, tbName2), rs)
        assert_frame_equal(loadTB(dbPath, tbName1), origin)

        # both dropPartition
        create_dfs_value_db()
        origin = loadTB(dbPath, tbName1)
        rs = loadTB(dbPath, tbName1, "where date!=2010.01.01")
        assert_frame_equal(loadTB(dbPath, tbName1), origin)
        assert_frame_equal(loadTB(dbPath, tbName2), origin)
        self.s.dropPartition(dbPath, ['2010.01.01'])
        assert_frame_equal(loadTB(dbPath, tbName1), rs)
        assert_frame_equal(loadTB(dbPath, tbName2), rs)

    def test_dropPartition_dfs_value_drop_multiple(self):
        dbPath = DBInfo.dfsDBName
        tbName1 = DBInfo.table1
        tbName2 = DBInfo.table2

        # tb1 dropPartition
        create_dfs_value_db()
        origin = loadTB(dbPath, tbName1)
        rs = loadTB(dbPath, tbName1, "where !in(date,2010.01.01+[0,7,14,21])")
        assert_frame_equal(loadTB(dbPath, tbName2), origin)
        assert_frame_equal(loadTB(dbPath, tbName1), origin)
        self.s.dropPartition(dbPath, ['2010.01.01', '2010.01.08', '2010.01.15', '2010.01.22'], tbName1)
        assert_frame_equal(loadTB(dbPath, tbName1), rs)
        assert_frame_equal(loadTB(dbPath, tbName2), origin)

        # tb2 dropPartition
        create_dfs_value_db()
        origin = loadTB(dbPath, tbName1)
        rs = loadTB(dbPath, tbName1, "where !in(date,2010.01.01+[0,7,14,21])")
        assert_frame_equal(loadTB(dbPath, tbName1), origin)
        assert_frame_equal(loadTB(dbPath, tbName2), origin)
        self.s.dropPartition(dbPath, ['2010.01.01', '2010.01.08', '2010.01.15', '2010.01.22'], tbName2)
        assert_frame_equal(loadTB(dbPath, tbName2), rs)
        assert_frame_equal(loadTB(dbPath, tbName1), origin)

        # both dropPartition
        create_dfs_value_db()
        origin = loadTB(dbPath, tbName1)
        rs = loadTB(dbPath, tbName1, "where !in(date,2010.01.01+[0,7,14,21])")
        assert_frame_equal(loadTB(dbPath, tbName1), origin)
        assert_frame_equal(loadTB(dbPath, tbName2), origin)
        self.s.dropPartition(dbPath, ['2010.01.01', '2010.01.08', '2010.01.15', '2010.01.22'])
        assert_frame_equal(loadTB(dbPath, tbName1), rs)
        assert_frame_equal(loadTB(dbPath, tbName2), rs)

    def test_dropPartition_dfs_list_drop_single(self):
        dbPath = DBInfo.dfsDBName
        tbName1 = DBInfo.table1
        tbName2 = DBInfo.table2

        # tb1 dropPartition
        create_dfs_list_db()
        origin = loadTB(dbPath, tbName1)
        rs = loadTB(dbPath, tbName1, "where !in(sym,`AMD`QWE`CES)")
        assert_frame_equal(loadTB(dbPath, tbName2), origin)
        assert_frame_equal(loadTB(dbPath, tbName1), origin)
        self.s.dropPartition(dbPath, ["'/List0'"], tbName1)
        assert_frame_equal(loadTB(dbPath, tbName1), rs)
        assert_frame_equal(loadTB(dbPath, tbName2), origin)

        # tb2 dropPartition
        create_dfs_list_db()
        origin = loadTB(dbPath, tbName1)
        rs = loadTB(dbPath, tbName1, "where !in(sym,`AMD`QWE`CES)")
        assert_frame_equal(loadTB(dbPath, tbName1), origin)
        assert_frame_equal(loadTB(dbPath, tbName2), origin)
        self.s.dropPartition(dbPath, ["'/List0'"], tbName2)
        assert_frame_equal(loadTB(dbPath, tbName2), rs)
        assert_frame_equal(loadTB(dbPath, tbName1), origin)

        # both dropPartition
        create_dfs_list_db()
        origin = loadTB(dbPath, tbName1)
        rs = loadTB(dbPath, tbName1, "where !in(sym,`AMD`QWE`CES)")
        assert_frame_equal(loadTB(dbPath, tbName1), origin)
        assert_frame_equal(loadTB(dbPath, tbName2), origin)
        self.s.dropPartition(dbPath, ["'/List0'"])
        assert_frame_equal(loadTB(dbPath, tbName1), rs)
        assert_frame_equal(loadTB(dbPath, tbName2), rs)

    def test_dropPartition_dfs_list_drop_multiple(self):
        dbPath = DBInfo.dfsDBName
        tbName1 = DBInfo.table1
        tbName2 = DBInfo.table2

        # tb1 dropPartition
        create_dfs_list_db()
        origin = loadTB(dbPath, tbName1)
        rs = loadTB(dbPath, tbName1, "where !in(sym,`DOP`ASZ`FSD`BBVC)")
        assert_frame_equal(loadTB(dbPath, tbName2), origin)
        assert_frame_equal(loadTB(dbPath, tbName1), origin)
        self.s.dropPartition(dbPath, ["'/List1'", "'/List2'"], tbName1)
        assert_frame_equal(loadTB(dbPath, tbName1), rs)
        assert_frame_equal(loadTB(dbPath, tbName2), origin)

        # tb2 dropPartition
        create_dfs_list_db()
        origin = loadTB(dbPath, tbName1)
        rs = loadTB(dbPath, tbName1, "where !in(sym,`DOP`ASZ`FSD`BBVC)")
        assert_frame_equal(loadTB(dbPath, tbName1), origin)
        assert_frame_equal(loadTB(dbPath, tbName2), origin)
        self.s.dropPartition(dbPath, ["'/List1'", "'/List2'"], tbName2)
        assert_frame_equal(loadTB(dbPath, tbName2), rs)
        assert_frame_equal(loadTB(dbPath, tbName1), origin)

        # both dropPartition
        create_dfs_list_db()
        origin = loadTB(dbPath, tbName1)
        rs = loadTB(dbPath, tbName1, "where !in(sym,`DOP`ASZ`FSD`BBVC)")
        assert_frame_equal(loadTB(dbPath, tbName1), origin)
        assert_frame_equal(loadTB(dbPath, tbName2), origin)
        self.s.dropPartition(dbPath, ["'/List1'", "'/List2'"])
        assert_frame_equal(loadTB(dbPath, tbName1), rs)
        assert_frame_equal(loadTB(dbPath, tbName2), rs)

    def test_dropPartition_dfs_compo2_range_range_drop_level1_single(self):
        dbPath = DBInfo.dfsDBName
        tbName1 = DBInfo.table1
        tbName2 = DBInfo.table2

        # tb1 dropPartition
        create_dfs_compo_range_range_db()
        origin = loadTB(dbPath, tbName1)
        rs = loadTB(dbPath, tbName1, "where date>=2010.02.01")
        assert_frame_equal(loadTB(dbPath, tbName2), origin)
        assert_frame_equal(loadTB(dbPath, tbName1), origin)
        self.s.dropPartition(dbPath, ["2010.01.01"], tbName1)
        assert_frame_equal(loadTB(dbPath, tbName1), rs)
        assert_frame_equal(loadTB(dbPath, tbName2), origin)

        # tb2 dropPartition
        create_dfs_compo_range_range_db()
        origin = loadTB(dbPath, tbName1)
        rs = loadTB(dbPath, tbName1, "where date>=2010.02.01")
        assert_frame_equal(loadTB(dbPath, tbName1), origin)
        assert_frame_equal(loadTB(dbPath, tbName2), origin)
        self.s.dropPartition(dbPath, ["2010.01.01"], tbName2)
        assert_frame_equal(loadTB(dbPath, tbName2), rs)
        assert_frame_equal(loadTB(dbPath, tbName1), origin)

        # both dropPartition
        create_dfs_compo_range_range_db()
        origin = loadTB(dbPath, tbName1)
        rs = loadTB(dbPath, tbName1, "where date>=2010.02.01")
        assert_frame_equal(loadTB(dbPath, tbName1), origin)
        assert_frame_equal(loadTB(dbPath, tbName2), origin)
        self.s.dropPartition(dbPath, ["2010.01.01"])
        assert_frame_equal(loadTB(dbPath, tbName1), rs)
        assert_frame_equal(loadTB(dbPath, tbName2), rs)

    def test_dropPartition_dfs_compo2_range_range_drop_level1_multiple(self):
        dbPath = DBInfo.dfsDBName
        tbName1 = DBInfo.table1
        tbName2 = DBInfo.table2

        # tb1 dropPartition
        create_dfs_compo_range_range_db()
        origin = loadTB(dbPath, tbName1)
        rs = loadTB(dbPath, tbName1, "where date>=2010.03.01")
        assert_frame_equal(loadTB(dbPath, tbName2), origin)
        assert_frame_equal(loadTB(dbPath, tbName1), origin)
        self.s.dropPartition(dbPath, ["2010.01.01", "2010.02.01"], tbName1)
        assert_frame_equal(loadTB(dbPath, tbName1), rs)
        assert_frame_equal(loadTB(dbPath, tbName2), origin)

        # tb2 dropPartition
        create_dfs_compo_range_range_db()
        origin = loadTB(dbPath, tbName1)
        rs = loadTB(dbPath, tbName1, "where date>=2010.03.01")
        assert_frame_equal(loadTB(dbPath, tbName1), origin)
        assert_frame_equal(loadTB(dbPath, tbName2), origin)
        self.s.dropPartition(dbPath, ["2010.01.01", "2010.02.01"], tbName2)
        assert_frame_equal(loadTB(dbPath, tbName2), rs)
        assert_frame_equal(loadTB(dbPath, tbName1), origin)

        # both dropPartition
        create_dfs_compo_range_range_db()
        origin = loadTB(dbPath, tbName1)
        rs = loadTB(dbPath, tbName1, "where date>=2010.03.01")
        assert_frame_equal(loadTB(dbPath, tbName1), origin)
        assert_frame_equal(loadTB(dbPath, tbName2), origin)
        self.s.dropPartition(dbPath, ["2010.01.01", "2010.02.01"])
        assert_frame_equal(loadTB(dbPath, tbName1), rs)
        assert_frame_equal(loadTB(dbPath, tbName2), rs)

    def test_dropPartition_dfs_compo2_range_range_drop_level2_single(self):
        dbPath = DBInfo.dfsDBName
        tbName1 = DBInfo.table1
        tbName2 = DBInfo.table2

        # tb1 dropPartition
        create_dfs_compo_range_range_db()
        origin = loadTB(dbPath, tbName1)
        rs = loadTB(dbPath, tbName1, "where date>=2010.02.01 or id>=3")
        assert_frame_equal(loadTB(dbPath, tbName2), origin)
        assert_frame_equal(loadTB(dbPath, tbName1), origin)
        self.s.dropPartition(dbPath, ["2010.01.01", "1"], tbName1)
        assert_frame_equal(loadTB(dbPath, tbName1), rs)
        assert_frame_equal(loadTB(dbPath, tbName2), origin)

        # tb2 dropPartition
        create_dfs_compo_range_range_db()
        origin = loadTB(dbPath, tbName1)
        rs = loadTB(dbPath, tbName1, "where date>=2010.02.01 or id>=3")
        assert_frame_equal(loadTB(dbPath, tbName1), origin)
        assert_frame_equal(loadTB(dbPath, tbName2), origin)
        self.s.dropPartition(dbPath, ["2010.01.01", "1"], tbName2)
        assert_frame_equal(loadTB(dbPath, tbName2), rs)
        assert_frame_equal(loadTB(dbPath, tbName1), origin)

        # both dropPartition
        create_dfs_compo_range_range_db()
        origin = loadTB(dbPath, tbName1)
        rs = loadTB(dbPath, tbName1, "where date>=2010.02.01 or id>=3")
        assert_frame_equal(loadTB(dbPath, tbName1), origin)
        assert_frame_equal(loadTB(dbPath, tbName2), origin)
        self.s.dropPartition(dbPath, ["2010.01.01", "1"])
        assert_frame_equal(loadTB(dbPath, tbName1), rs)
        assert_frame_equal(loadTB(dbPath, tbName2), rs)

    def test_dropPartition_dfs_compo2_range_range_drop_level2_multiple(self):
        dbPath = DBInfo.dfsDBName
        tbName1 = DBInfo.table1
        tbName2 = DBInfo.table2

        # tb1 dropPartition
        create_dfs_compo_range_range_db()
        origin = loadTB(dbPath, tbName1)
        rs = loadTB(dbPath, tbName1, "where date>=2010.02.01 or !between(id,3:6)")
        assert_frame_equal(loadTB(dbPath, tbName2), origin)
        assert_frame_equal(loadTB(dbPath, tbName1), origin)
        self.s.dropPartition(dbPath, ["2010.01.01", "[3,5]"], tbName1)
        assert_frame_equal(loadTB(dbPath, tbName1), rs)
        assert_frame_equal(loadTB(dbPath, tbName2), origin)

        # tb2 dropPartition
        create_dfs_compo_range_range_db()
        origin = loadTB(dbPath, tbName1)
        rs = loadTB(dbPath, tbName1, "where date>=2010.02.01 or !between(id,3:6)")
        assert_frame_equal(loadTB(dbPath, tbName1), origin)
        assert_frame_equal(loadTB(dbPath, tbName2), origin)
        self.s.dropPartition(dbPath, ["2010.01.01", "[3,5]"], tbName2)
        assert_frame_equal(loadTB(dbPath, tbName2), rs)
        assert_frame_equal(loadTB(dbPath, tbName1), origin)

        # both dropPartition
        create_dfs_compo_range_range_db()
        origin = loadTB(dbPath, tbName1)
        rs = loadTB(dbPath, tbName1, "where date>=2010.02.01 or !between(id,3:6)")
        assert_frame_equal(loadTB(dbPath, tbName1), origin)
        assert_frame_equal(loadTB(dbPath, tbName2), origin)
        self.s.dropPartition(dbPath, ["2010.01.01", "[3,5]"])
        assert_frame_equal(loadTB(dbPath, tbName1), rs)
        assert_frame_equal(loadTB(dbPath, tbName2), rs)

    def test_dropPartition_dfs_compo2_range_range_drop_multiple(self):
        dbPath = DBInfo.dfsDBName
        tbName1 = DBInfo.table1
        tbName2 = DBInfo.table2

        # tb1 dropPartition
        create_dfs_compo_range_range_db()
        origin = loadTB(dbPath, tbName1)
        rs = loadTB(dbPath, tbName1, "where date>=2010.03.01 or !between(id,3:6)")
        assert_frame_equal(loadTB(dbPath, tbName2), origin)
        assert_frame_equal(loadTB(dbPath, tbName1), origin)
        self.s.dropPartition(dbPath, ["[2010.01.01,2010.02.01]", "[3,5]"], tbName1)
        assert_frame_equal(loadTB(dbPath, tbName1), rs)
        assert_frame_equal(loadTB(dbPath, tbName2), origin)

        # tb2 dropPartition
        create_dfs_compo_range_range_db()
        origin = loadTB(dbPath, tbName1)
        rs = loadTB(dbPath, tbName1, "where date>=2010.03.01 or !between(id,3:6)")
        assert_frame_equal(loadTB(dbPath, tbName1), origin)
        assert_frame_equal(loadTB(dbPath, tbName2), origin)
        self.s.dropPartition(dbPath, ["[2010.01.01,2010.02.01]", "[3,5]"], tbName2)
        assert_frame_equal(loadTB(dbPath, tbName2), rs)
        assert_frame_equal(loadTB(dbPath, tbName1), origin)

        # both dropPartition
        create_dfs_compo_range_range_db()
        origin = loadTB(dbPath, tbName1)
        rs = loadTB(dbPath, tbName1, "where date>=2010.03.01 or !between(id,3:6)")
        assert_frame_equal(loadTB(dbPath, tbName1), origin)
        assert_frame_equal(loadTB(dbPath, tbName2), origin)
        self.s.dropPartition(dbPath, ["[2010.01.01,2010.02.01]", "[3,5]"])
        assert_frame_equal(loadTB(dbPath, tbName1), rs)
        assert_frame_equal(loadTB(dbPath, tbName2), rs)

    def test_dropPartition_dfs_compo2_range_hash_drop_level1_single(self):
        dbPath = DBInfo.dfsDBName
        tbName1 = DBInfo.table1
        tbName2 = DBInfo.table2

        # tb1 dropPartition
        create_dfs_compo_range_hash_db()
        origin = loadTB(dbPath, tbName1)
        rs = loadTB(dbPath, tbName1, "where date>=2010.02.01")
        assert_frame_equal(loadTB(dbPath, tbName2), origin)
        assert_frame_equal(loadTB(dbPath, tbName1), origin)
        self.s.dropPartition(dbPath, ["2010.01.01"], tbName1)
        assert_frame_equal(loadTB(dbPath, tbName1), rs)
        assert_frame_equal(loadTB(dbPath, tbName2), origin)

        # tb2 dropPartition
        create_dfs_compo_range_hash_db()
        origin = loadTB(dbPath, tbName1)
        rs = loadTB(dbPath, tbName1, "where date>=2010.02.01")
        assert_frame_equal(loadTB(dbPath, tbName1), origin)
        assert_frame_equal(loadTB(dbPath, tbName2), origin)
        self.s.dropPartition(dbPath, ["2010.01.01"], tbName2)
        assert_frame_equal(loadTB(dbPath, tbName2), rs)
        assert_frame_equal(loadTB(dbPath, tbName1), origin)

        # both dropPartition
        create_dfs_compo_range_hash_db()
        origin = loadTB(dbPath, tbName1)
        rs = loadTB(dbPath, tbName1, "where date>=2010.02.01")
        assert_frame_equal(loadTB(dbPath, tbName1), origin)
        assert_frame_equal(loadTB(dbPath, tbName2), origin)
        self.s.dropPartition(dbPath, ["2010.01.01"])
        assert_frame_equal(loadTB(dbPath, tbName1), rs)
        assert_frame_equal(loadTB(dbPath, tbName2), rs)

    def test_dropPartition_dfs_compo2_range_hash_drop_level1_multiple(self):
        dbPath = DBInfo.dfsDBName
        tbName1 = DBInfo.table1
        tbName2 = DBInfo.table2

        # tb1 dropPartition
        create_dfs_compo_range_hash_db()
        origin = loadTB(dbPath, tbName1)
        rs = loadTB(dbPath, tbName1, "where date>=2010.03.01")
        assert_frame_equal(loadTB(dbPath, tbName2), origin)
        assert_frame_equal(loadTB(dbPath, tbName1), origin)
        self.s.dropPartition(dbPath, ["2010.01.01", "2010.02.01"], tbName1)
        assert_frame_equal(loadTB(dbPath, tbName1), rs)
        assert_frame_equal(loadTB(dbPath, tbName2), origin)

        # tb2 dropPartition
        create_dfs_compo_range_hash_db()
        origin = loadTB(dbPath, tbName1)
        rs = loadTB(dbPath, tbName1, "where date>=2010.03.01")
        assert_frame_equal(loadTB(dbPath, tbName1), origin)
        assert_frame_equal(loadTB(dbPath, tbName2), origin)
        self.s.dropPartition(dbPath, ["2010.01.01", "2010.02.01"], tbName2)
        assert_frame_equal(loadTB(dbPath, tbName2), rs)
        assert_frame_equal(loadTB(dbPath, tbName1), origin)

        # both dropPartition
        create_dfs_compo_range_hash_db()
        origin = loadTB(dbPath, tbName1)
        rs = loadTB(dbPath, tbName1, "where date>=2010.03.01")
        assert_frame_equal(loadTB(dbPath, tbName1), origin)
        assert_frame_equal(loadTB(dbPath, tbName2), origin)
        self.s.dropPartition(dbPath, ["2010.01.01", "2010.02.01"])
        assert_frame_equal(loadTB(dbPath, tbName1), rs)
        assert_frame_equal(loadTB(dbPath, tbName2), rs)

    def test_dropPartition_dfs_compo2_range_hash_drop_level2_single(self):
        dbPath = DBInfo.dfsDBName
        tbName1 = DBInfo.table1
        tbName2 = DBInfo.table2

        # tb1 dropPartition
        create_dfs_compo_range_hash_db()
        origin = loadTB(dbPath, tbName1)
        rs = loadTB(dbPath, tbName1, "where date>=2010.02.01 or id!=1")
        assert_frame_equal(loadTB(dbPath, tbName2), origin)
        assert_frame_equal(loadTB(dbPath, tbName1), origin)
        self.s.dropPartition(dbPath, ["2010.01.01", "1"], tbName1)
        assert_frame_equal(loadTB(dbPath, tbName1), rs)
        assert_frame_equal(loadTB(dbPath, tbName2), origin)

        # tb2 dropPartition
        create_dfs_compo_range_hash_db()
        origin = loadTB(dbPath, tbName1)
        rs = loadTB(dbPath, tbName1, "where date>=2010.02.01 or id!=1")
        assert_frame_equal(loadTB(dbPath, tbName1), origin)
        assert_frame_equal(loadTB(dbPath, tbName2), origin)
        self.s.dropPartition(dbPath, ["2010.01.01", "1"], tbName2)
        assert_frame_equal(loadTB(dbPath, tbName2), rs)
        assert_frame_equal(loadTB(dbPath, tbName1), origin)

        # both dropPartition
        create_dfs_compo_range_hash_db()
        origin = loadTB(dbPath, tbName1)
        rs = loadTB(dbPath, tbName1, "where date>=2010.02.01 or id!=1")
        assert_frame_equal(loadTB(dbPath, tbName1), origin)
        assert_frame_equal(loadTB(dbPath, tbName2), origin)
        self.s.dropPartition(dbPath, ["2010.01.01", "1"])
        assert_frame_equal(loadTB(dbPath, tbName1), rs)
        assert_frame_equal(loadTB(dbPath, tbName2), rs)

    def test_dropPartition_dfs_compo2_range_hash_drop_level2_multiple(self):
        dbPath = DBInfo.dfsDBName
        tbName1 = DBInfo.table1
        tbName2 = DBInfo.table2

        # tb1 dropPartition
        create_dfs_compo_range_hash_db()
        origin = loadTB(dbPath, tbName1)
        rs = loadTB(dbPath, tbName1, "where date>=2010.02.01 or !in(id,[3,5])")
        assert_frame_equal(loadTB(dbPath, tbName2), origin)
        assert_frame_equal(loadTB(dbPath, tbName1), origin)
        self.s.dropPartition(dbPath, ["2010.01.01", "[3,5]"], tbName1)
        assert_frame_equal(loadTB(dbPath, tbName1), rs)
        assert_frame_equal(loadTB(dbPath, tbName2), origin)

        # tb2 dropPartition
        create_dfs_compo_range_hash_db()
        origin = loadTB(dbPath, tbName1)
        rs = loadTB(dbPath, tbName1, "where date>=2010.02.01 or !in(id,[3,5])")
        assert_frame_equal(loadTB(dbPath, tbName1), origin)
        assert_frame_equal(loadTB(dbPath, tbName2), origin)
        self.s.dropPartition(dbPath, ["2010.01.01", "[3,5]"], tbName2)
        assert_frame_equal(loadTB(dbPath, tbName2), rs)
        assert_frame_equal(loadTB(dbPath, tbName1), origin)

        # both dropPartition
        create_dfs_compo_range_hash_db()
        origin = loadTB(dbPath, tbName1)
        rs = loadTB(dbPath, tbName1, "where date>=2010.02.01 or !in(id,[3,5])")
        assert_frame_equal(loadTB(dbPath, tbName1), origin)
        assert_frame_equal(loadTB(dbPath, tbName2), origin)
        self.s.dropPartition(dbPath, ["2010.01.01", "[3,5]"])
        assert_frame_equal(loadTB(dbPath, tbName1), rs)
        assert_frame_equal(loadTB(dbPath, tbName2), rs)

    def test_dropPartition_dfs_compo2_range_hash_drop_multiple(self):
        dbPath = DBInfo.dfsDBName
        tbName1 = DBInfo.table1
        tbName2 = DBInfo.table2

        # tb1 dropPartition
        create_dfs_compo_range_hash_db()
        origin = loadTB(dbPath, tbName1)
        rs = loadTB(dbPath, tbName1, "where date>=2010.03.01 or !in(id,[3,5])")
        assert_frame_equal(loadTB(dbPath, tbName2), origin)
        assert_frame_equal(loadTB(dbPath, tbName1), origin)
        self.s.dropPartition(dbPath, ["[2010.01.01,2010.02.01]", "[3,5]"], tbName1)
        assert_frame_equal(loadTB(dbPath, tbName1), rs)
        assert_frame_equal(loadTB(dbPath, tbName2), origin)

        # tb2 dropPartition
        create_dfs_compo_range_hash_db()
        origin = loadTB(dbPath, tbName1)
        rs = loadTB(dbPath, tbName1, "where date>=2010.03.01 or !in(id,[3,5])")
        assert_frame_equal(loadTB(dbPath, tbName1), origin)
        assert_frame_equal(loadTB(dbPath, tbName2), origin)
        self.s.dropPartition(dbPath, ["[2010.01.01,2010.02.01]", "[3,5]"], tbName2)
        assert_frame_equal(loadTB(dbPath, tbName2), rs)
        assert_frame_equal(loadTB(dbPath, tbName1), origin)

        # both dropPartition
        create_dfs_compo_range_hash_db()
        origin = loadTB(dbPath, tbName1)
        rs = loadTB(dbPath, tbName1, "where date>=2010.03.01 or !in(id,[3,5])")
        assert_frame_equal(loadTB(dbPath, tbName1), origin)
        assert_frame_equal(loadTB(dbPath, tbName2), origin)
        self.s.dropPartition(dbPath, ["[2010.01.01,2010.02.01]", "[3,5]"])
        assert_frame_equal(loadTB(dbPath, tbName1), rs)
        assert_frame_equal(loadTB(dbPath, tbName2), rs)

    def test_dropPartition_dfs_compo2_range_value_drop_level1_single(self):
        dbPath = DBInfo.dfsDBName
        tbName1 = DBInfo.table1
        tbName2 = DBInfo.table2

        # tb1 dropPartition
        create_dfs_compo_range_value_db()
        origin = loadTB(dbPath, tbName1)
        rs = loadTB(dbPath, tbName1, "where date>=2010.02.01")
        assert_frame_equal(loadTB(dbPath, tbName2), origin)
        assert_frame_equal(loadTB(dbPath, tbName1), origin)
        self.s.dropPartition(dbPath, ["2010.01.01"], tbName1)
        assert_frame_equal(loadTB(dbPath, tbName1), rs)
        assert_frame_equal(loadTB(dbPath, tbName2), origin)

        # tb2 dropPartition
        create_dfs_compo_range_value_db()
        origin = loadTB(dbPath, tbName1)
        rs = loadTB(dbPath, tbName1, "where date>=2010.02.01")
        assert_frame_equal(loadTB(dbPath, tbName1), origin)
        assert_frame_equal(loadTB(dbPath, tbName2), origin)
        self.s.dropPartition(dbPath, ["2010.01.01"], tbName2)
        assert_frame_equal(loadTB(dbPath, tbName2), rs)
        assert_frame_equal(loadTB(dbPath, tbName1), origin)

        # both dropPartition
        create_dfs_compo_range_value_db()
        origin = loadTB(dbPath, tbName1)
        rs = loadTB(dbPath, tbName1, "where date>=2010.02.01")
        assert_frame_equal(loadTB(dbPath, tbName1), origin)
        assert_frame_equal(loadTB(dbPath, tbName2), origin)
        self.s.dropPartition(dbPath, ["2010.01.01"])
        assert_frame_equal(loadTB(dbPath, tbName1), rs)
        assert_frame_equal(loadTB(dbPath, tbName2), rs)

    def test_dropPartition_dfs_compo2_range_value_drop_level1_multiple(self):
        dbPath = DBInfo.dfsDBName
        tbName1 = DBInfo.table1
        tbName2 = DBInfo.table2

        # tb1 dropPartition
        create_dfs_compo_range_value_db()
        origin = loadTB(dbPath, tbName1)
        rs = loadTB(dbPath, tbName1, "where date>=2010.03.01")
        assert_frame_equal(loadTB(dbPath, tbName2), origin)
        assert_frame_equal(loadTB(dbPath, tbName1), origin)
        self.s.dropPartition(dbPath, ["2010.01.01", "2010.02.01"], tbName1)
        assert_frame_equal(loadTB(dbPath, tbName1), rs)
        assert_frame_equal(loadTB(dbPath, tbName2), origin)

        # tb2 dropPartition
        create_dfs_compo_range_value_db()
        origin = loadTB(dbPath, tbName1)
        rs = loadTB(dbPath, tbName1, "where date>=2010.03.01")
        assert_frame_equal(loadTB(dbPath, tbName1), origin)
        assert_frame_equal(loadTB(dbPath, tbName2), origin)
        self.s.dropPartition(dbPath, ["2010.01.01", "2010.02.01"], tbName2)
        assert_frame_equal(loadTB(dbPath, tbName2), rs)
        assert_frame_equal(loadTB(dbPath, tbName1), origin)

        # both dropPartition
        create_dfs_compo_range_value_db()
        origin = loadTB(dbPath, tbName1)
        rs = loadTB(dbPath, tbName1, "where date>=2010.03.01")
        assert_frame_equal(loadTB(dbPath, tbName1), origin)
        assert_frame_equal(loadTB(dbPath, tbName2), origin)
        self.s.dropPartition(dbPath, ["2010.01.01", "2010.02.01"])
        assert_frame_equal(loadTB(dbPath, tbName1), rs)
        assert_frame_equal(loadTB(dbPath, tbName2), rs)

    def test_dropPartition_dfs_compo2_range_value_drop_level2_single(self):
        dbPath = DBInfo.dfsDBName
        tbName1 = DBInfo.table1
        tbName2 = DBInfo.table2

        # tb1 dropPartition
        create_dfs_compo_range_value_db()
        origin = loadTB(dbPath, tbName1)
        rs = loadTB(dbPath, tbName1, "where date>=2010.02.01 or id!=1")
        assert_frame_equal(loadTB(dbPath, tbName2), origin)
        assert_frame_equal(loadTB(dbPath, tbName1), origin)
        self.s.dropPartition(dbPath, ["2010.01.01", "1"], tbName1)
        assert_frame_equal(loadTB(dbPath, tbName1), rs)
        assert_frame_equal(loadTB(dbPath, tbName2), origin)

        # tb2 dropPartition
        create_dfs_compo_range_value_db()
        origin = loadTB(dbPath, tbName1)
        rs = loadTB(dbPath, tbName1, "where date>=2010.02.01 or id!=1")
        assert_frame_equal(loadTB(dbPath, tbName1), origin)
        assert_frame_equal(loadTB(dbPath, tbName2), origin)
        self.s.dropPartition(dbPath, ["2010.01.01", "1"], tbName2)
        assert_frame_equal(loadTB(dbPath, tbName2), rs)
        assert_frame_equal(loadTB(dbPath, tbName1), origin)

        # both dropPartition
        create_dfs_compo_range_value_db()
        origin = loadTB(dbPath, tbName1)
        rs = loadTB(dbPath, tbName1, "where date>=2010.02.01 or id!=1")
        assert_frame_equal(loadTB(dbPath, tbName1), origin)
        assert_frame_equal(loadTB(dbPath, tbName2), origin)
        self.s.dropPartition(dbPath, ["2010.01.01", "1"])
        assert_frame_equal(loadTB(dbPath, tbName1), rs)
        assert_frame_equal(loadTB(dbPath, tbName2), rs)

    def test_dropPartition_dfs_compo2_range_value_drop_level2_multiple(self):
        dbPath = DBInfo.dfsDBName
        tbName1 = DBInfo.table1
        tbName2 = DBInfo.table2

        # tb1 dropPartition
        create_dfs_compo_range_value_db()
        origin = loadTB(dbPath, tbName1)
        rs = loadTB(dbPath, tbName1, "where date>=2010.02.01 or !between(id,2:4)")
        assert_frame_equal(loadTB(dbPath, tbName2), origin)
        assert_frame_equal(loadTB(dbPath, tbName1), origin)
        self.s.dropPartition(dbPath, ["2010.01.01", "[2,3,4]"], tbName1)
        assert_frame_equal(loadTB(dbPath, tbName1), rs)
        assert_frame_equal(loadTB(dbPath, tbName2), origin)

        # tb2 dropPartition
        create_dfs_compo_range_value_db()
        origin = loadTB(dbPath, tbName1)
        rs = loadTB(dbPath, tbName1, "where date>=2010.02.01 or !between(id,2:4)")
        assert_frame_equal(loadTB(dbPath, tbName1), origin)
        assert_frame_equal(loadTB(dbPath, tbName2), origin)
        self.s.dropPartition(dbPath, ["2010.01.01", "[2,3,4]"], tbName2)
        assert_frame_equal(loadTB(dbPath, tbName2), rs)
        assert_frame_equal(loadTB(dbPath, tbName1), origin)

        # both dropPartition
        create_dfs_compo_range_value_db()
        origin = loadTB(dbPath, tbName1)
        rs = loadTB(dbPath, tbName1, "where date>=2010.02.01 or !between(id,2:4)")
        assert_frame_equal(loadTB(dbPath, tbName1), origin)
        assert_frame_equal(loadTB(dbPath, tbName2), origin)
        self.s.dropPartition(dbPath, ["2010.01.01", "[2,3,4]"])
        assert_frame_equal(loadTB(dbPath, tbName1), rs)
        assert_frame_equal(loadTB(dbPath, tbName2), rs)

    def test_dropPartition_dfs_compo2_range_value_drop_multiple(self):
        dbPath = DBInfo.dfsDBName
        tbName1 = DBInfo.table1
        tbName2 = DBInfo.table2

        # tb1 dropPartition
        create_dfs_compo_range_value_db()
        origin = loadTB(dbPath, tbName1)
        rs = loadTB(dbPath, tbName1, "where date>=2010.03.01 or !between(id,2:4)")
        assert_frame_equal(loadTB(dbPath, tbName2), origin)
        assert_frame_equal(loadTB(dbPath, tbName1), origin)
        self.s.dropPartition(dbPath, ["[2010.01.01,2010.02.01]", "[2,3,4]"], tbName1)
        assert_frame_equal(loadTB(dbPath, tbName1), rs)
        assert_frame_equal(loadTB(dbPath, tbName2), origin)

        # tb2 dropPartition
        create_dfs_compo_range_value_db()
        origin = loadTB(dbPath, tbName1)
        rs = loadTB(dbPath, tbName1, "where date>=2010.03.01 or !between(id,2:4)")
        assert_frame_equal(loadTB(dbPath, tbName1), origin)
        assert_frame_equal(loadTB(dbPath, tbName2), origin)
        self.s.dropPartition(dbPath, ["[2010.01.01,2010.02.01]", "[2,3,4]"], tbName2)
        assert_frame_equal(loadTB(dbPath, tbName2), rs)
        assert_frame_equal(loadTB(dbPath, tbName1), origin)

        # both dropPartition
        create_dfs_compo_range_value_db()
        origin = loadTB(dbPath, tbName1)
        rs = loadTB(dbPath, tbName1, "where date>=2010.03.01 or !between(id,2:4)")
        assert_frame_equal(loadTB(dbPath, tbName1), origin)
        assert_frame_equal(loadTB(dbPath, tbName2), origin)
        self.s.dropPartition(dbPath, ["[2010.01.01,2010.02.01]", "[2,3,4]"])
        assert_frame_equal(loadTB(dbPath, tbName1), rs)
        assert_frame_equal(loadTB(dbPath, tbName2), rs)

    def test_dropPartition_dfs_compo2_range_list_drop_level1_single(self):
        dbPath = DBInfo.dfsDBName
        tbName1 = DBInfo.table1
        tbName2 = DBInfo.table2

        # tb1 dropPartition
        create_dfs_compo_range_list_db()
        origin = loadTB(dbPath, tbName1)
        rs = loadTB(dbPath, tbName1, "where date>=2010.02.01")
        assert_frame_equal(loadTB(dbPath, tbName2), origin)
        assert_frame_equal(loadTB(dbPath, tbName1), origin)
        self.s.dropPartition(dbPath, ["2010.01.01"], tbName1)
        assert_frame_equal(loadTB(dbPath, tbName1), rs)
        assert_frame_equal(loadTB(dbPath, tbName2), origin)

        # tb2 dropPartition
        create_dfs_compo_range_list_db()
        origin = loadTB(dbPath, tbName1)
        rs = loadTB(dbPath, tbName1, "where date>=2010.02.01")
        assert_frame_equal(loadTB(dbPath, tbName1), origin)
        assert_frame_equal(loadTB(dbPath, tbName2), origin)
        self.s.dropPartition(dbPath, ["2010.01.01"], tbName2)
        assert_frame_equal(loadTB(dbPath, tbName2), rs)
        assert_frame_equal(loadTB(dbPath, tbName1), origin)

        # both dropPartition
        create_dfs_compo_range_list_db()
        origin = loadTB(dbPath, tbName1)
        rs = loadTB(dbPath, tbName1, "where date>=2010.02.01")
        assert_frame_equal(loadTB(dbPath, tbName1), origin)
        assert_frame_equal(loadTB(dbPath, tbName2), origin)
        self.s.dropPartition(dbPath, ["2010.01.01"])
        assert_frame_equal(loadTB(dbPath, tbName1), rs)
        assert_frame_equal(loadTB(dbPath, tbName2), rs)

    def test_dropPartition_dfs_compo2_range_list_drop_level1_multiple(self):
        dbPath = DBInfo.dfsDBName
        tbName1 = DBInfo.table1
        tbName2 = DBInfo.table2

        # tb1 dropPartition
        create_dfs_compo_range_list_db()
        origin = loadTB(dbPath, tbName1)
        rs = loadTB(dbPath, tbName1, "where date>=2010.03.01")
        assert_frame_equal(loadTB(dbPath, tbName2), origin)
        assert_frame_equal(loadTB(dbPath, tbName1), origin)
        self.s.dropPartition(dbPath, ["2010.01.01", "2010.02.01"], tbName1)
        assert_frame_equal(loadTB(dbPath, tbName1), rs)
        assert_frame_equal(loadTB(dbPath, tbName2), origin)

        # tb2 dropPartition
        create_dfs_compo_range_list_db()
        origin = loadTB(dbPath, tbName1)
        rs = loadTB(dbPath, tbName1, "where date>=2010.03.01")
        assert_frame_equal(loadTB(dbPath, tbName1), origin)
        assert_frame_equal(loadTB(dbPath, tbName2), origin)
        self.s.dropPartition(dbPath, ["2010.01.01", "2010.02.01"], tbName2)
        assert_frame_equal(loadTB(dbPath, tbName2), rs)
        assert_frame_equal(loadTB(dbPath, tbName1), origin)

        # both dropPartition
        create_dfs_compo_range_list_db()
        origin = loadTB(dbPath, tbName1)
        rs = loadTB(dbPath, tbName1, "where date>=2010.03.01")
        assert_frame_equal(loadTB(dbPath, tbName1), origin)
        assert_frame_equal(loadTB(dbPath, tbName2), origin)
        self.s.dropPartition(dbPath, ["2010.01.01", "2010.02.01"])
        assert_frame_equal(loadTB(dbPath, tbName1), rs)
        assert_frame_equal(loadTB(dbPath, tbName2), rs)

    def test_dropPartition_dfs_compo2_range_list_drop_level2_single(self):
        dbPath = DBInfo.dfsDBName
        tbName1 = DBInfo.table1
        tbName2 = DBInfo.table2

        # tb1 dropPartition
        create_dfs_compo_range_list_db()
        origin = loadTB(dbPath, tbName1)
        rs = loadTB(dbPath, tbName1, "where date>=2010.02.01 or !in(sym,`AMD`QWE`CES)")
        assert_frame_equal(loadTB(dbPath, tbName2), origin)
        assert_frame_equal(loadTB(dbPath, tbName1), origin)
        self.s.dropPartition(dbPath, ["2010.01.01", "'AMD'"], tbName1)
        assert_frame_equal(loadTB(dbPath, tbName1), rs)
        assert_frame_equal(loadTB(dbPath, tbName2), origin)

        # tb2 dropPartition
        create_dfs_compo_range_list_db()
        origin = loadTB(dbPath, tbName1)
        rs = loadTB(dbPath, tbName1, "where date>=2010.02.01 or !in(sym,`AMD`QWE`CES)")
        assert_frame_equal(loadTB(dbPath, tbName1), origin)
        assert_frame_equal(loadTB(dbPath, tbName2), origin)
        self.s.dropPartition(dbPath, ["2010.01.01", "'AMD'"], tbName2)
        assert_frame_equal(loadTB(dbPath, tbName2), rs)
        assert_frame_equal(loadTB(dbPath, tbName1), origin)

        # both dropPartition
        create_dfs_compo_range_list_db()
        origin = loadTB(dbPath, tbName1)
        rs = loadTB(dbPath, tbName1, "where date>=2010.02.01 or !in(sym,`AMD`QWE`CES)")
        assert_frame_equal(loadTB(dbPath, tbName1), origin)
        assert_frame_equal(loadTB(dbPath, tbName2), origin)
        self.s.dropPartition(dbPath, ["2010.01.01", "'AMD'"])
        assert_frame_equal(loadTB(dbPath, tbName1), rs)
        assert_frame_equal(loadTB(dbPath, tbName2), rs)

    def test_dropPartition_dfs_compo2_range_list_drop_level2_multiple(self):
        dbPath = DBInfo.dfsDBName
        tbName1 = DBInfo.table1
        tbName2 = DBInfo.table2

        # tb1 dropPartition
        create_dfs_compo_range_list_db()
        origin = loadTB(dbPath, tbName1)
        rs = loadTB(dbPath, tbName1, "where date>=2010.02.01 or !in(sym,`DOP`ASZ`FSD`BBVC)")
        assert_frame_equal(loadTB(dbPath, tbName2), origin)
        assert_frame_equal(loadTB(dbPath, tbName1), origin)
        self.s.dropPartition(dbPath, ["2010.01.01", "['DOP','FSD']"], tbName1)
        assert_frame_equal(loadTB(dbPath, tbName1), rs)
        assert_frame_equal(loadTB(dbPath, tbName2), origin)

        # tb2 dropPartition
        create_dfs_compo_range_list_db()
        origin = loadTB(dbPath, tbName1)
        rs = loadTB(dbPath, tbName1, "where date>=2010.02.01 or !in(sym,`DOP`ASZ`FSD`BBVC)")
        assert_frame_equal(loadTB(dbPath, tbName1), origin)
        assert_frame_equal(loadTB(dbPath, tbName2), origin)
        self.s.dropPartition(dbPath, ["2010.01.01", "['DOP','FSD']"], tbName2)
        assert_frame_equal(loadTB(dbPath, tbName2), rs)
        assert_frame_equal(loadTB(dbPath, tbName1), origin)

        # both dropPartition
        create_dfs_compo_range_list_db()
        origin = loadTB(dbPath, tbName1)
        rs = loadTB(dbPath, tbName1, "where date>=2010.02.01 or !in(sym,`DOP`ASZ`FSD`BBVC)")
        assert_frame_equal(loadTB(dbPath, tbName1), origin)
        assert_frame_equal(loadTB(dbPath, tbName2), origin)
        self.s.dropPartition(dbPath, ["2010.01.01", "['DOP','FSD']"])
        assert_frame_equal(loadTB(dbPath, tbName1), rs)
        assert_frame_equal(loadTB(dbPath, tbName2), rs)

    def test_dropPartition_dfs_compo2_range_list_drop_multiple(self):
        dbPath = DBInfo.dfsDBName
        tbName1 = DBInfo.table1
        tbName2 = DBInfo.table2

        # tb1 dropPartition
        create_dfs_compo_range_list_db()
        origin = loadTB(dbPath, tbName1)
        rs = loadTB(dbPath, tbName1, "where date>=2010.03.01 or !in(sym,`DOP`ASZ`FSD`BBVC)")
        assert_frame_equal(loadTB(dbPath, tbName2), origin)
        assert_frame_equal(loadTB(dbPath, tbName1), origin)
        self.s.dropPartition(dbPath, ["[2010.01.01,2010.02.01]", "['DOP','FSD']"], tbName1)
        assert_frame_equal(loadTB(dbPath, tbName1), rs)
        assert_frame_equal(loadTB(dbPath, tbName2), origin)

        # tb2 dropPartition
        create_dfs_compo_range_list_db()
        origin = loadTB(dbPath, tbName1)
        rs = loadTB(dbPath, tbName1, "where date>=2010.03.01 or !in(sym,`DOP`ASZ`FSD`BBVC)")
        assert_frame_equal(loadTB(dbPath, tbName1), origin)
        assert_frame_equal(loadTB(dbPath, tbName2), origin)
        self.s.dropPartition(dbPath, ["[2010.01.01,2010.02.01]", "['DOP','FSD']"], tbName2)
        assert_frame_equal(loadTB(dbPath, tbName2), rs)
        assert_frame_equal(loadTB(dbPath, tbName1), origin)

        # both dropPartition
        create_dfs_compo_range_list_db()
        origin = loadTB(dbPath, tbName1)
        rs = loadTB(dbPath, tbName1, "where date>=2010.03.01 or !in(sym,`DOP`ASZ`FSD`BBVC)")
        assert_frame_equal(loadTB(dbPath, tbName1), origin)
        assert_frame_equal(loadTB(dbPath, tbName2), origin)
        self.s.dropPartition(dbPath, ["[2010.01.01,2010.02.01]", "['DOP','FSD']"])
        assert_frame_equal(loadTB(dbPath, tbName1), rs)
        assert_frame_equal(loadTB(dbPath, tbName2), rs)

    def test_dropPartition_dfs_compo3_range_hash_list_drop_level1_single(self):
        dbPath = DBInfo.dfsDBName
        tbName1 = DBInfo.table1
        tbName2 = DBInfo.table2

        # tb1 dropPartition
        create_dfs_compo_range_hash_list_db()
        origin = loadTB(dbPath, tbName1)
        rs = loadTB(dbPath, tbName1, "where date>=2010.02.01")
        assert_frame_equal(loadTB(dbPath, tbName2), origin)
        assert_frame_equal(loadTB(dbPath, tbName1), origin)
        self.s.dropPartition(dbPath, ["2010.01.01"], tbName1)
        assert_frame_equal(loadTB(dbPath, tbName1), rs)
        assert_frame_equal(loadTB(dbPath, tbName2), origin)

        # tb2 dropPartition
        create_dfs_compo_range_hash_list_db()
        origin = loadTB(dbPath, tbName1)
        rs = loadTB(dbPath, tbName1, "where date>=2010.02.01")
        assert_frame_equal(loadTB(dbPath, tbName1), origin)
        assert_frame_equal(loadTB(dbPath, tbName2), origin)
        self.s.dropPartition(dbPath, ["2010.01.01"], tbName2)
        assert_frame_equal(loadTB(dbPath, tbName2), rs)
        assert_frame_equal(loadTB(dbPath, tbName1), origin)

        # both dropPartition
        create_dfs_compo_range_hash_list_db()
        origin = loadTB(dbPath, tbName1)
        rs = loadTB(dbPath, tbName1, "where date>=2010.02.01")
        assert_frame_equal(loadTB(dbPath, tbName1), origin)
        assert_frame_equal(loadTB(dbPath, tbName2), origin)
        self.s.dropPartition(dbPath, ["2010.01.01"])
        assert_frame_equal(loadTB(dbPath, tbName1), rs)
        assert_frame_equal(loadTB(dbPath, tbName2), rs)

    def test_dropPartition_dfs_compo3_range_hash_list_drop_level1_multiple(self):
        dbPath = DBInfo.dfsDBName
        tbName1 = DBInfo.table1
        tbName2 = DBInfo.table2

        # tb1 dropPartition
        create_dfs_compo_range_hash_list_db()
        origin = loadTB(dbPath, tbName1)
        rs = loadTB(dbPath, tbName1, "where date>=2010.03.01")
        assert_frame_equal(loadTB(dbPath, tbName2), origin)
        assert_frame_equal(loadTB(dbPath, tbName1), origin)
        self.s.dropPartition(dbPath, ["2010.01.01", "2010.02.01"], tbName1)
        assert_frame_equal(loadTB(dbPath, tbName1), rs)
        assert_frame_equal(loadTB(dbPath, tbName2), origin)

        # tb2 dropPartition
        create_dfs_compo_range_hash_list_db()
        origin = loadTB(dbPath, tbName1)
        rs = loadTB(dbPath, tbName1, "where date>=2010.03.01")
        assert_frame_equal(loadTB(dbPath, tbName1), origin)
        assert_frame_equal(loadTB(dbPath, tbName2), origin)
        self.s.dropPartition(dbPath, ["2010.01.01", "2010.02.01"], tbName2)
        assert_frame_equal(loadTB(dbPath, tbName2), rs)
        assert_frame_equal(loadTB(dbPath, tbName1), origin)

        # both dropPartition
        create_dfs_compo_range_hash_list_db()
        origin = loadTB(dbPath, tbName1)
        rs = loadTB(dbPath, tbName1, "where date>=2010.03.01")
        assert_frame_equal(loadTB(dbPath, tbName1), origin)
        assert_frame_equal(loadTB(dbPath, tbName2), origin)
        self.s.dropPartition(dbPath, ["2010.01.01", "2010.02.01"])
        assert_frame_equal(loadTB(dbPath, tbName1), rs)
        assert_frame_equal(loadTB(dbPath, tbName2), rs)

    def test_dropPartition_dfs_compo3_range_hash_list_drop_level2_single(self):
        dbPath = DBInfo.dfsDBName
        tbName1 = DBInfo.table1
        tbName2 = DBInfo.table2

        # tb1 dropPartition
        create_dfs_compo_range_hash_list_db()
        origin = loadTB(dbPath, tbName1)
        rs = loadTB(dbPath, tbName1, "where date>=2010.02.01 or id!=1")
        assert_frame_equal(loadTB(dbPath, tbName2), origin)
        assert_frame_equal(loadTB(dbPath, tbName1), origin)
        self.s.dropPartition(dbPath, ["2010.01.01", "1"], tbName1)
        assert_frame_equal(loadTB(dbPath, tbName1), rs)
        assert_frame_equal(loadTB(dbPath, tbName2), origin)

        # tb2 dropPartition
        create_dfs_compo_range_hash_list_db()
        origin = loadTB(dbPath, tbName1)
        rs = loadTB(dbPath, tbName1, "where date>=2010.02.01 or id!=1")
        assert_frame_equal(loadTB(dbPath, tbName1), origin)
        assert_frame_equal(loadTB(dbPath, tbName2), origin)
        self.s.dropPartition(dbPath, ["2010.01.01", "1"], tbName2)
        assert_frame_equal(loadTB(dbPath, tbName2), rs)
        assert_frame_equal(loadTB(dbPath, tbName1), origin)

        # both dropPartition
        create_dfs_compo_range_hash_list_db()
        origin = loadTB(dbPath, tbName1)
        rs = loadTB(dbPath, tbName1, "where date>=2010.02.01 or id!=1")
        assert_frame_equal(loadTB(dbPath, tbName1), origin)
        assert_frame_equal(loadTB(dbPath, tbName2), origin)
        self.s.dropPartition(dbPath, ["2010.01.01", "1"])
        assert_frame_equal(loadTB(dbPath, tbName1), rs)
        assert_frame_equal(loadTB(dbPath, tbName2), rs)

    def test_dropPartition_dfs_compo3_range_hash_list_drop_level2_multiple(self):
        dbPath = DBInfo.dfsDBName
        tbName1 = DBInfo.table1
        tbName2 = DBInfo.table2

        # tb1 dropPartition
        create_dfs_compo_range_hash_list_db()
        origin = loadTB(dbPath, tbName1)
        rs = loadTB(dbPath, tbName1, "where date>=2010.02.01 or !in(id,1..3)")
        assert_frame_equal(loadTB(dbPath, tbName2), origin)
        assert_frame_equal(loadTB(dbPath, tbName1), origin)
        self.s.dropPartition(dbPath, ["2010.01.01", "1..3"], tbName1)
        assert_frame_equal(loadTB(dbPath, tbName1), rs)
        assert_frame_equal(loadTB(dbPath, tbName2), origin)

        # tb2 dropPartition
        create_dfs_compo_range_hash_list_db()
        origin = loadTB(dbPath, tbName1)
        rs = loadTB(dbPath, tbName1, "where date>=2010.02.01 or !in(id,1..3)")
        assert_frame_equal(loadTB(dbPath, tbName1), origin)
        assert_frame_equal(loadTB(dbPath, tbName2), origin)
        self.s.dropPartition(dbPath, ["2010.01.01", "1..3"], tbName2)
        assert_frame_equal(loadTB(dbPath, tbName2), rs)
        assert_frame_equal(loadTB(dbPath, tbName1), origin)

        # both dropPartition
        create_dfs_compo_range_hash_list_db()
        origin = loadTB(dbPath, tbName1)
        rs = loadTB(dbPath, tbName1, "where date>=2010.02.01 or !in(id,1..3)")
        assert_frame_equal(loadTB(dbPath, tbName1), origin)
        assert_frame_equal(loadTB(dbPath, tbName2), origin)
        self.s.dropPartition(dbPath, ["2010.01.01", "1..3"])
        assert_frame_equal(loadTB(dbPath, tbName1), rs)
        assert_frame_equal(loadTB(dbPath, tbName2), rs)

    def test_dropPartition_dfs_compo3_range_hash_list_drop_level3_single(self):
        dbPath = DBInfo.dfsDBName
        tbName1 = DBInfo.table1
        tbName2 = DBInfo.table2

        # tb1 dropPartition
        create_dfs_compo_range_hash_list_db()
        origin = loadTB(dbPath, tbName1)
        rs = loadTB(dbPath, tbName1, "where date>=2010.02.01 or id!=1 or !in(sym,`AMD`QWE`CES)")
        assert_frame_equal(loadTB(dbPath, tbName2), origin)
        assert_frame_equal(loadTB(dbPath, tbName1), origin)
        self.s.dropPartition(dbPath, ["2010.01.01", "1", "'AMD'"], tbName1)
        assert_frame_equal(loadTB(dbPath, tbName1), rs)
        assert_frame_equal(loadTB(dbPath, tbName2), origin)

        # tb2 dropPartition
        create_dfs_compo_range_hash_list_db()
        origin = loadTB(dbPath, tbName1)
        rs = loadTB(dbPath, tbName1, "where date>=2010.02.01 or id!=1 or !in(sym,`AMD`QWE`CES)")
        assert_frame_equal(loadTB(dbPath, tbName1), origin)
        assert_frame_equal(loadTB(dbPath, tbName2), origin)
        self.s.dropPartition(dbPath, ["2010.01.01", "1", "'AMD'"], tbName2)
        assert_frame_equal(loadTB(dbPath, tbName2), rs)
        assert_frame_equal(loadTB(dbPath, tbName1), origin)

        # both dropPartition
        create_dfs_compo_range_hash_list_db()
        origin = loadTB(dbPath, tbName1)
        rs = loadTB(dbPath, tbName1, "where date>=2010.02.01 or id!=1 or !in(sym,`AMD`QWE`CES)")
        assert_frame_equal(loadTB(dbPath, tbName1), origin)
        assert_frame_equal(loadTB(dbPath, tbName2), origin)
        self.s.dropPartition(dbPath, ["2010.01.01", "1", "'AMD'"])
        assert_frame_equal(loadTB(dbPath, tbName1), rs)
        assert_frame_equal(loadTB(dbPath, tbName2), rs)

    def test_dropPartition_dfs_compo3_range_hash_list_drop_level3_multiple(self):
        dbPath = DBInfo.dfsDBName
        tbName1 = DBInfo.table1
        tbName2 = DBInfo.table2

        # tb1 dropPartition
        create_dfs_compo_range_hash_list_db()
        origin = loadTB(dbPath, tbName1)
        rs = loadTB(dbPath, tbName1, "where date>=2010.02.01 or id!=1 or !in(sym,`DOP`ASZ`FSD`BBVC)")
        assert_frame_equal(loadTB(dbPath, tbName2), origin)
        assert_frame_equal(loadTB(dbPath, tbName1), origin)
        self.s.dropPartition(dbPath, ["2010.01.01", "1", "['DOP','FSD']"], tbName1)
        assert_frame_equal(loadTB(dbPath, tbName1), rs)
        assert_frame_equal(loadTB(dbPath, tbName2), origin)

        # tb2 dropPartition
        create_dfs_compo_range_hash_list_db()
        origin = loadTB(dbPath, tbName1)
        rs = loadTB(dbPath, tbName1, "where date>=2010.02.01 or id!=1 or !in(sym,`DOP`ASZ`FSD`BBVC)")
        assert_frame_equal(loadTB(dbPath, tbName1), origin)
        assert_frame_equal(loadTB(dbPath, tbName2), origin)
        self.s.dropPartition(dbPath, ["2010.01.01", "1", "['DOP','FSD']"], tbName2)
        assert_frame_equal(loadTB(dbPath, tbName2), rs)
        assert_frame_equal(loadTB(dbPath, tbName1), origin)

        # both dropPartition
        create_dfs_compo_range_hash_list_db()
        origin = loadTB(dbPath, tbName1)
        rs = loadTB(dbPath, tbName1, "where date>=2010.02.01 or id!=1 or !in(sym,`DOP`ASZ`FSD`BBVC)")
        assert_frame_equal(loadTB(dbPath, tbName1), origin)
        assert_frame_equal(loadTB(dbPath, tbName2), origin)
        self.s.dropPartition(dbPath, ["2010.01.01", "1", "['DOP','FSD']"])
        assert_frame_equal(loadTB(dbPath, tbName1), rs)
        assert_frame_equal(loadTB(dbPath, tbName2), rs)

    def test_dropPartition_dfs_compo3_range_hash_list_drop_multiple(self):
        dbPath = DBInfo.dfsDBName
        tbName1 = DBInfo.table1
        tbName2 = DBInfo.table2

        # tb1 dropPartition
        create_dfs_compo_range_hash_list_db()
        origin = loadTB(dbPath, tbName1)
        rs = loadTB(dbPath, tbName1, "where date>=2010.03.01 or !in(id,1..3) or !in(sym,`DOP`ASZ`FSD`BBVC)")
        assert_frame_equal(loadTB(dbPath, tbName2), origin)
        assert_frame_equal(loadTB(dbPath, tbName1), origin)
        self.s.dropPartition(dbPath, ["[2010.01.01,2010.02.01]", "1..3", "['DOP','FSD']"], tbName1)
        assert_frame_equal(loadTB(dbPath, tbName1), rs)
        assert_frame_equal(loadTB(dbPath, tbName2), origin)

        # tb2 dropPartition
        create_dfs_compo_range_hash_list_db()
        origin = loadTB(dbPath, tbName1)
        rs = loadTB(dbPath, tbName1, "where date>=2010.03.01 or !in(id,1..3) or !in(sym,`DOP`ASZ`FSD`BBVC)")
        assert_frame_equal(loadTB(dbPath, tbName1), origin)
        assert_frame_equal(loadTB(dbPath, tbName2), origin)
        self.s.dropPartition(dbPath, ["[2010.01.01,2010.02.01]", "1..3", "['DOP','FSD']"], tbName2)
        assert_frame_equal(loadTB(dbPath, tbName2), rs)
        assert_frame_equal(loadTB(dbPath, tbName1), origin)

        # both dropPartition
        create_dfs_compo_range_hash_list_db()
        origin = loadTB(dbPath, tbName1)
        rs = loadTB(dbPath, tbName1, "where date>=2010.03.01 or !in(id,1..3) or !in(sym,`DOP`ASZ`FSD`BBVC)")
        assert_frame_equal(loadTB(dbPath, tbName1), origin)
        assert_frame_equal(loadTB(dbPath, tbName2), origin)
        self.s.dropPartition(dbPath, ["[2010.01.01,2010.02.01]", "1..3", "['DOP','FSD']"])
        assert_frame_equal(loadTB(dbPath, tbName1), rs)
        assert_frame_equal(loadTB(dbPath, tbName2), rs)

    def test_dropPartition_dfs_compo3_range_value_list_drop_level1_single(self):
        dbPath = DBInfo.dfsDBName
        tbName1 = DBInfo.table1
        tbName2 = DBInfo.table2

        # tb1 dropPartition
        create_dfs_compo_range_value_list_db()
        origin = loadTB(dbPath, tbName1)
        rs = loadTB(dbPath, tbName1, "where date>=2010.02.01")
        assert_frame_equal(loadTB(dbPath, tbName2), origin)
        assert_frame_equal(loadTB(dbPath, tbName1), origin)
        self.s.dropPartition(dbPath, ["2010.01.01"], tbName1)
        assert_frame_equal(loadTB(dbPath, tbName1), rs)
        assert_frame_equal(loadTB(dbPath, tbName2), origin)

        # tb2 dropPartition
        create_dfs_compo_range_value_list_db()
        origin = loadTB(dbPath, tbName1)
        rs = loadTB(dbPath, tbName1, "where date>=2010.02.01")
        assert_frame_equal(loadTB(dbPath, tbName1), origin)
        assert_frame_equal(loadTB(dbPath, tbName2), origin)
        self.s.dropPartition(dbPath, ["2010.01.01"], tbName2)
        assert_frame_equal(loadTB(dbPath, tbName2), rs)
        assert_frame_equal(loadTB(dbPath, tbName1), origin)

        # both dropPartition
        create_dfs_compo_range_value_list_db()
        origin = loadTB(dbPath, tbName1)
        rs = loadTB(dbPath, tbName1, "where date>=2010.02.01")
        assert_frame_equal(loadTB(dbPath, tbName1), origin)
        assert_frame_equal(loadTB(dbPath, tbName2), origin)
        self.s.dropPartition(dbPath, ["2010.01.01"])
        assert_frame_equal(loadTB(dbPath, tbName1), rs)
        assert_frame_equal(loadTB(dbPath, tbName2), rs)

    def test_dropPartition_dfs_compo3_range_value_list_drop_level1_multiple(self):
        dbPath = DBInfo.dfsDBName
        tbName1 = DBInfo.table1
        tbName2 = DBInfo.table2

        # tb1 dropPartition
        create_dfs_compo_range_value_list_db()
        origin = loadTB(dbPath, tbName1)
        rs = loadTB(dbPath, tbName1, "where date>=2010.03.01")
        assert_frame_equal(loadTB(dbPath, tbName2), origin)
        assert_frame_equal(loadTB(dbPath, tbName1), origin)
        self.s.dropPartition(dbPath, ["2010.01.01", "2010.02.01"], tbName1)
        assert_frame_equal(loadTB(dbPath, tbName1), rs)
        assert_frame_equal(loadTB(dbPath, tbName2), origin)

        # tb2 dropPartition
        create_dfs_compo_range_value_list_db()
        origin = loadTB(dbPath, tbName1)
        rs = loadTB(dbPath, tbName1, "where date>=2010.03.01")
        assert_frame_equal(loadTB(dbPath, tbName1), origin)
        assert_frame_equal(loadTB(dbPath, tbName2), origin)
        self.s.dropPartition(dbPath, ["2010.01.01", "2010.02.01"], tbName2)
        assert_frame_equal(loadTB(dbPath, tbName2), rs)
        assert_frame_equal(loadTB(dbPath, tbName1), origin)

        # both dropPartition
        create_dfs_compo_range_value_list_db()
        origin = loadTB(dbPath, tbName1)
        rs = loadTB(dbPath, tbName1, "where date>=2010.03.01")
        assert_frame_equal(loadTB(dbPath, tbName1), origin)
        assert_frame_equal(loadTB(dbPath, tbName2), origin)
        self.s.dropPartition(dbPath, ["2010.01.01", "2010.02.01"])
        assert_frame_equal(loadTB(dbPath, tbName1), rs)
        assert_frame_equal(loadTB(dbPath, tbName2), rs)

    def test_dropPartition_dfs_compo3_range_value_list_drop_level2_single(self):
        dbPath = DBInfo.dfsDBName
        tbName1 = DBInfo.table1
        tbName2 = DBInfo.table2

        # tb1 dropPartition
        create_dfs_compo_range_value_list_db()
        origin = loadTB(dbPath, tbName1)
        rs = loadTB(dbPath, tbName1, "where date>=2010.02.01 or id!=1")
        assert_frame_equal(loadTB(dbPath, tbName2), origin)
        assert_frame_equal(loadTB(dbPath, tbName1), origin)
        self.s.dropPartition(dbPath, ["2010.01.01", "1"], tbName1)
        assert_frame_equal(loadTB(dbPath, tbName1), rs)
        assert_frame_equal(loadTB(dbPath, tbName2), origin)

        # tb2 dropPartition
        create_dfs_compo_range_value_list_db()
        origin = loadTB(dbPath, tbName1)
        rs = loadTB(dbPath, tbName1, "where date>=2010.02.01 or id!=1")
        assert_frame_equal(loadTB(dbPath, tbName1), origin)
        assert_frame_equal(loadTB(dbPath, tbName2), origin)
        self.s.dropPartition(dbPath, ["2010.01.01", "1"], tbName2)
        assert_frame_equal(loadTB(dbPath, tbName2), rs)
        assert_frame_equal(loadTB(dbPath, tbName1), origin)

        # both dropPartition
        create_dfs_compo_range_value_list_db()
        origin = loadTB(dbPath, tbName1)
        rs = loadTB(dbPath, tbName1, "where date>=2010.02.01 or id!=1")
        assert_frame_equal(loadTB(dbPath, tbName1), origin)
        assert_frame_equal(loadTB(dbPath, tbName2), origin)
        self.s.dropPartition(dbPath, ["2010.01.01", "1"])
        assert_frame_equal(loadTB(dbPath, tbName1), rs)
        assert_frame_equal(loadTB(dbPath, tbName2), rs)

    def test_dropPartition_dfs_compo3_range_value_list_drop_level2_multiple(self):
        dbPath = DBInfo.dfsDBName
        tbName1 = DBInfo.table1
        tbName2 = DBInfo.table2

        # tb1 dropPartition
        create_dfs_compo_range_value_list_db()
        origin = loadTB(dbPath, tbName1)
        rs = loadTB(dbPath, tbName1, "where date>=2010.02.01 or !in(id,1..3)")
        assert_frame_equal(loadTB(dbPath, tbName2), origin)
        assert_frame_equal(loadTB(dbPath, tbName1), origin)
        self.s.dropPartition(dbPath, ["2010.01.01", "1..3"], tbName1)
        assert_frame_equal(loadTB(dbPath, tbName1), rs)
        assert_frame_equal(loadTB(dbPath, tbName2), origin)

        # tb2 dropPartition
        create_dfs_compo_range_value_list_db()
        origin = loadTB(dbPath, tbName1)
        rs = loadTB(dbPath, tbName1, "where date>=2010.02.01 or !in(id,1..3)")
        assert_frame_equal(loadTB(dbPath, tbName1), origin)
        assert_frame_equal(loadTB(dbPath, tbName2), origin)
        self.s.dropPartition(dbPath, ["2010.01.01", "1..3"], tbName2)
        assert_frame_equal(loadTB(dbPath, tbName2), rs)
        assert_frame_equal(loadTB(dbPath, tbName1), origin)

        # both dropPartition
        create_dfs_compo_range_value_list_db()
        origin = loadTB(dbPath, tbName1)
        rs = loadTB(dbPath, tbName1, "where date>=2010.02.01 or !in(id,1..3)")
        assert_frame_equal(loadTB(dbPath, tbName1), origin)
        assert_frame_equal(loadTB(dbPath, tbName2), origin)
        self.s.dropPartition(dbPath, ["2010.01.01", "1..3"])
        assert_frame_equal(loadTB(dbPath, tbName1), rs)
        assert_frame_equal(loadTB(dbPath, tbName2), rs)

    def test_dropPartition_dfs_compo3_range_value_list_drop_level3_single(self):
        dbPath = DBInfo.dfsDBName
        tbName1 = DBInfo.table1
        tbName2 = DBInfo.table2

        # tb1 dropPartition
        create_dfs_compo_range_value_list_db()
        origin = loadTB(dbPath, tbName1)
        rs = loadTB(dbPath, tbName1, "where date>=2010.02.01 or id!=1 or !in(sym,`AMD`QWE`CES)")
        assert_frame_equal(loadTB(dbPath, tbName2), origin)
        assert_frame_equal(loadTB(dbPath, tbName1), origin)
        self.s.dropPartition(dbPath, ["2010.01.01", "1", "'AMD'"], tbName1)
        assert_frame_equal(loadTB(dbPath, tbName1), rs)
        assert_frame_equal(loadTB(dbPath, tbName2), origin)

        # tb2 dropPartition
        create_dfs_compo_range_value_list_db()
        origin = loadTB(dbPath, tbName1)
        rs = loadTB(dbPath, tbName1, "where date>=2010.02.01 or id!=1 or !in(sym,`AMD`QWE`CES)")
        assert_frame_equal(loadTB(dbPath, tbName1), origin)
        assert_frame_equal(loadTB(dbPath, tbName2), origin)
        self.s.dropPartition(dbPath, ["2010.01.01", "1", "'AMD'"], tbName2)
        assert_frame_equal(loadTB(dbPath, tbName2), rs)
        assert_frame_equal(loadTB(dbPath, tbName1), origin)

        # both dropPartition
        create_dfs_compo_range_value_list_db()
        origin = loadTB(dbPath, tbName1)
        rs = loadTB(dbPath, tbName1, "where date>=2010.02.01 or id!=1 or !in(sym,`AMD`QWE`CES)")
        assert_frame_equal(loadTB(dbPath, tbName1), origin)
        assert_frame_equal(loadTB(dbPath, tbName2), origin)
        self.s.dropPartition(dbPath, ["2010.01.01", "1", "'AMD'"])
        assert_frame_equal(loadTB(dbPath, tbName1), rs)
        assert_frame_equal(loadTB(dbPath, tbName2), rs)

    def test_dropPartition_dfs_compo3_range_value_list_drop_level3_multiple(self):
        dbPath = DBInfo.dfsDBName
        tbName1 = DBInfo.table1
        tbName2 = DBInfo.table2

        # tb1 dropPartition
        create_dfs_compo_range_value_list_db()
        origin = loadTB(dbPath, tbName1)
        rs = loadTB(dbPath, tbName1, "where date>=2010.02.01 or id!=1 or !in(sym,`DOP`ASZ`FSD`BBVC)")
        assert_frame_equal(loadTB(dbPath, tbName2), origin)
        assert_frame_equal(loadTB(dbPath, tbName1), origin)
        self.s.dropPartition(dbPath, ["2010.01.01", "1", "['DOP','FSD']"], tbName1)
        assert_frame_equal(loadTB(dbPath, tbName1), rs)
        assert_frame_equal(loadTB(dbPath, tbName2), origin)

        # tb2 dropPartition
        create_dfs_compo_range_value_list_db()
        origin = loadTB(dbPath, tbName1)
        rs = loadTB(dbPath, tbName1, "where date>=2010.02.01 or id!=1 or !in(sym,`DOP`ASZ`FSD`BBVC)")
        assert_frame_equal(loadTB(dbPath, tbName1), origin)
        assert_frame_equal(loadTB(dbPath, tbName2), origin)
        self.s.dropPartition(dbPath, ["2010.01.01", "1", "['DOP','FSD']"], tbName2)
        assert_frame_equal(loadTB(dbPath, tbName2), rs)
        assert_frame_equal(loadTB(dbPath, tbName1), origin)

        # both dropPartition
        create_dfs_compo_range_value_list_db()
        origin = loadTB(dbPath, tbName1)
        rs = loadTB(dbPath, tbName1, "where date>=2010.02.01 or id!=1 or !in(sym,`DOP`ASZ`FSD`BBVC)")
        assert_frame_equal(loadTB(dbPath, tbName1), origin)
        assert_frame_equal(loadTB(dbPath, tbName2), origin)
        self.s.dropPartition(dbPath, ["2010.01.01", "1", "['DOP','FSD']"])
        assert_frame_equal(loadTB(dbPath, tbName1), rs)
        assert_frame_equal(loadTB(dbPath, tbName2), rs)

    def test_dropPartition_dfs_compo3_range_value_list_drop_multiple(self):
        dbPath = DBInfo.dfsDBName
        tbName1 = DBInfo.table1
        tbName2 = DBInfo.table2

        # tb1 dropPartition
        create_dfs_compo_range_value_list_db()
        origin = loadTB(dbPath, tbName1)
        rs = loadTB(dbPath, tbName1, "where date>=2010.03.01 or !in(id,1..3) or !in(sym,`DOP`ASZ`FSD`BBVC)")
        assert_frame_equal(loadTB(dbPath, tbName2), origin)
        assert_frame_equal(loadTB(dbPath, tbName1), origin)
        self.s.dropPartition(dbPath, ["[2010.01.01,2010.02.01]", "1..3", "['DOP','FSD']"], tbName1)
        assert_frame_equal(loadTB(dbPath, tbName1), rs)
        assert_frame_equal(loadTB(dbPath, tbName2), origin)

        # tb2 dropPartition
        create_dfs_compo_range_value_list_db()
        origin = loadTB(dbPath, tbName1)
        rs = loadTB(dbPath, tbName1, "where date>=2010.03.01 or !in(id,1..3) or !in(sym,`DOP`ASZ`FSD`BBVC)")
        assert_frame_equal(loadTB(dbPath, tbName1), origin)
        assert_frame_equal(loadTB(dbPath, tbName2), origin)
        self.s.dropPartition(dbPath, ["[2010.01.01,2010.02.01]", "1..3", "['DOP','FSD']"], tbName2)
        assert_frame_equal(loadTB(dbPath, tbName2), rs)
        assert_frame_equal(loadTB(dbPath, tbName1), origin)

        # both dropPartition
        create_dfs_compo_range_value_list_db()
        origin = loadTB(dbPath, tbName1)
        rs = loadTB(dbPath, tbName1, "where date>=2010.03.01 or !in(id,1..3) or !in(sym,`DOP`ASZ`FSD`BBVC)")
        assert_frame_equal(loadTB(dbPath, tbName1), origin)
        assert_frame_equal(loadTB(dbPath, tbName2), origin)
        self.s.dropPartition(dbPath, ["[2010.01.01,2010.02.01]", "1..3", "['DOP','FSD']"])
        assert_frame_equal(loadTB(dbPath, tbName1), rs)
        assert_frame_equal(loadTB(dbPath, tbName2), rs)
