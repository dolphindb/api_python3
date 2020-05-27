import unittest
import dolphindb as ddb
from pandas.testing import assert_frame_equal
from setup import HOST, PORT, WORK_DIR, DATA_DIR


class DBInfo:
    dfsDBName = 'dfs://testLoadTextEx'
    diskDBName = WORK_DIR + '/testLoadTextEx'
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
    '''.format(db=DBInfo.dfsDBName)
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
    '''.format(db=DBInfo.dfsDBName)
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
    '''.format(db=DBInfo.dfsDBName)
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
    '''.format(db=DBInfo.dfsDBName)
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
    '''.format(db=DBInfo.dfsDBName)
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
    '''.format(db=DBInfo.dfsDBName)
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
    '''.format(db=DBInfo.dfsDBName)
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
    '''.format(db=DBInfo.dfsDBName)
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
    '''.format(db=DBInfo.dfsDBName)
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
    '''.format(db=DBInfo.dfsDBName)
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
    '''.format(db=DBInfo.diskDBName)
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
    '''.format(db=DBInfo.diskDBName)
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
    '''.format(db=DBInfo.diskDBName)
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
    '''.format(db=DBInfo.diskDBName)
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
    '''.format(db=DBInfo.diskDBName)
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
    '''.format(db=DBInfo.diskDBName)
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
    '''.format(db=DBInfo.diskDBName)
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
    '''.format(db=DBInfo.diskDBName)
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
    '''.format(db=DBInfo.diskDBName)
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
    '''.format(db=DBInfo.diskDBName)
    s.run(ddb_script)
    s.close()


class LoadTextExTest(unittest.TestCase):
    @classmethod
    def setUp(cls):
        cls.s = ddb.session()
        cls.s.connect(HOST, PORT, "admin", "123456")
        cls.data1 = DATA_DIR + "/loadTextExTest1.csv"
        cls.data2 = DATA_DIR + "/loadTextExTest2.csv"

    @classmethod
    def tearDown(cls):
        pass

    def test_loadTextEx_dfs_range(self):
        create_dfs_range_db()
        tmp = self.s.loadTextEx(DBInfo.dfsDBName, DBInfo.table1, "`id", self.data1)
        rs = self.s.run("select * from loadTextEx(database('{db}'), `{tb}, `id, '{data}')"
                        .format(db=DBInfo.dfsDBName, tb=DBInfo.table2, data=self.data1))
        assert_frame_equal(tmp.toDF(), rs)

    def test_loadTextEx_dfs_hash(self):
        create_dfs_hash_db()
        tmp = self.s.loadTextEx(DBInfo.dfsDBName, DBInfo.table1, "`id", self.data1)
        rs = self.s.run("select * from loadTextEx(database('{db}'), `{tb}, `id, '{data}')"
                        .format(db=DBInfo.dfsDBName, tb=DBInfo.table2, data=self.data1))
        assert_frame_equal(tmp.toDF(), rs)

    def test_loadTextEx_dfs_value(self):
        create_dfs_value_db()
        tmp = self.s.loadTextEx(DBInfo.dfsDBName, DBInfo.table1, "`date", self.data2)
        rs = self.s.run("select * from loadTextEx(database('{db}'), `{tb}, `date, '{data}')"
                        .format(db=DBInfo.dfsDBName, tb=DBInfo.table2, data=self.data2))
        assert_frame_equal(tmp.toDF(), rs)

    def test_loadTextEx_dfs_list(self):
        create_dfs_list_db()
        tmp = self.s.loadTextEx(DBInfo.dfsDBName, DBInfo.table1, "`sym", self.data2)
        rs = self.s.run("select * from loadTextEx(database('{db}'), `{tb}, `sym, '{data}')"
                        .format(db=DBInfo.dfsDBName, tb=DBInfo.table2, data=self.data2))
        assert_frame_equal(tmp.toDF(), rs)

    def test_loadTextEx_dfs_compo_range_range(self):
        create_dfs_compo_range_range_db()
        tmp = self.s.loadTextEx(DBInfo.dfsDBName, DBInfo.table1, "`date`id", self.data1)
        rs = self.s.run("select * from loadTextEx(database('{db}'), `{tb}, `date`id, '{data}')"
                        .format(db=DBInfo.dfsDBName, tb=DBInfo.table2, data=self.data1))
        assert_frame_equal(tmp.toDF(), rs)

    def test_loadTextEx_dfs_compo_range_hash(self):
        create_dfs_compo_range_hash_db()
        tmp = self.s.loadTextEx(DBInfo.dfsDBName, DBInfo.table1, "`date`id", self.data1)
        rs = self.s.run("select * from loadTextEx(database('{db}'), `{tb}, `date`id, '{data}')"
                        .format(db=DBInfo.dfsDBName, tb=DBInfo.table2, data=self.data1))
        assert_frame_equal(tmp.toDF(), rs)

    def test_loadTextEx_dfs_compo_range_value(self):
        create_dfs_compo_range_value_db()
        tmp = self.s.loadTextEx(DBInfo.dfsDBName, DBInfo.table1, "`date`id", self.data1)
        rs = self.s.run("select * from loadTextEx(database('{db}'), `{tb}, `date`id, '{data}')"
                        .format(db=DBInfo.dfsDBName, tb=DBInfo.table2, data=self.data1))
        assert_frame_equal(tmp.toDF(), rs)

    def test_loadTextEx_dfs_compo_range_list(self):
        create_dfs_compo_range_list_db()
        tmp = self.s.loadTextEx(DBInfo.dfsDBName, DBInfo.table1, "`date`sym", self.data1)
        rs = self.s.run("select * from loadTextEx(database('{db}'), `{tb}, `date`sym, '{data}')"
                        .format(db=DBInfo.dfsDBName, tb=DBInfo.table2, data=self.data1))
        assert_frame_equal(tmp.toDF(), rs)

    def test_loadTextEx_dfs_compo_range_hash_list(self):
        create_dfs_compo_range_hash_list_db()
        tmp = self.s.loadTextEx(DBInfo.dfsDBName, DBInfo.table1, "`date`id`sym", self.data1)
        rs = self.s.run("select * from loadTextEx(database('{db}'), `{tb}, `date`id`sym, '{data}')"
                        .format(db=DBInfo.dfsDBName, tb=DBInfo.table2, data=self.data1))
        assert_frame_equal(tmp.toDF(), rs)

    def test_loadTextEx_dfs_compo_range_value_list(self):
        create_dfs_compo_range_value_list_db()
        tmp = self.s.loadTextEx(DBInfo.dfsDBName, DBInfo.table1, "`date`id`sym", self.data1)
        rs = self.s.run("select * from loadTextEx(database('{db}'), `{tb}, `date`id`sym, '{data}')"
                        .format(db=DBInfo.dfsDBName, tb=DBInfo.table2, data=self.data1))
        assert_frame_equal(tmp.toDF(), rs)

    def test_loadTextEx_disk_range(self):
        create_disk_range_db()
        tmp = self.s.loadTextEx(DBInfo.diskDBName, DBInfo.table1, "`id", self.data1)
        rs = self.s.run("select * from loadTextEx(database('{db}'), `{tb}, `id, '{data}')"
                        .format(db=DBInfo.diskDBName, tb=DBInfo.table2, data=self.data1))
        assert_frame_equal(tmp.toDF(), rs)

    def test_loadTextEx_disk_hash(self):
        create_disk_hash_db()
        tmp = self.s.loadTextEx(DBInfo.diskDBName, DBInfo.table1, "`id", self.data1)
        rs = self.s.run("select * from loadTextEx(database('{db}'), `{tb}, `id, '{data}')"
                        .format(db=DBInfo.diskDBName, tb=DBInfo.table2, data=self.data1))
        assert_frame_equal(tmp.toDF(), rs)

    def test_loadTextEx_disk_value(self):
        create_disk_value_db()
        tmp = self.s.loadTextEx(DBInfo.diskDBName, DBInfo.table1, "`date", self.data2)
        rs = self.s.run("select * from loadTextEx(database('{db}'), `{tb}, `date, '{data}')"
                        .format(db=DBInfo.diskDBName, tb=DBInfo.table2, data=self.data2))
        assert_frame_equal(tmp.toDF(), rs)

    def test_loadTextEx_disk_list(self):
        create_disk_list_db()
        tmp = self.s.loadTextEx(DBInfo.diskDBName, DBInfo.table1, "`sym", self.data2)
        rs = self.s.run("select * from loadTextEx(database('{db}'), `{tb}, `sym, '{data}')"
                        .format(db=DBInfo.diskDBName, tb=DBInfo.table2, data=self.data2))
        assert_frame_equal(tmp.toDF(), rs)

    def test_loadTextEx_disk_compo_range_range(self):
        create_disk_compo_range_range_db()
        tmp = self.s.loadTextEx(DBInfo.diskDBName, DBInfo.table1, "`date`id", self.data1)
        rs = self.s.run("select * from loadTextEx(database('{db}'), `{tb}, `date`id, '{data}')"
                        .format(db=DBInfo.diskDBName, tb=DBInfo.table2, data=self.data1))
        assert_frame_equal(tmp.toDF(), rs)

    def test_loadTextEx_disk_compo_range_hash(self):
        create_disk_compo_range_hash_db()
        tmp = self.s.loadTextEx(DBInfo.diskDBName, DBInfo.table1, "`date`id", self.data1)
        rs = self.s.run("select * from loadTextEx(database('{db}'), `{tb}, `date`id, '{data}')"
                        .format(db=DBInfo.diskDBName, tb=DBInfo.table2, data=self.data1))
        assert_frame_equal(tmp.toDF(), rs)

    def test_loadTextEx_disk_compo_range_value(self):
        create_disk_compo_range_value_db()
        tmp = self.s.loadTextEx(DBInfo.diskDBName, DBInfo.table1, "`date`id", self.data1)
        rs = self.s.run("select * from loadTextEx(database('{db}'), `{tb}, `date`id, '{data}')"
                        .format(db=DBInfo.diskDBName, tb=DBInfo.table2, data=self.data1))
        assert_frame_equal(tmp.toDF(), rs)

    def test_loadTextEx_disk_compo_range_list(self):
        create_disk_compo_range_list_db()
        tmp = self.s.loadTextEx(DBInfo.diskDBName, DBInfo.table1, "`date`sym", self.data1)
        rs = self.s.run("select * from loadTextEx(database('{db}'), `{tb}, `date`sym, '{data}')"
                        .format(db=DBInfo.diskDBName, tb=DBInfo.table2, data=self.data1))
        assert_frame_equal(tmp.toDF(), rs)

    def test_loadTextEx_disk_compo_range_hash_list(self):
        create_disk_compo_range_hash_list_db()
        tmp = self.s.loadTextEx(DBInfo.diskDBName, DBInfo.table1, "`date`id`sym", self.data1)
        rs = self.s.run("select * from loadTextEx(database('{db}'), `{tb}, `date`id`sym, '{data}')"
                        .format(db=DBInfo.diskDBName, tb=DBInfo.table2, data=self.data1))
        assert_frame_equal(tmp.toDF(), rs)

    def test_loadTextEx_disk_compo_range_value_list(self):
        create_disk_compo_range_value_list_db()
        tmp = self.s.loadTextEx(DBInfo.diskDBName, DBInfo.table1, "`date`id`sym", self.data1)
        rs = self.s.run("select * from loadTextEx(database('{db}'), `{tb}, `date`id`sym, '{data}')"
                        .format(db=DBInfo.diskDBName, tb=DBInfo.table2, data=self.data1))
        assert_frame_equal(tmp.toDF(), rs)


if __name__ == '__main__':
    unittest.main()