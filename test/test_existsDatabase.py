import unittest
import dolphindb as ddb
from os import path
from setup import HOST, PORT, WORK_DIR


class DBInfo:
    dfsDBName = 'dfs://testExistsDatabase'
    diskDBName = WORK_DIR + '/testExistsDatabase'
    table = 'tb1'


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
    db.createTable(tdata,`{tb}).append!(tdata)
    '''.format(db=DBInfo.dfsDBName, tb=DBInfo.table)
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
    db.createPartitionedTable(tdata,`{tb},`id).append!(tdata)
    '''.format(db=DBInfo.dfsDBName, tb=DBInfo.table)
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
    db.createPartitionedTable(tdata,`{tb},`id).append!(tdata)
    '''.format(db=DBInfo.dfsDBName, tb=DBInfo.table)
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
    db.createPartitionedTable(tdata,`{tb},`date).append!(tdata)
    '''.format(db=DBInfo.dfsDBName, tb=DBInfo.table)
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
    db.createPartitionedTable(tdata,`{tb},`sym).append!(tdata)
    '''.format(db=DBInfo.dfsDBName, tb=DBInfo.table)
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
    db.createPartitionedTable(tdata,`{tb},`date`id).append!(tdata)
    '''.format(db=DBInfo.dfsDBName, tb=DBInfo.table)
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
    saveTable(db,tdata)
    '''.format(db=DBInfo.diskDBName)
    s.run(ddb_script)
    s.close()


def create_disk_range_db():
    s = ddb.session()
    s.connect(HOST, PORT, "admin", "123456")
    ddb_script = '''
    login('admin','123456')
    dbPath='{db}'
    if(exists(dbPath))
        dropDatabase(dbPath)
    db=database(dbPath,RANGE,0..10*10000+1)
    n=100000
    tdata=table(sort(take(2010.01.01..2010.12.31, n)) as date, 1..n as id,take(`AMD`QWE`CES`DOP`ASZ`FSD`BBVC`AWQ`DS, n) as sym,rand(100,n) as val)
    db.createPartitionedTable(tdata,`{tb},`id).append!(tdata)
    '''.format(db=DBInfo.diskDBName, tb=DBInfo.table)
    s.run(ddb_script)
    s.close()


def create_disk_hash_db():
    s = ddb.session()
    s.connect(HOST, PORT, "admin", "123456")
    ddb_script = '''
    login('admin','123456')
    dbPath='{db}'
    if(exists(dbPath))
        dropDatabase(dbPath)
    db=database(dbPath,HASH,[INT,10])
    n=100000
    tdata=table(sort(take(2010.01.01..2010.12.31, n)) as date, take(1..10,n) as id,take(`AMD`QWE`CES`DOP`ASZ`FSD`BBVC`AWQ`DS, n) as sym,rand(100,n) as val)
    db.createPartitionedTable(tdata,`{tb},`id).append!(tdata)
    '''.format(db=DBInfo.diskDBName, tb=DBInfo.table)
    s.run(ddb_script)
    s.close()


def create_disk_value_db():
    s = ddb.session()
    s.connect(HOST, PORT, "admin", "123456")
    ddb_script = '''
    login('admin','123456')
    dbPath='{db}'
    if(exists(dbPath))
        dropDatabase(dbPath)
    db=database(dbPath,VALUE,2010.01.01..2010.01.30)
    n=100000
    tdata=table(sort(take(2010.01.01..2010.01.30, n)) as date, take(1..10,n) as id,take(`AMD`QWE`CES`DOP`ASZ`FSD`BBVC`AWQ`DS, n) as sym,rand(100,n) as val)
    db.createPartitionedTable(tdata,`{tb},`date).append!(tdata)
    '''.format(db=DBInfo.diskDBName, tb=DBInfo.table)
    s.run(ddb_script)
    s.close()


def create_disk_list_db():
    s = ddb.session()
    s.connect(HOST, PORT, "admin", "123456")
    ddb_script = '''
    login('admin','123456')
    dbPath='{db}'
    if(exists(dbPath))
        dropDatabase(dbPath)
    db=database(dbPath,LIST,[`AMD`QWE`CES,`DOP`ASZ,`FSD`BBVC,`AWQ`DS])
    n=100000
    tdata=table(sort(take(2010.01.01..2010.12.31, n)) as date, take(1..10,n) as id,take(`AMD`QWE`CES`DOP`ASZ`FSD`BBVC`AWQ`DS, n) as sym,rand(100,n) as val)
    db.createPartitionedTable(tdata,`{tb},`sym).append!(tdata)
    '''.format(db=DBInfo.diskDBName, tb=DBInfo.table)
    s.run(ddb_script)
    s.close()


def create_disk_compo_range_range_db():
    s = ddb.session()
    s.connect(HOST, PORT, "admin", "123456")
    ddb_script = '''
    login('admin','123456')
    dbPath='{db}'
    if(exists(dbPath))
        //dropDatabase(dbPath)
        rmdir(dbPath, true)
    db1=database('',RANGE,2010.01M+0..12)
    db2=database('',RANGE,1 3 5 7 9 11)
    db=database(dbPath,COMPO,[db1,db2])
    n=100000
    tdata=table(sort(take(2010.01.01..2010.12.31, n)) as date, take(1..10,n) as id,take(`AMD`QWE`CES`DOP`ASZ`FSD`BBVC`AWQ`DS, n) as sym,rand(100,n) as val)
    db.createPartitionedTable(tdata,`{tb},`date`id).append!(tdata)
    '''.format(db=DBInfo.diskDBName, tb=DBInfo.table)
    s.run(ddb_script)
    s.close()


def dropDB(dbName):
    s = ddb.session()
    s.connect(HOST, PORT, "admin", "123456")
    s.run("dropDatabase('{db}')".format(db=dbName))


class ExistsDatabaseTest(unittest.TestCase):
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

    def test_existsDatabase_dfs_dimension(self):
        dbName = DBInfo.dfsDBName
        self.assertEqual(self.s.existsDatabase(dbName), False)
        create_dfs_dimension_db()
        self.assertEqual(self.s.existsDatabase(dbName), True)
        dropDB(dbName)
        self.assertEqual(self.s.existsDatabase(dbName), False)

    def test_existsDatabase_dfs_range(self):
        dbName = DBInfo.dfsDBName
        self.assertEqual(self.s.existsDatabase(dbName), False)
        create_dfs_range_db()
        self.assertEqual(self.s.existsDatabase(dbName), True)
        dropDB(dbName)
        self.assertEqual(self.s.existsDatabase(dbName), False)

    def test_existsDatabase_dfs_hash(self):
        dbName = DBInfo.dfsDBName
        self.assertEqual(self.s.existsDatabase(dbName), False)
        create_dfs_hash_db()
        self.assertEqual(self.s.existsDatabase(dbName), True)
        dropDB(dbName)
        self.assertEqual(self.s.existsDatabase(dbName), False)

    def test_existsDatabase_dfs_value(self):
        dbName = DBInfo.dfsDBName
        self.assertEqual(self.s.existsDatabase(dbName), False)
        create_dfs_value_db()
        self.assertEqual(self.s.existsDatabase(dbName), True)
        dropDB(dbName)
        self.assertEqual(self.s.existsDatabase(dbName), False)

    def test_existsDatabase_dfs_list(self):
        dbName = DBInfo.dfsDBName
        self.assertEqual(self.s.existsDatabase(dbName), False)
        create_dfs_list_db()
        self.assertEqual(self.s.existsDatabase(dbName), True)
        dropDB(dbName)
        self.assertEqual(self.s.existsDatabase(dbName), False)

    def test_existsDatabase_dfs_compo_range_range(self):
        dbName = DBInfo.dfsDBName
        self.assertEqual(self.s.existsDatabase(dbName), False)
        create_dfs_compo_range_range_db()
        self.assertEqual(self.s.existsDatabase(dbName), True)
        dropDB(dbName)
        self.assertEqual(self.s.existsDatabase(dbName), False)

    def test_existsDatabase_disk_unpartitioned(self):
        dbName = DBInfo.diskDBName
        self.assertEqual(path.exists(dbName), False)
        create_disk_unpartitioned_db()
        self.assertEqual(path.exists(dbName), True)
        dropDB(dbName)
        self.assertEqual(path.exists(dbName), False)

    def test_existsDatabase_disk_range(self):
        dbName = DBInfo.diskDBName
        self.assertEqual(self.s.existsDatabase(dbName), False)
        create_disk_range_db()
        self.assertEqual(self.s.existsDatabase(dbName), True)
        dropDB(dbName)
        self.assertEqual(self.s.existsDatabase(dbName), False)

    def test_existsDatabase_disk_hash(self):
        dbName = DBInfo.diskDBName
        self.assertEqual(self.s.existsDatabase(dbName), False)
        create_disk_hash_db()
        self.assertEqual(self.s.existsDatabase(dbName), True)
        dropDB(dbName)
        self.assertEqual(self.s.existsDatabase(dbName), False)

    def test_existsDatabase_disk_value(self):
        dbName = DBInfo.diskDBName
        self.assertEqual(self.s.existsDatabase(dbName), False)
        create_disk_value_db()
        self.assertEqual(self.s.existsDatabase(dbName), True)
        dropDB(dbName)
        self.assertEqual(self.s.existsDatabase(dbName), False)

    def test_existsDatabase_disk_list(self):
        dbName = DBInfo.diskDBName
        self.assertEqual(self.s.existsDatabase(dbName), False)
        create_disk_list_db()
        self.assertEqual(self.s.existsDatabase(dbName), True)
        dropDB(dbName)
        self.assertEqual(self.s.existsDatabase(dbName), False)

    def test_existsDatabase_disk_compo_range_range(self):
        dbName = DBInfo.diskDBName
        self.assertEqual(self.s.existsDatabase(dbName), False)
        create_disk_compo_range_range_db()
        self.assertEqual(self.s.existsDatabase(dbName), True)
        dropDB(dbName)
        self.assertEqual(self.s.existsDatabase(dbName), False)


if __name__ == '__main__':
    unittest.main()
