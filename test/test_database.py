import unittest
import dolphindb as ddb
import dolphindb.settings as keys
import numpy as np
from numpy.testing import *
from setup import HOST, PORT, WORK_DIR


class DBInfo:
    dfsDBName = 'dfs://testDatabase'
    diskDBName = WORK_DIR + '/testDatabase'


def existsDB(dbName):
    s = ddb.session()
    s.connect(HOST, PORT, "admin", "123456")
    return s.run("existsDatabase('{db}')".format(db=dbName))


def dropDB(dbName):
    s = ddb.session()
    s.connect(HOST, PORT, "admin", "123456")
    s.run("dropDatabase('{db}')".format(db=dbName))


class DatabaseTest(unittest.TestCase):
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

    def test_create_dfs_database_range_partition(self):
        if existsDB(DBInfo.dfsDBName):
            dropDB(DBInfo.dfsDBName)
        self.s.database('db', partitionType=keys.RANGE, partitions=[1, 11, 21], dbPath=DBInfo.dfsDBName)
        self.assertEqual(existsDB(DBInfo.dfsDBName), True)

        dct = {'databaseDir': DBInfo.dfsDBName,
               'partitionSchema': np.array([1, 11, 21], dtype=np.int32),
               'partitionSites': None,
               'partitionType': 2}
        self.assertEqual(repr(self.s.run("schema(db)")), repr(dct))

    def test_create_dfs_database_hash_partition(self):
        if existsDB(DBInfo.dfsDBName):
            dropDB(DBInfo.dfsDBName)
        self.s.database('db', partitionType=keys.HASH, partitions=[keys.DT_INT, 2], dbPath=DBInfo.dfsDBName)
        self.assertEqual(existsDB(DBInfo.dfsDBName), True)

        dct = {'databaseDir': DBInfo.dfsDBName,
               'partitionSchema': 2,
               'partitionSites': None,
               'partitionType': 5}
        self.assertEqual(repr(self.s.run("schema(db)")), repr(dct))

    def test_create_dfs_database_value_partition(self):
        if existsDB(DBInfo.dfsDBName):
            dropDB(DBInfo.dfsDBName)
        self.s.database('db', partitionType=keys.VALUE, partitions=[1, 2, 3], dbPath=DBInfo.dfsDBName)
        self.assertEqual(existsDB(DBInfo.dfsDBName), True)

        dct = {'databaseDir': DBInfo.dfsDBName,
               'partitionSchema': np.array([3, 1, 2], dtype=np.int32),
               'partitionSites': None,
               'partitionType': 1}
        self.assertEqual(repr(self.s.run("schema(db)")), repr(dct))

    def test_create_dfs_database_list_partition(self):
        if existsDB(DBInfo.dfsDBName):
            dropDB(DBInfo.dfsDBName)
        self.s.database('db', partitionType=keys.LIST, partitions=[['IBM', 'ORCL', 'MSFT'], ['GOOG', 'FB']],
                        dbPath=DBInfo.dfsDBName)
        self.assertEqual(existsDB(DBInfo.dfsDBName), True)

        dct = {'databaseDir': DBInfo.dfsDBName,
               'partitionSchema': np.array([np.array(['IBM', 'ORCL', 'MSFT']), np.array(['GOOG', 'FB'])]),
               'partitionSites': None,
               'partitionType': 3}
        self.assertEqual(repr(self.s.run("schema(db)")), repr(dct))

    def test_create_dfs_database_compo_partition(self):
        if existsDB(DBInfo.dfsDBName):
            dropDB(DBInfo.dfsDBName)
        # TODO: BUG
        # self.s.database('db1', partitionType=keys.VALUE,
        #            partitions=[np.datetime64("2012-01-01"), np.datetime64("2012-01-06")], dbPath='')
        # self.s.database('db2', partitionType=keys.RANGE,
        #                 partitions=[1, 6, 11], dbPath='')
        # TODO: COMPO BUG
        # self.s.database('db', keys.COMPO, partitions=["db1", "db2"], dbPath=DBInfo.dfsDBName)
        # self.assertEqual(existsDB(DBInfo.dfsDBName), True)


if __name__ == '__main__':
    unittest.main()
