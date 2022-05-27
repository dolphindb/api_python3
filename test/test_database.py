from difflib import diff_bytes
import unittest
import dolphindb as ddb
import dolphindb.settings as keys
import numpy as np
from numpy.testing import *
from pandas.testing import assert_frame_equal
from setup import HOST, PORT, WORK_DIR
import pandas as pd
from setup.settings import WORK_DIR

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
        cls.s_compress = ddb.session(compress=True)
        cls.s_compress.connect(HOST, PORT, "admin", "123456")
        cls.pool = ddb.DBConnectionPool(HOST, PORT, 10, "admin", "123456", compress=True)
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
        db = self.s.database('db', partitionType=keys.RANGE, partitions=[1, 11, 21], dbPath=DBInfo.dfsDBName)
        self.assertEqual(existsDB(DBInfo.dfsDBName), True)

        dct = {'databaseDir': DBInfo.dfsDBName,
               'partitionSchema': np.array([1, 11, 21]),
               'partitionSites': None,
               'partitionTypeName': 'RANGE',
               'partitionType': 2}
        re = self.s.run("schema(db)")
        self.assertEqual(re['databaseDir'], dct['databaseDir'])
        assert_array_equal(re['partitionSchema'], dct['partitionSchema'])
        self.assertEqual(re['partitionSites'], dct['partitionSites'])
        df = pd.DataFrame({'id': np.arange(1, 21), 'val': np.repeat(1, 20)})
        t = self.s.table(data=df, tableAliasName='t')
        db.createPartitionedTable(table=t, tableName='pt', partitionColumns='id').append(t)
        re = self.s.loadTable(tableName='pt', dbPath=DBInfo.dfsDBName).toDF()
        assert_array_equal(re['id'], np.arange(1, 21))
        assert_array_equal(re['val'], np.repeat(1, 20))
        db.createTable(table=t, tableName='dt').append(t)
        re = self.s.loadTable(tableName='dt', dbPath=DBInfo.dfsDBName).toDF()
        assert_array_equal(re['id'], np.arange(1, 21))
        assert_array_equal(re['val'], np.repeat(1, 20))

    def test_create_dfs_database_hash_partition(self):
        if existsDB(DBInfo.dfsDBName):
            dropDB(DBInfo.dfsDBName)
        db = self.s.database('db', partitionType=keys.HASH, partitions=[keys.DT_INT, 2], dbPath=DBInfo.dfsDBName)
        self.assertEqual(existsDB(DBInfo.dfsDBName), True)

        dct = {'databaseDir': DBInfo.dfsDBName,
               'partitionSchema': 2,
               'partitionSites': None,
               'partitionTypeName': 'HASH',
               'partitionType': 5}
        re = self.s.run("schema(db)")
        self.assertEqual(re['databaseDir'], dct['databaseDir'])
        self.assertEqual(re['partitionSchema'], dct['partitionSchema'])
        self.assertEqual(re['partitionSites'], dct['partitionSites'])
        df = pd.DataFrame({'id':[1,2,3,4,5], 'val':[10, 20, 30, 40, 50]})
        t = self.s.table(data=df)
        pt = db.createPartitionedTable(table=t, tableName='pt', partitionColumns='id')
        pt.append(t)
        re = self.s.loadTable(tableName='pt', dbPath=DBInfo.dfsDBName).toDF()
        assert_array_equal(np.sort(re['id']), df['id'])
        assert_array_equal(np.sort(re['val']), df['val'])
        dt = db.createTable(table=t, tableName='dt')
        dt.append(t)
        re = self.s.loadTable(tableName='pt', dbPath=DBInfo.dfsDBName).toDF()
        assert_array_equal(np.sort(re['id']), df['id'])
        assert_array_equal(np.sort(re['val']), df['val'])

    def test_create_dfs_database_value_partition(self):
        if existsDB(DBInfo.dfsDBName):
            dropDB(DBInfo.dfsDBName)
        db = self.s.database('db', partitionType=keys.VALUE, partitions=[1, 2, 3], dbPath=DBInfo.dfsDBName)
        self.assertEqual(existsDB(DBInfo.dfsDBName), True)

        dct = {'databaseDir': DBInfo.dfsDBName,
               'partitionSchema': np.array([3, 1, 2]),
               'partitionSites': None,
               'partitionTypeName': 'VALUE',
               'partitionType': 1}
        re = self.s.run("schema(db)")
        self.assertEqual(re['databaseDir'], dct['databaseDir'])
        assert_array_equal(re['partitionSchema'], dct['partitionSchema'])
        self.assertEqual(re['partitionSites'], dct['partitionSites'])
        df = pd.DataFrame({'id':[1, 2, 3, 1, 2, 3], 'val':[11, 12, 13, 14, 15, 16]})
        t = self.s.table(data=df)
        pt = db.createPartitionedTable(table=t, tableName='pt', partitionColumns='id').append(t)
        re = self.s.loadTable(tableName='pt', dbPath=DBInfo.dfsDBName).toDF()
        assert_array_equal(np.sort(df['id']), np.sort(re['id']))
        assert_array_equal(np.sort(df['val']), np.sort(re['val']))
        dt = db.createTable(table=t, tableName='dt').append(t)
        re = self.s.loadTable(tableName='dt', dbPath=DBInfo.dfsDBName).toDF()
        assert_array_equal(np.sort(df['id']), np.sort(re['id']))
        assert_array_equal(np.sort(df['val']), np.sort(re['val']))



    def test_create_dfs_database_list_partition(self):
        if existsDB(DBInfo.dfsDBName):
            dropDB(DBInfo.dfsDBName)
        db = self.s.database('db', partitionType=keys.LIST, partitions=[['IBM', 'ORCL', 'MSFT'], ['GOOG', 'FB']],
                        dbPath=DBInfo.dfsDBName)
        self.assertEqual(existsDB(DBInfo.dfsDBName), True)

        dct = {'databaseDir': DBInfo.dfsDBName,
               'partitionSchema': np.array([np.array(['IBM', 'ORCL', 'MSFT']), np.array(['GOOG', 'FB'])]),
               'partitionSites': None,
               'partitionTypeName': 'LIST',
               'partitionType': 3}
        re = self.s.run("schema(db)")
        self.assertEqual(re['databaseDir'], dct['databaseDir'])
        assert_array_equal(re['partitionSchema'][0], dct['partitionSchema'][0])
        assert_array_equal(re['partitionSchema'][1], dct['partitionSchema'][1])
        self.assertEqual(re['partitionSites'], dct['partitionSites'])
        df = pd.DataFrame({'sym':['IBM', 'ORCL', 'MSFT', 'GOOG', 'FB'], 'val':[1,2,3,4,5]})
        t = self.s.table(data=df)
        db.createPartitionedTable(table=t, tableName='pt', partitionColumns='sym').append(t)
        re = self.s.loadTable(tableName='pt', dbPath=DBInfo.dfsDBName).toDF()
        assert_array_equal(re['sym'], df['sym'])
        assert_array_equal(re['val'], df['val'])
        db.createTable(table=t, tableName='dt').append(t)
        re = self.s.loadTable(tableName='dt', dbPath=DBInfo.dfsDBName).toDF()
        assert_array_equal(re['sym'], df['sym'])
        assert_array_equal(re['val'], df['val'])



    def test_create_dfs_database_value_partition_np_date(self):
        if existsDB(DBInfo.dfsDBName):
            dropDB(DBInfo.dfsDBName)
        dates=np.array(pd.date_range(start='20120101', end='20120110'), dtype="datetime64[D]")
        db = self.s.database('db', partitionType=keys.VALUE, partitions=dates,
                        dbPath=DBInfo.dfsDBName)
        
        self.assertEqual(existsDB(DBInfo.dfsDBName), True)

        dct = {'databaseDir': DBInfo.dfsDBName,
                'partitionType': 1,
               'partitionSchema': np.array(pd.date_range(start='20120101', end='20120110'), dtype="datetime64[D]"),
               'partitionSites': None
               }
        re = self.s.run("schema(db)")
        self.assertEqual(re['databaseDir'], dct['databaseDir'])
        self.assertEqual(re['partitionType'], dct['partitionType'])
        assert_array_equal(np.sort(re['partitionSchema']), dct['partitionSchema'])
        df = pd.DataFrame({'datetime':np.array(['2012-01-01T00:00:00', '2012-01-02T00:00:00'], dtype='datetime64'), 'sym':['AA', 'BB'], 'val':[1,2]})
        t = self.s.table(data=df)
        db.createPartitionedTable(table=t, tableName='pt', partitionColumns='datetime').append(t)
        re = self.s.run("schema(loadTable('{dbPath}', 'pt')).colDefs".format(dbPath=DBInfo.dfsDBName))
        assert_array_equal(re['name'], ['datetime', 'sym', 'val'])
        assert_array_equal(re['typeString'], ['NANOTIMESTAMP', 'STRING', 'LONG'])
        re = self.s.loadTable(tableName='pt', dbPath=DBInfo.dfsDBName).toDF()
        assert_array_equal(re['datetime'], df['datetime'])
        assert_array_equal(re['sym'], df['sym'])
        assert_array_equal(re['val'], df['val'])
        db.createTable(table=t, tableName='dt').append(t)
        re = self.s.loadTable(tableName='dt', dbPath=DBInfo.dfsDBName).toDF()
        assert_array_equal(re['datetime'], df['datetime'])
        assert_array_equal(re['sym'], df['sym'])
        assert_array_equal(re['val'], df['val'])

    def test_create_dfs_database_value_partition_np_month(self):
        if existsDB(DBInfo.dfsDBName):
            dropDB(DBInfo.dfsDBName)
        months=np.array(pd.date_range(start='2012-01', end='2012-10', freq="M"), dtype="datetime64[M]")
        db = self.s.database('db', partitionType=keys.VALUE, partitions=months,
                       dbPath=DBInfo.dfsDBName)
        self.assertEqual(existsDB(DBInfo.dfsDBName), True)

        dct = {'databaseDir': DBInfo.dfsDBName,
                'partitionType': 1,
               'partitionSchema': months,
               'partitionSites': None
               }
        re = self.s.run("schema(db)")
        self.assertEqual(re['databaseDir'], dct['databaseDir'])
        self.assertEqual(re['partitionType'], dct['partitionType'])
        assert_array_equal(np.sort(re['partitionSchema']), dct['partitionSchema'])
        df = pd.DataFrame({'date': np.array(['2012-01-01', '2012-02-01', '2012-05-01', '2012-06-01'], dtype="datetime64"), 'val':[1,2,3,4]})
        t = self.s.table(data=df)
        db.createPartitionedTable(table=t, tableName='pt', partitionColumns='date').append(t)
        scm = self.s.run("schema(loadTable('{dbPath}', 'pt')).colDefs".format(dbPath=DBInfo.dfsDBName))
        assert_array_equal(scm['name'], ['date', 'val'])
        assert_array_equal(scm['typeString'], ['NANOTIMESTAMP', 'LONG'])
        re = self.s.loadTable(tableName='pt', dbPath=DBInfo.dfsDBName).toDF()
        assert_array_equal(re['date'], df['date'])
        assert_array_equal(re['val'], df['val'])
        



    def test_create_dfs_database_value_partition_np_datehour(self):
        if existsDB(DBInfo.dfsDBName):
            dropDB(DBInfo.dfsDBName)
        times=np.array(['2012-01-01T00:00', '2012-01-01T01:00', '2012-01-01T02:00'], dtype="datetime64")
        self.s.database('db', partitionType=keys.VALUE, partitions=times,
                       dbPath=DBInfo.dfsDBName)
        self.assertEqual(existsDB(DBInfo.dfsDBName), True)

        dct = {'databaseDir': DBInfo.dfsDBName,
                'partitionType': 1,
               'partitionSchema': times,
               'partitionSites': None
               }
        # re = self.s.run("schema(db)")
        # print(re)
        # self.assertEqual(re['databaseDir'], dct['databaseDir'])
        # self.assertEqual(re['partitionType'], dct['partitionType'])
        # assert_array_equal(np.sort(re['partitionSchema']), dct['partitionSchema'])

        # script = '''
        # dbName="dfs://testDatabase"
        # db=database(dbName)
        # t=table([2012.01.01T00:00:00, 2012.01.01T01:00:00, 2012.01.01T02:00:00] as time)
        # pt=db.createPartitionedTable(t, `pt, `time).append!(t)
        # exec  count(*) from pt
        # '''
        # num = self.s.run(script)
        # self.assertEqual(num, 3)
    
    def test_create_dfs_database_value_partition_np_arange_date(self):
        if existsDB(DBInfo.dfsDBName):
            dropDB(DBInfo.dfsDBName)
        dates=np.arange('2012-01-01', '2012-01-10', dtype='datetime64[D]')
        db = self.s.database('db', partitionType=keys.VALUE, partitions=dates,
                        dbPath=DBInfo.dfsDBName)
        
        self.assertEqual(existsDB(DBInfo.dfsDBName), True)

        dct = {'databaseDir': DBInfo.dfsDBName,
                'partitionType': 1,
               'partitionSchema': dates,
               'partitionSites': None
               }
        re = self.s.run("schema(db)")
        self.assertEqual(re['databaseDir'], dct['databaseDir'])
        self.assertEqual(re['partitionType'], dct['partitionType'])
        assert_array_equal(np.sort(re['partitionSchema']), dct['partitionSchema'])
        df = pd.DataFrame({'datetime':np.array(['2012-01-01T00:00:00', '2012-01-02T00:00:00'], dtype='datetime64'), 'sym':['AA', 'BB'], 'val':[1,2]})
        t = self.s.table(data=df)
        db.createPartitionedTable(table=t, tableName='pt', partitionColumns='datetime').append(t)
        re = self.s.run("schema(loadTable('{dbPath}', 'pt')).colDefs".format(dbPath=DBInfo.dfsDBName))
        assert_array_equal(re['name'], ['datetime', 'sym', 'val'])
        assert_array_equal(re['typeString'], ['NANOTIMESTAMP', 'STRING', 'LONG'])
        re = self.s.loadTable(tableName='pt', dbPath=DBInfo.dfsDBName).toDF()
        assert_array_equal(re['datetime'], df['datetime'])
        assert_array_equal(re['sym'], df['sym'])
        assert_array_equal(re['val'], df['val'])
        db.createTable(table=t, tableName='dt').append(t)
        re = self.s.loadTable(tableName='dt', dbPath=DBInfo.dfsDBName).toDF()
        assert_array_equal(re['datetime'], df['datetime'])
        assert_array_equal(re['sym'], df['sym'])
        assert_array_equal(re['val'], df['val'])


    def test_create_dfs_database_value_partition_np_arange_month(self):
        if existsDB(DBInfo.dfsDBName):
            dropDB(DBInfo.dfsDBName)
        months=np.arange('2012-01', '2012-10', dtype='datetime64[M]')
        db = self.s.database('db', partitionType=keys.VALUE, partitions=months,
                        dbPath=DBInfo.dfsDBName)
        self.assertEqual(existsDB(DBInfo.dfsDBName), True)

        dct = {'databaseDir': DBInfo.dfsDBName,
                'partitionType': 1,
               'partitionSchema': months,
               'partitionSites': None
               }
        re = self.s.run("schema(db)")
        self.assertEqual(re['databaseDir'], dct['databaseDir'])
        self.assertEqual(re['partitionType'], dct['partitionType'])
        assert_array_equal(np.sort(re['partitionSchema']), dct['partitionSchema'])

        dct = {'databaseDir': DBInfo.dfsDBName,
                'partitionType': 1,
               'partitionSchema': months,
               'partitionSites': None
               }
        re = self.s.run("schema(db)")
        self.assertEqual(re['databaseDir'], dct['databaseDir'])
        self.assertEqual(re['partitionType'], dct['partitionType'])
        assert_array_equal(np.sort(re['partitionSchema']), dct['partitionSchema'])
        df = pd.DataFrame({'date': np.array(['2012-01-01', '2012-02-01', '2012-05-01', '2012-06-01'], dtype="datetime64"), 'val':[1,2,3,4]})
        t = self.s.table(data=df)
        db.createPartitionedTable(table=t, tableName='pt', partitionColumns='date').append(t)
        scm = self.s.run("schema(loadTable('{dbPath}', 'pt')).colDefs".format(dbPath=DBInfo.dfsDBName))
        assert_array_equal(scm['name'], ['date', 'val'])
        assert_array_equal(scm['typeString'], ['NANOTIMESTAMP', 'LONG'])
        re = self.s.loadTable(tableName='pt', dbPath=DBInfo.dfsDBName).toDF()
        assert_array_equal(re['date'], df['date'])
        assert_array_equal(re['val'], df['val'])


    def test_create_dfs_database_value_partition_np_datetime(self):
        if existsDB(DBInfo.dfsDBName):
            dropDB(DBInfo.dfsDBName)
        times=np.array(['2012-01-01T00:00:00', '2012-01-01T01:00:00', '2012-01-01T02:00:00'], dtype="datetime64")
        self.s.database('db', partitionType=keys.VALUE, partitions=times,
                       dbPath=DBInfo.dfsDBName)
        
        if existsDB(DBInfo.dfsDBName):
            dropDB(DBInfo.dfsDBName)
        times=np.array(['2012-01-01T00:00', '2012-01-01T01:00', '2012-01-01T02:00'], dtype="datetime64")
        self.s.database('db', partitionType=keys.VALUE, partitions=times,
                       dbPath=DBInfo.dfsDBName)
        self.assertEqual(existsDB(DBInfo.dfsDBName), True)

        dct = {'databaseDir': DBInfo.dfsDBName,
                'partitionType': 1,
               'partitionSchema': times,
               'partitionSites': None
               }
        # re = self.s.run("schema(db)")
        # self.assertEqual(re['databaseDir'], dct['databaseDir'])
        # self.assertEqual(re['partitionType'], dct['partitionType'])
        # assert_array_equal(np.sort(re['partitionSchema']), dct['partitionSchema'])

        # script = '''
        # dbName="dfs://testDatabase"
        # db=database(dbName)
        # t=table([2012.01.01T00:00:00, 2012.01.01T01:00:00, 2012.01.01T02:00:00] as time)
        # pt=db.createPartitionedTable(t, `pt, `time).append!(t)
        # exec  count(*) from pt
        # '''
        # num = self.s.run(script)
        # self.assertEqual(num, 3)


    def test_create_dfs_database_compo_partition(self):
        if existsDB(DBInfo.dfsDBName):
            dropDB(DBInfo.dfsDBName)
        db1 = self.s.database('db1', partitionType=keys.VALUE,
                   partitions=np.array(["2012-01-01", "2012-01-06"], dtype="datetime64"), dbPath='')
        db2 = self.s.database('db2', partitionType=keys.RANGE,
                        partitions=[1, 6, 11], dbPath='')
        db = self.s.database('db', keys.COMPO, partitions=[db1, db2], dbPath=DBInfo.dfsDBName)
        self.assertEqual(existsDB(DBInfo.dfsDBName), True)

        dct = {'databaseDir': DBInfo.dfsDBName,
                'partitionType': [1, 2],
               'partitionSchema': [np.array(["2012-01-06", "2012-01-01"], dtype="datetime64"), np.array([1, 6, 11])],
               'partitionSites': None
               }
        re = self.s.run("schema(db)")
        self.assertEqual(re['databaseDir'], dct['databaseDir'])
        assert_array_equal(re['partitionType'], dct['partitionType'])
        assert_array_equal(re['partitionSchema'][0], dct['partitionSchema'][0])
        assert_array_equal(re['partitionSchema'][1], dct['partitionSchema'][1])
        df = pd.DataFrame({'date':np.array(['2012-01-01', '2012-01-01', '2012-01-06', '2012-01-06'], dtype='datetime64'), 'val': [1, 6, 1, 6]})
        t = self.s.table(data=df)
        db.createPartitionedTable(table=t, tableName='pt', partitionColumns=['date', 'val']).append(t)
        re = self.s.loadTable(tableName='pt', dbPath=DBInfo.dfsDBName).toDF()
        assert_array_equal(re['date'], df['date'])
        assert_array_equal(re['val'], df['val'])
        db.createTable(table=t, tableName='dt').append(t)
        re = self.s.loadTable(tableName='dt', dbPath=DBInfo.dfsDBName).toDF()
        assert_array_equal(re['date'], df['date'])
        assert_array_equal(re['val'], df['val'])

    def test_create_dfs_table_with_chineses_column_name(self):
        if existsDB(DBInfo.dfsDBName):
            dropDB(DBInfo.dfsDBName)
        db = self.s.database('db', partitionType=keys.RANGE, partitions=[1, 11, 21], dbPath=DBInfo.dfsDBName)
        self.assertEqual(existsDB(DBInfo.dfsDBName), True)

        dct = {'databaseDir': DBInfo.dfsDBName,
               'partitionSchema': np.array([1, 11, 21], dtype=np.int32),
               'partitionSites': None,
               'partitionTypeName':'RANGE',
               'partitionType': 2}
        re = self.s.run("schema(db)")
        self.assertEqual(re['databaseDir'], dct['databaseDir'])
        assert_array_equal(re['partitionSchema'], dct['partitionSchema'])
        self.assertEqual(re['partitionSites'], dct['partitionSites'])
        df = pd.DataFrame({'编号': np.arange(1, 21), '值': np.repeat(1, 20)})
        t = self.s.table(data=df, tableAliasName='t')
        db.createPartitionedTable(table=t, tableName='pt', partitionColumns='编号').append(t)
        re = self.s.loadTable(tableName='pt', dbPath=DBInfo.dfsDBName).toDF()
        assert_array_equal(re['编号'], np.arange(1, 21))
        assert_array_equal(re['值'], np.repeat(1, 20))
        db.createTable(table=t, tableName='dt').append(t)
        re = self.s.loadTable(tableName='dt', dbPath=DBInfo.dfsDBName).toDF()
        assert_array_equal(re['编号'], np.arange(1, 21))
        assert_array_equal(re['值'], np.repeat(1, 20))

    def test_database_already_exists_with_partition_none(self):
        if existsDB(DBInfo.dfsDBName):
            dropDB(DBInfo.dfsDBName)
        dbPath=DBInfo.dfsDBName
        script='''
        dbPath='{db}'
        db = database(dbPath, VALUE, 1 2 3 4 5)
        t = table(1..5 as id, rand(string('A'..'Z'),5) as val)
        pt = db.createPartitionedTable(t, `pt, `id).append!(t)
        '''.format(db=dbPath)
        self.s.run(script)
        self.assertEqual(existsDB(DBInfo.dfsDBName), True)
        db = self.s.database(dbPath=dbPath)
        df = pd.DataFrame({'id':np.array([1,2,3]), 'sym':['A', 'B','C']})
        t = self.s.table(data=df)
        db.createPartitionedTable(table=t,tableName='pt1',partitionColumns='id').append(t)
        re = self.s.loadTable(tableName='pt1',dbPath=DBInfo.dfsDBName).toDF()
        assert_array_equal(re['sym'],np.array(['A','B','C']))

    
    def test_database_dfs_table_value_datehour_as_partitionSchema(self):
        if existsDB(DBInfo.dfsDBName):
            dropDB(DBInfo.dfsDBName)
        datehour = np.array(["2021-01-01T01","2021-01-01T02","2021-01-01T03","2021-01-01T04"],dtype="datetime64")
        db = self.s.database('db', partitionType=keys.VALUE, partitions=datehour, dbPath=DBInfo.dfsDBName)
        self.assertEqual(existsDB(DBInfo.dfsDBName), True)

        dct = {'databaseDir': DBInfo.dfsDBName,
        'partitionType': 1,
        'partitionSchema': datehour,
        'partitionSites': None
        }
        re = self.s.run("schema(db)")
        self.assertEqual(re['databaseDir'], dct['databaseDir'])
        self.assertEqual(re['partitionType'], dct['partitionType'])
        assert_array_equal(np.sort(re['partitionSchema']), dct['partitionSchema'])
        df = pd.DataFrame({'datehour': np.array(["2021-01-01T01","2021-01-01T02","2021-01-01T03","2021-01-01T04"],dtype="datetime64"), 'val':[1,2,3,4]})
        t = self.s.table(data=df)
        db.createPartitionedTable(table=t, tableName='pt', partitionColumns='datehour').append(t)
        scm = self.s.run("schema(loadTable('{dbPath}', 'pt')).colDefs".format(dbPath=DBInfo.dfsDBName))
        assert_array_equal(scm['name'],['datehour','val'])
        assert_array_equal(scm['typeString'],['NANOTIMESTAMP','LONG'])
        re = self.s.loadTable(tableName='pt', dbPath=DBInfo.dfsDBName).toDF()
        assert_array_equal(re['datehour'], df['datehour'])
        assert_array_equal(re['val'], df['val'])

    def test_database_dfs_table_range_datehour_as_partitionSchema(self):
        if existsDB(DBInfo.dfsDBName):
            dropDB(DBInfo.dfsDBName)
        datehour = np.array(["2021-01-01T01","2021-01-01T04","2021-01-01T08","2021-01-01T10","2021-01-01T13"],dtype="datetime64")
        db = self.s.database('db', partitionType=keys.RANGE, partitions=datehour, dbPath=DBInfo.dfsDBName)
        self.assertEqual(existsDB(DBInfo.dfsDBName), True)

        dct = {'databaseDir': DBInfo.dfsDBName,
        'partitionType': 2,
        'partitionSchema': datehour,
        'partitionSites': None
        }
        re = self.s.run("schema(db)")
        self.assertEqual(re['databaseDir'], dct['databaseDir'])
        self.assertEqual(re['partitionType'], dct['partitionType'])
        assert_array_equal(np.sort(re['partitionSchema']), dct['partitionSchema'])
        df = pd.DataFrame({'datehour': np.array(["2021-01-01T01:01:01","2021-01-01T02:01:01","2021-01-01T07:01:01","2021-01-01T08:01:01","2021-01-01T10:01:01","2021-01-01T12:01:01"],dtype="datetime64"), 'val':[1,2,3,4,5,6]})
        t = self.s.table(data=df)
        db.createPartitionedTable(table=t, tableName='pt', partitionColumns='datehour').append(t)
        scm = self.s.run("schema(loadTable('{dbPath}', 'pt')).colDefs".format(dbPath=DBInfo.dfsDBName))
        assert_array_equal(scm['name'],['datehour','val'])
        assert_array_equal(scm['typeString'],['NANOTIMESTAMP','LONG'])
        re = self.s.loadTable(tableName='pt', dbPath=DBInfo.dfsDBName).toDF()
        assert_array_equal(re['datehour'], df['datehour'])
        assert_array_equal(re['val'], df['val'])

    def test_database_paramete(self):
        if existsDB(DBInfo.dfsDBName):
            dropDB(DBInfo.dfsDBName)
        datehour = np.array(["2021-01-01T01","2021-01-01T04","2021-01-01T08","2021-01-01T10","2021-01-01T13"],dtype="datetime64")
        with self.assertRaises(TypeError):
            db = self.s.database(dbName_ERROR='db', partitionType=keys.RANGE, partitions=datehour, dbPath=DBInfo.dfsDBName)
        with self.assertRaises(TypeError):
            db = self.s.database(dbName='db', partitionType_ERROR=keys.RANGE, partitions=datehour, dbPath=DBInfo.dfsDBName)
        with self.assertRaises(TypeError):
            db = self.s.database(dbName='db', partitionType=keys.RANGE, partitions_ERROR=datehour, dbPath=DBInfo.dfsDBName)
        with self.assertRaises(TypeError):
            db = self.s.database(dbName='db', partitionType=keys.RANGE, partitions=datehour, dbPath_ERROR=DBInfo.dfsDBName)
        db = self.s.database(dbName='db', partitionType=keys.RANGE, partitions=datehour, dbPath=DBInfo.dfsDBName)

    def test_createTable_and_createPartitionedTable_paramete(self):
        if existsDB(DBInfo.dfsDBName):
            dropDB(DBInfo.dfsDBName)
        db = self.s.database('db', partitionType=keys.RANGE, partitions=[1, 11, 21], dbPath=DBInfo.dfsDBName)
        self.assertEqual(existsDB(DBInfo.dfsDBName), True)

        dct = {'databaseDir': DBInfo.dfsDBName,
                'partitionSchema': np.array([1, 11, 21], dtype=np.int32),
                'partitionSites': None,
                'partitionTypeName':'RANGE',
                'partitionType': 2}
        re = self.s.run("schema(db)")
        self.assertEqual(re['databaseDir'], dct['databaseDir'])
        df = pd.DataFrame({'编号': np.arange(1, 21), '值': np.repeat(1, 20)})
        t = self.s.table(data=df, tableAliasName='t')
        
        with self.assertRaises(TypeError):
            db.createPartitionedTable(table_ERROR=t, tableName='pt', partitionColumns='编号')
        with self.assertRaises(TypeError):
            db.createPartitionedTable(table=t, tableName_ERROR='pt', partitionColumns='编号')
        with self.assertRaises(TypeError):
            db.createPartitionedTable(table=t, tableName='pt', partitionColumns_ERROR='编号')
        db.createPartitionedTable(table=t, tableName='pt', partitionColumns='编号')
        with self.assertRaises(TypeError):
            db.createTable(table_ERROR=t, tableName='dt')
        with self.assertRaises(TypeError):
            db.createTable(table=t, tableName_ERROR='dt')
        db.createTable(table=t, tableName='dt')
        
    def test_create_database_engine_olap(self):
        if existsDB(DBInfo.dfsDBName):
            dropDB(DBInfo.dfsDBName)
        db = self.s.database('db', partitionType=keys.RANGE, partitions=[1, 11, 21], dbPath=DBInfo.dfsDBName, engine="OLAP")
        self.assertEqual(existsDB(DBInfo.dfsDBName), True)

        dct = {'databaseDir': DBInfo.dfsDBName,
               'partitionSchema': np.array([1, 11, 21]),
               'partitionSites': None,
               'partitionTypeName': 'RANGE',
               'partitionType': 2,
               'engineType':'OLAP'}
        re = self.s.run("schema(db)")
        self.assertEqual(re['engineType'], dct['engineType'])
        self.assertEqual(re['databaseDir'], dct['databaseDir'])
        assert_array_equal(re['partitionSchema'], dct['partitionSchema'])
        self.assertEqual(re['partitionSites'], dct['partitionSites'])
        df = pd.DataFrame({'id': np.arange(1, 21), 'val': np.repeat(1, 20)})
        t = self.s.table(data=df, tableAliasName='t')
        db.createPartitionedTable(table=t, tableName='pt', partitionColumns='id').append(t)
        re = self.s.loadTable(tableName='pt', dbPath=DBInfo.dfsDBName).toDF()
        assert_array_equal(re['id'], np.arange(1, 21))
        assert_array_equal(re['val'], np.repeat(1, 20))
        db.createTable(table=t, tableName='dt').append(t)
        re = self.s.loadTable(tableName='dt', dbPath=DBInfo.dfsDBName).toDF()
        assert_array_equal(re['id'], np.arange(1, 21))
        assert_array_equal(re['val'], np.repeat(1, 20))

    def test_create_database_engine_tsdb(self):
        if existsDB(DBInfo.dfsDBName):
            dropDB(DBInfo.dfsDBName)
        db = self.s.database('db', partitionType=keys.RANGE, partitions=[1, 11, 21], dbPath=DBInfo.dfsDBName, engine="TSDB")
        self.assertEqual(existsDB(DBInfo.dfsDBName), True)

        dct = {'databaseDir': DBInfo.dfsDBName,
               'partitionSchema': np.array([1, 11, 21]),
               'partitionSites': None,
               'partitionTypeName': 'RANGE',
               'partitionType': 2,
               'engineType':'TSDB'}
        re = self.s.run("schema(db)")
        self.assertEqual(re['engineType'], dct['engineType'])
        self.assertEqual(re['databaseDir'], dct['databaseDir'])
        assert_array_equal(re['partitionSchema'], dct['partitionSchema'])
        self.assertEqual(re['partitionSites'], dct['partitionSites'])
        df = pd.DataFrame({'id': np.arange(1, 21), 'val': np.repeat(1, 20)})
        t = self.s.table(data=df, tableAliasName='t')
        db.createPartitionedTable(table=t, tableName='pt', partitionColumns='id', sortColumns="val").append(t)
        re = self.s.loadTable(tableName='pt', dbPath=DBInfo.dfsDBName).toDF()
        assert_array_equal(re['id'], np.arange(1, 21))
        assert_array_equal(re['val'], np.repeat(1, 20))
        df = pd.DataFrame({'id': np.arange(20, 0, -1), 'val': np.repeat(1, 20)})
        t = self.s.table(data=df, tableAliasName='t')
        db.createTable(table=t, tableName='dt', sortColumns="id").append(t)
        re = self.s.loadTable(tableName='dt', dbPath=DBInfo.dfsDBName).toDF()
        assert_array_equal(re['id'], np.arange(1, 21))
        assert_array_equal(re['val'], np.repeat(1, 20))

    def test_create_database_atomic_TRANS(self):
        if existsDB(DBInfo.dfsDBName):
            dropDB(DBInfo.dfsDBName)
        db = self.s.database('db', partitionType=keys.RANGE, partitions=[1, 11, 21], dbPath=DBInfo.dfsDBName, atomic="TRANS")
        self.assertEqual(existsDB(DBInfo.dfsDBName), True)

        dct = {'databaseDir': DBInfo.dfsDBName,
               'partitionSchema': np.array([1, 11, 21]),
               'partitionSites': None,
               'partitionTypeName': 'RANGE',
               'partitionType': 2,
               'atomic':'TRANS'}
        re = self.s.run("schema(db)")
        self.assertEqual(re['atomic'], dct['atomic'])
        self.assertEqual(re['databaseDir'], dct['databaseDir'])
        assert_array_equal(re['partitionSchema'], dct['partitionSchema'])
        self.assertEqual(re['partitionSites'], dct['partitionSites'])
        df = pd.DataFrame({'id': np.arange(1, 21), 'val': np.repeat(1, 20)})
        t = self.s.table(data=df, tableAliasName='t')
        db.createPartitionedTable(table=t, tableName='pt', partitionColumns='id').append(t)
        re = self.s.loadTable(tableName='pt', dbPath=DBInfo.dfsDBName).toDF()
        assert_array_equal(re['id'], np.arange(1, 21))
        assert_array_equal(re['val'], np.repeat(1, 20))
        db.createTable(table=t, tableName='dt').append(t)
        re = self.s.loadTable(tableName='dt', dbPath=DBInfo.dfsDBName).toDF()
        assert_array_equal(re['id'], np.arange(1, 21))
        assert_array_equal(re['val'], np.repeat(1, 20))

    def test_create_database_atomic_CHUNK(self):
        if existsDB(DBInfo.dfsDBName):
            dropDB(DBInfo.dfsDBName)
        db = self.s.database('db', partitionType=keys.RANGE, partitions=[1, 11, 21], dbPath=DBInfo.dfsDBName, atomic="CHUNK")
        self.assertEqual(existsDB(DBInfo.dfsDBName), True)

        dct = {'databaseDir': DBInfo.dfsDBName,
               'partitionSchema': np.array([1, 11, 21]),
               'partitionSites': None,
               'partitionTypeName': 'RANGE',
               'partitionType': 2,
               'atomic':'CHUNK'}
        re = self.s.run("schema(db)")
        self.assertEqual(re['atomic'], dct['atomic'])
        self.assertEqual(re['databaseDir'], dct['databaseDir'])
        assert_array_equal(re['partitionSchema'], dct['partitionSchema'])
        self.assertEqual(re['partitionSites'], dct['partitionSites'])
        df = pd.DataFrame({'id': np.arange(1, 21), 'val': np.repeat(1, 20)})
        t = self.s.table(data=df, tableAliasName='t')
        db.createPartitionedTable(table=t, tableName='pt', partitionColumns='id').append(t)
        re = self.s.loadTable(tableName='pt', dbPath=DBInfo.dfsDBName).toDF()
        assert_array_equal(re['id'], np.arange(1, 21))
        assert_array_equal(re['val'], np.repeat(1, 20))
        db.createTable(table=t, tableName='dt').append(t)
        re = self.s.loadTable(tableName='dt', dbPath=DBInfo.dfsDBName).toDF()
        assert_array_equal(re['id'], np.arange(1, 21))
        assert_array_equal(re['val'], np.repeat(1, 20))

    #enableChunkGranularityConfig=true
    def test_create_database_chunkGranularity_TABLE(self):
        if existsDB(DBInfo.dfsDBName):
            dropDB(DBInfo.dfsDBName)
        db = self.s.database('db', partitionType=keys.RANGE, partitions=[1, 11, 21], dbPath=DBInfo.dfsDBName, chunkGranularity="TABLE")
        self.assertEqual(existsDB(DBInfo.dfsDBName), True)

        dct = {'databaseDir': DBInfo.dfsDBName,
               'partitionSchema': np.array([1, 11, 21]),
               'partitionSites': None,
               'partitionTypeName': 'RANGE',
               'partitionType': 2,
               'chunkGranularity':'TABLE'}
        re = self.s.run("schema(db)")
        self.assertEqual(re['chunkGranularity'], dct['chunkGranularity'])
        self.assertEqual(re['databaseDir'], dct['databaseDir'])
        assert_array_equal(re['partitionSchema'], dct['partitionSchema'])
        self.assertEqual(re['partitionSites'], dct['partitionSites'])
        df = pd.DataFrame({'id': np.arange(1, 21), 'val': np.repeat(1, 20)})
        t = self.s.table(data=df, tableAliasName='t')
        db.createPartitionedTable(table=t, tableName='pt', partitionColumns='id').append(t)
        re = self.s.loadTable(tableName='pt', dbPath=DBInfo.dfsDBName).toDF()
        assert_array_equal(re['id'], np.arange(1, 21))
        assert_array_equal(re['val'], np.repeat(1, 20))
        db.createTable(table=t, tableName='dt').append(t)
        re = self.s.loadTable(tableName='dt', dbPath=DBInfo.dfsDBName).toDF()
        assert_array_equal(re['id'], np.arange(1, 21))
        assert_array_equal(re['val'], np.repeat(1, 20))

    #enableChunkGranularityConfig=true
    def test_create_database_chunkGranularity_DATABASE(self):
        s_chunkGranularity = ddb.session(enableChunkGranularityConfig=True)
        s_chunkGranularity.connect(HOST, PORT, "admin", "123456")
        if existsDB(DBInfo.dfsDBName):
            dropDB(DBInfo.dfsDBName)
        db = s_chunkGranularity.database('db', partitionType=keys.RANGE, partitions=[1, 11, 21], dbPath=DBInfo.dfsDBName, chunkGranularity="DATABASE")
        self.assertEqual(existsDB(DBInfo.dfsDBName), True)

        dct = {'databaseDir': DBInfo.dfsDBName,
               'partitionSchema': np.array([1, 11, 21]),
               'partitionSites': None,
               'partitionTypeName': 'RANGE',
               'partitionType': 2,
               'chunkGranularity':'DATABASE'}
        re = s_chunkGranularity.run("schema(db)")
        print("+++++++++++++++++++++++++++++")
        print(re['chunkGranularity'])
        print(dct['chunkGranularity'])
        
        self.assertEqual(re['chunkGranularity'], dct['chunkGranularity'])
        self.assertEqual(re['databaseDir'], dct['databaseDir'])
        assert_array_equal(re['partitionSchema'], dct['partitionSchema'])
        self.assertEqual(re['partitionSites'], dct['partitionSites'])
        df = pd.DataFrame({'id': np.arange(1, 21), 'val': np.repeat(1, 20)})
        t = s_chunkGranularity.table(data=df, tableAliasName='t')
        db.createPartitionedTable(table=t, tableName='pt', partitionColumns='id').append(t)
        re = s_chunkGranularity.loadTable(tableName='pt', dbPath=DBInfo.dfsDBName).toDF()
        assert_array_equal(re['id'], np.arange(1, 21))
        assert_array_equal(re['val'], np.repeat(1, 20))
        db.createTable(table=t, tableName='dt').append(t)
        re = s_chunkGranularity.loadTable(tableName='dt', dbPath=DBInfo.dfsDBName).toDF()
        assert_array_equal(re['id'], np.arange(1, 21))
        assert_array_equal(re['val'], np.repeat(1, 20))
        s_chunkGranularity.close()

    #enableChunkGranularityConfig=true
    def test_create_database_engine_atomic_chunkGranularity(self):
        s_chunkGranularity = ddb.session(enableChunkGranularityConfig=True)
        s_chunkGranularity.connect(HOST, PORT, "admin", "123456")
        if existsDB(DBInfo.dfsDBName):
            dropDB(DBInfo.dfsDBName)
        db = s_chunkGranularity.database('db', partitionType=keys.RANGE, partitions=[1, 11, 21], dbPath=DBInfo.dfsDBName, engine="TSDB", atomic="CHUNK", chunkGranularity="DATABASE")
        self.assertEqual(existsDB(DBInfo.dfsDBName), True)

        dct = {'databaseDir': DBInfo.dfsDBName,
               'partitionSchema': np.array([1, 11, 21]),
               'partitionSites': None,
               'partitionTypeName': 'RANGE',
               'partitionType': 2,
               'engineType':'TSDB',
               'atomic':'CHUNK',
               'chunkGranularity':'DATABASE'}
        re = s_chunkGranularity.run("schema(db)")
        self.assertEqual(re['engineType'], dct['engineType'])
        self.assertEqual(re['atomic'], dct['atomic'])
        self.assertEqual(re['chunkGranularity'], dct['chunkGranularity'])
        self.assertEqual(re['databaseDir'], dct['databaseDir'])
        assert_array_equal(re['partitionSchema'], dct['partitionSchema'])
        self.assertEqual(re['partitionSites'], dct['partitionSites'])
        df = pd.DataFrame({'id': np.arange(1, 21), 'val': np.repeat(1, 20)})
        t = s_chunkGranularity.table(data=df, tableAliasName='t')
        db.createPartitionedTable(table=t, tableName='pt', partitionColumns='id', sortColumns="val").append(t)
        
        re = s_chunkGranularity.loadTable(tableName='pt', dbPath=DBInfo.dfsDBName).toDF()
        assert_array_equal(re['id'], np.arange(1, 21))
        assert_array_equal(re['val'], np.repeat(1, 20))

    def test_createPartitionedTable_compress_delta(self):
        if existsDB(DBInfo.dfsDBName):
            dropDB(DBInfo.dfsDBName)
        db = self.s.database('db', partitionType=keys.RANGE, partitions=[1, 11, 21], dbPath=DBInfo.dfsDBName, engine="TSDB")
        self.assertEqual(existsDB(DBInfo.dfsDBName), True)

        dct = {'databaseDir': DBInfo.dfsDBName,
               'partitionSchema': np.array([1, 11, 21]),
               'partitionSites': None,
               'partitionTypeName': 'RANGE',
               'partitionType': 2,
               'engineType':'TSDB'}
        re = self.s.run("schema(db)")
        self.assertEqual(re['engineType'], dct['engineType'])
        self.assertEqual(re['databaseDir'], dct['databaseDir'])
        assert_array_equal(re['partitionSchema'], dct['partitionSchema'])
        self.assertEqual(re['partitionSites'], dct['partitionSites'])
        df = pd.DataFrame({'id': np.arange(1, 21), 'val': np.arange(1, 21, dtype=np.short)})
        t = self.s.table(data=df, tableAliasName='t')
        db.createPartitionedTable(table=t, tableName='pt', partitionColumns='id', compressMethods={"id":"lz4", "val":"delta"}, sortColumns="val").append(t)
        re = self.s.loadTable(tableName='pt', dbPath=DBInfo.dfsDBName).toDF()
        assert_array_equal(re['id'], np.arange(1, 21))
        assert_array_equal(re['val'], np.arange(1, 21, dtype=np.short))
    
    def test_createPartitionedTable_KeepDuplicates_ALL(self):
        if existsDB(DBInfo.dfsDBName):
            dropDB(DBInfo.dfsDBName)
        db = self.s.database('db', partitionType=keys.RANGE, partitions=[1, 11, 21], dbPath=DBInfo.dfsDBName, engine="TSDB")
        self.assertEqual(existsDB(DBInfo.dfsDBName), True)

        dct = {'databaseDir': DBInfo.dfsDBName,
                'partitionSchema': np.array([1, 11, 21]),
                'partitionSites': None,
                'partitionTypeName': 'RANGE',
                'partitionType': 2,
                'engineType':'TSDB'}
        re = self.s.run("schema(db)")
        self.assertEqual(re['engineType'], dct['engineType'])
        self.assertEqual(re['databaseDir'], dct['databaseDir'])
        assert_array_equal(re['partitionSchema'], dct['partitionSchema'])
        self.assertEqual(re['partitionSites'], dct['partitionSites'])
        df = pd.DataFrame({'id': np.arange(1, 21), 'val': np.array([1]*20, dtype=np.short)})
        t = self.s.table(data=df, tableAliasName='t')
        db.createPartitionedTable(table=t, tableName='pt', partitionColumns='id', sortColumns="val", keepDuplicates="ALL").append(t)
        re = self.s.loadTable(tableName='pt', dbPath=DBInfo.dfsDBName).toDF()
        assert_array_equal(re['id'], np.arange(1, 21))
        assert_array_equal(re['val'], np.array([1]*20, dtype=np.short))

    def test_createPartitionedTable_KeepDuplicates_LAST(self):
        if existsDB(DBInfo.dfsDBName):
            dropDB(DBInfo.dfsDBName)
        db = self.s.database('db', partitionType=keys.RANGE, partitions=[1, 11, 21], dbPath=DBInfo.dfsDBName, engine="TSDB")
        self.assertEqual(existsDB(DBInfo.dfsDBName), True)

        dct = {'databaseDir': DBInfo.dfsDBName,
                'partitionSchema': np.array([1, 11, 21]),
                'partitionSites': None,
                'partitionTypeName': 'RANGE',
                'partitionType': 2,
                'engineType':'TSDB'}
        re = self.s.run("schema(db)")
        self.assertEqual(re['engineType'], dct['engineType'])
        self.assertEqual(re['databaseDir'], dct['databaseDir'])
        assert_array_equal(re['partitionSchema'], dct['partitionSchema'])
        self.assertEqual(re['partitionSites'], dct['partitionSites'])
        df = pd.DataFrame({'id': np.array(list(range(20))), 
                           'x': np.array([1]*20),
                           'y': np.array([2]*20)})
        t = self.s.table(data=df, tableAliasName='t')
        db.createPartitionedTable(table=t, tableName='pt', partitionColumns='id', sortColumns=["x", "y"], keepDuplicates="LAST").append(t)
        re = self.s.loadTable(tableName='pt', dbPath=DBInfo.dfsDBName).toDF()
        ex = pd.DataFrame({'id': np.array([10, 19]), 
                           'x': np.array([1]*2),
                           'y': np.array([2]*2)})
        assert_frame_equal(re, ex)

    def test_createPartitionedTable_KeepDuplicates_FIRST(self):
        if existsDB(DBInfo.dfsDBName):
            dropDB(DBInfo.dfsDBName)
        db = self.s.database('db', partitionType=keys.RANGE, partitions=[1, 11, 21], dbPath=DBInfo.dfsDBName, engine="TSDB")
        self.assertEqual(existsDB(DBInfo.dfsDBName), True)

        dct = {'databaseDir': DBInfo.dfsDBName,
                'partitionSchema': np.array([1, 11, 21]),
                'partitionSites': None,
                'partitionTypeName': 'RANGE',
                'partitionType': 2,
                'engineType':'TSDB'}
        re = self.s.run("schema(db)")
        self.assertEqual(re['engineType'], dct['engineType'])
        self.assertEqual(re['databaseDir'], dct['databaseDir'])
        assert_array_equal(re['partitionSchema'], dct['partitionSchema'])
        self.assertEqual(re['partitionSites'], dct['partitionSites'])
        df = pd.DataFrame({'id': np.array(list(range(20))), 
                           'x': np.array([1]*20),
                           'y': np.array([2]*20)})
        t = self.s.table(data=df, tableAliasName='t')
        db.createPartitionedTable(table=t, tableName='pt', partitionColumns='id', sortColumns=["x", "y"], keepDuplicates="FIRST").append(t)
        re = self.s.loadTable(tableName='pt', dbPath=DBInfo.dfsDBName).toDF()
        ex = pd.DataFrame({'id': np.array([1, 11]), 
                           'x': np.array([1]*2),
                           'y': np.array([2]*2)})
        assert_frame_equal(re, ex)

    def test_DBConnectionPool_compress(self):
        self.s.run('''
            dbPath = "dfs://PTA_test"
            if(existsDatabase(dbPath))
                dropDatabase(dbPath)
            t = table(100:100,`sym`id`qty`price,[SYMBOL,INT,INT,DOUBLE])
            db=database(dbPath,RANGE,[1,10001,20001,30001,40001,50001,60001])
            pt = db.createPartitionedTable(t, `pt, `id)
        ''')
        appender = ddb.PartitionedTableAppender("dfs://PTA_test", "pt", "id", self.pool)
        sym = list(map(str, np.arange(10001, 60001)))
        id = np.random.randint(1, 60001, 50000)
        qty = np.random.randint(0, 101, 50000)
        price = np.random.randint(0, 60001, 50000)*0.1
        data = pd.DataFrame({'sym': sym, 'id': id, 'qty': qty, 'price': price})
        num = appender.append(data)
        self.assertEqual(num, 50000)
        re = self.s.run("select * from loadTable('dfs://PTA_test', 'pt') order by id, sym, qty, price")
        expected = data.sort_values(by=['id', 'sym', 'qty', 'price'], ascending=[True, True, True, True])
        expected.set_index(np.arange(0, 50000), inplace=True)
        assert_frame_equal(re, expected, check_dtype=False)
    
    def test_session_compress(self):
        if existsDB(DBInfo.dfsDBName):
            dropDB(DBInfo.dfsDBName)
        db = self.s_compress.database('db', partitionType=keys.VALUE, partitions=[1, 2, 3], dbPath=DBInfo.dfsDBName)
        self.assertEqual(existsDB(DBInfo.dfsDBName), True)

        dct = {'databaseDir': DBInfo.dfsDBName,
               'partitionSchema': np.array([3, 1, 2]),
               'partitionSites': None,
               'partitionTypeName': 'VALUE',
               'partitionType': 1}
        re = self.s_compress.run("schema(db)")
        self.assertEqual(re['databaseDir'], dct['databaseDir'])
        assert_array_equal(re['partitionSchema'], dct['partitionSchema'])
        self.assertEqual(re['partitionSites'], dct['partitionSites'])
        df = pd.DataFrame({'id':[1, 2, 3, 1, 2, 3], 'val':[11, 12, 13, 14, 15, 16]})
        t = self.s_compress.table(data=df)
        pt = db.createPartitionedTable(table=t, tableName='pt', partitionColumns='id').append(t)
        re = self.s_compress.loadTable(tableName='pt', dbPath=DBInfo.dfsDBName).toDF()
        assert_array_equal(np.sort(df['id']), np.sort(re['id']))
        assert_array_equal(np.sort(df['val']), np.sort(re['val']))
        dt = db.createTable(table=t, tableName='dt').append(t)
        re = self.s_compress.loadTable(tableName='dt', dbPath=DBInfo.dfsDBName).toDF()
        assert_array_equal(np.sort(df['id']), np.sort(re['id']))
        assert_array_equal(np.sort(df['val']), np.sort(re['val']))

#unittest.main()
if __name__ == '__main__':
    unittest.main()
