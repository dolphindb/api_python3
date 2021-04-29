import unittest
import dolphindb as ddb
from pandas.testing import assert_frame_equal
from setup import HOST, PORT, WORK_DIR, DATA_DIR
import asyncio
import numpy as np
import pandas as pd

def create_value_db():
    s=ddb.session()
    s.connect(HOST, PORT, "admin", "123456")
    script='''
    login("admin", "123456")
    dbName="dfs://test_dbConnection"
    tableName="pt"
    if(existsDatabase(dbName)){
	    dropDatabase(dbName)
    }
    db=database(dbName, VALUE, 1..10)
    n=1000000
    t=table(loop(take{, n/10}, 1..10).flatten() as id, 1..1000000 as val)
    pt=db.createPartitionedTable(t, `pt, `id).append!(t)
    '''
    s.run(script)
    s.close()

async def get_row_count(pool):
    return await pool.run("exec count(*) from loadTable('dfs://test_dbConnection', 'pt')")

class TestDBConnectionPool(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.s = ddb.session()
        cls.s.connect(HOST, PORT, "admin", "123456")
        cls.pool = ddb.DBConnectionPool(HOST, PORT, 10, "admin", "123456")

    @classmethod
    def tearDownClass(cls):
        cls.pool.shutDown()

    def test_DBConnectionPool_read_dfs_table(self):
        create_value_db()
        tasks=[
        asyncio.ensure_future(get_row_count(self.pool)),
        asyncio.ensure_future(get_row_count(self.pool)),
        asyncio.ensure_future(get_row_count(self.pool)),
        asyncio.ensure_future(get_row_count(self.pool)),
        asyncio.ensure_future(get_row_count(self.pool)),
        asyncio.ensure_future(get_row_count(self.pool)),
        asyncio.ensure_future(get_row_count(self.pool)),
        asyncio.ensure_future(get_row_count(self.pool)),
        asyncio.ensure_future(get_row_count(self.pool)),
        asyncio.ensure_future(get_row_count(self.pool))]
        loop = asyncio.get_event_loop()
        loop.run_until_complete(asyncio.wait(tasks))
        for task in tasks:
            self.assertEqual(task.result(), 1000000)
        

    def test_PartitionedTableAppender_range_int(self):
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

    def test_PartitionedTableAppender_range_short(self):
        self.s.run('''
        dbPath = "dfs://PTA_test"
        if(existsDatabase(dbPath))
            dropDatabase(dbPath)
        t = table(100:100,`sym`id`qty`price,[SYMBOL,SHORT,INT,DOUBLE])
        db=database(dbPath,RANGE,short([1,10001,20001,30001]))
        pt = db.createPartitionedTable(t, `pt, `id)
        ''')
        appender = ddb.PartitionedTableAppender("dfs://PTA_test", "pt", "id", self.pool)
        sym = list(map(str, np.arange(10001, 60001)))
        id = np.random.randint(1, 30001, 50000)
        qty = np.random.randint(0, 101, 50000)
        price = np.random.randint(0, 60001, 50000)*0.1
        data = pd.DataFrame({'sym': sym, 'id': id, 'qty': qty, 'price': price})
        num = appender.append(data)
        self.assertEqual(num, 50000)
        re = self.s.run("select * from loadTable('dfs://PTA_test', 'pt') order by id, sym, qty, price")
        expected = data.sort_values(by=['id', 'sym', 'qty', 'price'], ascending=[True, True, True, True])
        expected.set_index(np.arange(0, 50000), inplace=True)
        assert_frame_equal(re, expected, check_dtype=False)

    # def test_PartitionedTableAppender_range_date_partitionColumn_date(self):
    #     self.s.run('''
    #     dbPath = "dfs://PTA_test"
    #     if(existsDatabase(dbPath))
    #         dropDatabase(dbPath)
    #     t = table(100:100,`sym`date`id`qty`price,[SYMBOL,DATE,INT,INT,DOUBLE])
    #     db=database(dbPath,RANGE,[2012.01.01, 2012.01.04, 2012.01.08, 2012.01.11])
    #     pt = db.createPartitionedTable(t, `pt, `date)
    #     ''')
    #     appender = ddb.PartitionedTableAppender("dfs://PTA_test", "pt", "date", self.pool)
    #     sym = list(map(str, np.random.randint(0, 60001, 50000)))
    #     x=np.arange('2012-01-01','2012-01-11', dtype='datetime64[D]')
    #     date = np.repeat(x, 5000, axis=0)
    #     id = np.random.randint(0, 60001, 50000)
    #     qty = np.random.randint(0, 101, 50000)
    #     price = np.random.randint(0, 60001, 50000)*0.1
    #     data = pd.DataFrame({'sym': sym, 'date':date, 'id': id, 'qty': qty, 'price': price})
    #     num = appender.append(data)
    #     self.assertEqual(num, 50000)
    #     re = self.s.run("select * from loadTable('dfs://PTA_test', 'pt') order by date, id, sym, qty, price")
    #     expected = data.sort_values(by=['date', 'id', 'sym', 'qty', 'price'], ascending=[True, True, True, True, True])
    #     assert_frame_equal(re, expected, check_dtype=False)


    # def test_PartitionedTableAppender_range_date_partitionColumn_datetime(self):
    #     self.s.run('''
    #     dbPath = "dfs://PTA_test"
    #     if(existsDatabase(dbPath))
    #         dropDatabase(dbPath)
    #     t = table(100:100,`sym`datetime`id`qty`price,[SYMBOL,DATETIME,INT,INT,DOUBLE])
    #     db=database(dbPath,RANGE,[2012.01.01, 2012.01.04, 2012.01.08, 2012.01.11])
    #     pt = db.createPartitionedTable(t, `pt, `datetime)
    #     ''')
    #     appender = ddb.PartitionedTableAppender("dfs://PTA_test", "pt", "datetime", self.pool)
    #     datetime = np.arange('2012-01-01T00:00:00','2012-01-11T00:00:00', dtype='datetime64[s]')
    #     total = len(datetime)
    #     sym = list(map(str, np.random.randint(0, 60001, total)))
    #     id = np.random.randint(0, 60001, total)
    #     qty = np.random.randint(0, 101, total)
    #     price = np.random.randint(0, 60001, total)*0.1
    #     data = pd.DataFrame({'sym': sym, 'datetime':datetime, 'id': id, 'qty': qty, 'price': price})
    #     num = appender.append(data)
    #     self.assertEqual(num, total)
    #     re = self.s.run("select * from loadTable('dfs://PTA_test', 'pt') order by datetime, id, sym, qty, price")
    #     expected = data.sort_values(by=['datetime', 'id', 'sym', 'qty', 'price'], ascending=[True, True, True, True, True])
    #     assert_frame_equal(re, expected, check_dtype=False)
    
    # def test_PartitionedTableAppender_range_date_partitionColumn_timestamp(self):
    #     self.s.run('''
    #     dbPath = "dfs://PTA_test"
    #     if(existsDatabase(dbPath))
    #         dropDatabase(dbPath)
    #     t = table(100:100,`sym`timestamp`id`qty`price,[SYMBOL,TIMESTAMP,INT,INT,DOUBLE])
    #     db=database(dbPath,RANGE,[2012.01.01, 2012.01.04, 2012.01.08, 2012.01.11])
    #     pt = db.createPartitionedTable(t, `pt, `timestamp)
    #     ''')
    #     appender = ddb.PartitionedTableAppender("dfs://PTA_test", "pt", "timestamp", self.pool)
    #     timestamp = np.arange('2012-01-01T00:00:00.000','2012-01-11T00:00:00.000', step=1000, dtype='datetime64[ms]')
    #     total = len(timestamp)
    #     sym = list(map(str, np.random.randint(0, 60001, total)))
    #     id = np.random.randint(0, 60001, total)
    #     qty = np.random.randint(0, 101, total)
    #     price = np.random.randint(0, 60001, total)*0.1
    #     data = pd.DataFrame({'sym': sym, 'timestamp':timestamp, 'id': id, 'qty': qty, 'price': price})
    #     num = appender.append(data)
    #     self.assertEqual(num, total)
    #     re = self.s.run("select * from loadTable('dfs://PTA_test', 'pt') order by timestamp, id, sym, qty, price")
    #     expected = data.sort_values(by=['timestamp', 'id', 'sym', 'qty', 'price'], ascending=[True, True, True, True, True])
    #     assert_frame_equal(re, expected, check_dtype=False)
    
    # def test_PartitionedTableAppender_range_date_partitionColumn_nanotimestamp(self):
    #     self.s.run('''
    #     dbPath = "dfs://PTA_test"
    #     if(existsDatabase(dbPath))
    #         dropDatabase(dbPath)
    #     t = table(100:100,`sym`nanotimestamp`id`qty`price,[SYMBOL,NANOTIMESTAMP,INT,INT,DOUBLE])
    #     db=database(dbPath,RANGE,[2012.01.01, 2012.01.04, 2012.01.08, 2012.01.11])
    #     pt = db.createPartitionedTable(t, `pt, `nanotimestamp)
    #     ''')
    #     appender = ddb.PartitionedTableAppender("dfs://PTA_test", "pt", "nanotimestamp", self.pool)
    #     nanotimestamp = np.arange('2012-01-01T00:00:00.000000000','2012-01-11T00:00:00.000000000', step=1000000000, dtype='datetime64[ns]')
    #     total = len(nanotimestamp)
    #     sym = list(map(str, np.random.randint(0, 60001, total)))
    #     id = np.random.randint(0, 60001, total)
    #     qty = np.random.randint(0, 101, total)
    #     price = np.random.randint(0, 60001, total)*0.1
    #     data = pd.DataFrame({'sym': sym, 'nanotimestamp': nanotimestamp, 'id': id, 'qty': qty, 'price': price})
    #     num = appender.append(data)
    #     self.assertEqual(num, total)
    #     re = self.s.run("select * from loadTable('dfs://PTA_test', 'pt') order by nanotimestamp, id, sym, qty, price")
    #     expected = data.sort_values(by=['nanotimestamp', 'id', 'sym', 'qty', 'price'], ascending=[True, True, True, True, True])
    #     assert_frame_equal(re, expected, check_dtype=False)
    
    def test_PartitionedTableAppender_range_symbol(self):
        self.s.run('''
        dbPath = "dfs://PTA_test"
        if(existsDatabase(dbPath))
            dropDatabase(dbPath)
        t = table(100:100,`sym`id`qty`price,[SYMBOL,INT,INT,DOUBLE])
        sym_range=cutPoints(symbol(string(10001..60000)), 10)
        db=database(dbPath,RANGE,sym_range)
        pt = db.createPartitionedTable(t, `pt, `sym)
        ''')
        appender = ddb.PartitionedTableAppender("dfs://PTA_test", "pt", "sym", self.pool)
        sym = list(map(str, np.arange(10001, 60001)))
        id = np.random.randint(0, 60001, 50000)
        qty = np.random.randint(0, 101, 50000)
        price = np.random.randint(0, 60001, 50000)*0.1
        data = pd.DataFrame({'sym': sym, 'id': id, 'qty': qty, 'price': price})
        num = appender.append(data)
        self.assertEqual(num, 50000)
        re = self.s.run("select * from loadTable('dfs://PTA_test', 'pt') order by id, sym, qty, price")
        expected = data.sort_values(by=['id', 'sym', 'qty', 'price'], ascending=[True, True, True, True])
        expected.set_index(np.arange(0, 50000), inplace=True)
        assert_frame_equal(re, expected, check_dtype=False)

    def test_PartitionedTableAppender_range_string(self):
        self.s.run('''
        dbPath = "dfs://PTA_test"
        if(existsDatabase(dbPath))
            dropDatabase(dbPath)
        t = table(100:100,`sym`id`qty`price,[STRING,INT,INT,DOUBLE])
        sym_range=cutPoints(string(10001..60000), 10)
        db=database(dbPath,RANGE,sym_range)
        pt = db.createPartitionedTable(t, `pt, `sym)
        ''')
        appender = ddb.PartitionedTableAppender("dfs://PTA_test", "pt", "sym", self.pool)
        sym = list(map(str, np.arange(10001, 60001)))
        id = np.random.randint(0, 60001, 50000)
        qty = np.random.randint(0, 101, 50000)
        price = np.random.randint(0, 60001, 50000)*0.1
        data = pd.DataFrame({'sym': sym, 'id': id, 'qty': qty, 'price': price})
        num = appender.append(data)
        self.assertEqual(num, 50000)
        re = self.s.run("select * from loadTable('dfs://PTA_test', 'pt') order by id, sym, qty, price")
        expected = data.sort_values(by=['id', 'sym', 'qty', 'price'], ascending=[True, True, True, True])
        expected.set_index(np.arange(0, 50000), inplace=True)
        assert_frame_equal(re, expected, check_dtype=False)

    def test_PartitionedTableAppender_value_int(self):
        self.s.run('''
        dbPath = "dfs://PTA_test"
        if(existsDatabase(dbPath))
            dropDatabase(dbPath)
        t = table(100:100,`sym`id`qty`price,[SYMBOL,INT,INT,DOUBLE])
        db=database(dbPath,VALUE,1..10)
        pt = db.createPartitionedTable(t, `pt, `id)
        ''')
        appender = ddb.PartitionedTableAppender("dfs://PTA_test", "pt", "id", self.pool)
        sym = list(map(str, np.arange(10001, 60001)))
        id = np.repeat(np.arange(1, 11), 5000, axis=0)
        qty = np.random.randint(0, 101, 50000)
        price = np.random.randint(0, 60001, 50000)*0.1
        data = pd.DataFrame({'sym': sym, 'id': id, 'qty': qty, 'price': price})
        num = appender.append(data)
        self.assertEqual(num, 50000)
        re = self.s.run("select * from loadTable('dfs://PTA_test', 'pt') order by id, sym, qty, price")
        expected = data.sort_values(by=['id', 'sym', 'qty', 'price'], ascending=[True, True, True, True])
        expected.set_index(np.arange(0, 50000), inplace=True)
        assert_frame_equal(re, expected, check_dtype=False)

    def test_PartitionedTableAppender_value_short(self):
        self.s.run('''
        dbPath = "dfs://PTA_test"
        if(existsDatabase(dbPath))
            dropDatabase(dbPath)
        t = table(100:100,`sym`id`qty`price,[SYMBOL,SHORT,INT,DOUBLE])
        db=database(dbPath,VALUE,short(1..10))
        pt = db.createPartitionedTable(t, `pt, `id)
        ''')
        appender = ddb.PartitionedTableAppender("dfs://PTA_test", "pt", "id", self.pool)
        sym = list(map(str, np.arange(10001, 60001)))
        id = np.repeat(np.arange(1, 11), 5000, axis=0)
        qty = np.random.randint(0, 101, 50000)
        price = np.random.randint(0, 60001, 50000)*0.1
        data = pd.DataFrame({'sym': sym, 'id': id, 'qty': qty, 'price': price})
        num = appender.append(data)
        self.assertEqual(num, 50000)
        re = self.s.run("select * from loadTable('dfs://PTA_test', 'pt') order by id, sym, qty, price")
        expected = data.sort_values(by=['id', 'sym', 'qty', 'price'], ascending=[True, True, True, True])
        expected.set_index(np.arange(0, 50000), inplace=True)
        assert_frame_equal(re, expected, check_dtype=False)
      
    def test_PartitionedTableAppender_value_symbol(self):
        self.s.run('''
        dbPath = "dfs://PTA_test"
        if(existsDatabase(dbPath))
            dropDatabase(dbPath)
        t = table(100:100,`sym`id`qty`price,[SYMBOL,INT,INT,DOUBLE])
        db=database(dbPath,VALUE,symbol(['AAPL', 'MSFT', 'IBM', 'GOOG', 'YHOO']))
        pt = db.createPartitionedTable(t, `pt, `sym)
        ''')
        appender = ddb.PartitionedTableAppender("dfs://PTA_test", "pt", "sym", self.pool)
        sym = np.repeat(['AAPL', 'MSFT', 'IBM', 'GOOG', 'YHOO'], 10000, axis=0)
        id = np.random.randint(0, 60001, 50000)
        qty = np.random.randint(0, 101, 50000)
        price = np.random.randint(0, 60001, 50000)*0.1
        data = pd.DataFrame({'sym': sym, 'id': id, 'qty': qty, 'price': price})
        num = appender.append(data)
        self.assertEqual(num, 50000)
        re = self.s.run("select * from loadTable('dfs://PTA_test', 'pt') order by id, sym, qty, price")
        expected = data.sort_values(by=['id', 'sym', 'qty', 'price'], ascending=[True, True, True, True])
        expected.set_index(np.arange(0, 50000), inplace=True)
        assert_frame_equal(re, expected, check_dtype=False)

    def test_PartitionedTableAppender_value_string(self):
        self.s.run('''
        dbPath = "dfs://PTA_test"
        if(existsDatabase(dbPath))
            dropDatabase(dbPath)
        t = table(100:100,`sym`id`qty`price,[STRING,INT,INT,DOUBLE])
        db=database(dbPath,VALUE,['AAPL', 'MSFT', 'IBM', 'GOOG', 'YHOO'])
        pt = db.createPartitionedTable(t, `pt, `sym)
        ''')
        appender = ddb.PartitionedTableAppender("dfs://PTA_test", "pt", "sym", self.pool)
        sym = np.repeat(['AAPL', 'MSFT', 'IBM', 'GOOG', 'YHOO'], 10000, axis=0)
        id = np.random.randint(0, 60001, 50000)
        qty = np.random.randint(0, 101, 50000)
        price = np.random.randint(0, 60001, 50000)*0.1
        data = pd.DataFrame({'sym': sym, 'id': id, 'qty': qty, 'price': price})
        num = appender.append(data)
        self.assertEqual(num, 50000)
        re = self.s.run("select * from loadTable('dfs://PTA_test', 'pt') order by id, sym, qty, price")
        expected = data.sort_values(by=['id', 'sym', 'qty', 'price'], ascending=[True, True, True, True])
        expected.set_index(np.arange(0, 50000), inplace=True)
        assert_frame_equal(re, expected, check_dtype=False)


    # def test_PartitionedTableAppender_value_date(self):
    #     self.s.run('''
    #     dbPath = "dfs://PTA_test"
    #     if(existsDatabase(dbPath))
    #         dropDatabase(dbPath)
    #     t = table(100:100,`date`id`qty`price,[DATE,INT,INT,DOUBLE])
    #     db = database(dbPath,VALUE,2017.02.01..2017.02.05)
    #     pt = db.createPartitionedTable(t,`pt,`date)
    #     ''')
    #     appender = ddb.PartitionedTableAppender("dfs://PTA_test", "pt", "date", self.pool)
    #     x = np.arange('2017-02-01','2017-02-06', dtype='datetime64[D]')
    #     date = np.repeat(x, 10000, axis=0)
    #     id = np.random.randint(0, 60001, 50000)
    #     qty = np.random.randint(0, 101, 50000)
    #     price = np.random.randint(0, 60001, 50000) * 0.1
    #     data = pd.DataFrame({'date': date, 'id': id, 'qty': qty, 'price': price})
    #     num = appender.append(data)
    #     self.assertEqual(num, 100000)
    #     re = self.s.run("select * from loadTable('dfs://PTA_test', 'pt') order by date, sym, qty, price")
    #     expected = data.sort_values(by=['date', 'sym', 'qty', 'price'], ascending=[True, True, True, True])
    #     expected.set_index(np.arange(0, 50000), inplace=True)
    #     assert_frame_equal(re, expected, check_dtype=False)


    # def test_PartitionedTableAppender_value_datetime(self):
    #     self.s.run('''
    #     dbPath = "dfs://PTA_test"
    #     if(existsDatabase(dbPath))
    #         dropDatabase(dbPath)
    #     t = table(100:100,`datetime`id`qty`price,[DATETIME,INT,INT,DOUBLE])
    #     db = database(dbPath,VALUE,2017.02.01 00:00:01..2017.02.01 00:00:05)
    #     pt = db.createPartitionedTable(t,`pt,`datetime)
    #     ''')
    #     appender = ddb.PartitionedTableAppender("dfs://PTA_test", "pt", "datetime", self.pool)
    #     x = np.arange('2017-02-01T00:00:01', '2017-02-01T00:00:06', dtype='datetime64[s]')
    #     datetime = np.repeat(x, 10000, axis=0)
    #     id = np.random.randint(0, 60001, 50000)
    #     qty = np.random.randint(0, 101, 50000)
    #     price = np.random.randint(0, 60001, 50000) * 0.1
    #     data = pd.DataFrame({'datetime': datetime, 'id': id, 'qty': qty, 'price': price})
    #     num = appender.append(data)
    #     self.assertEqual(num, 100000)
    #     re = self.s.run("select * from loadTable('dfs://PTA_test', 'pt') order by datetime, sym, qty, price")
    #     expected = data.sort_values(by=['datetime', 'sym', 'qty', 'price'], ascending=[True, True, True, True])
    #     expected.set_index(np.arange(0, 50000), inplace=True)
    #     assert_frame_equal(re, expected, check_dtype=False)

    # def test_PartitionedTableAppender_value_timestamp(self):
    #     self.s.run('''
    #     dbPath = "dfs://PTA_test"
    #     if(existsDatabase(dbPath))
    #         dropDatabase(dbPath)
    #     t = table(100:100,`timestamp`id`qty`price,[TIMESTAMP,INT,INT,DOUBLE])
    #     db = database(dbPath,VALUE,2017.02.01 00:00:01.001..2017.02.01 00:00:01.005)
    #     pt = db.createPartitionedTable(t,`pt,`timestamp)
    #     ''')
    #     appender = ddb.PartitionedTableAppender("dfs://PTA_test", "pt", "timestamp", self.pool)
    #     x = np.arange('2017-02-01T00:00:01.001', '2017-02-01T00:00:06.006', dtype='datetime64[ms]')
    #     timestamp = np.repeat(x, 10000, axis=0)
    #     id = np.random.randint(0, 60001, 50000)
    #     qty = np.random.randint(0, 101, 50000)
    #     price = np.random.randint(0, 60001, 50000) * 0.1
    #     data = pd.DataFrame({'timestamp': timestamp, 'id': id, 'qty': qty, 'price': price})
    #     num = appender.append(data)
    #     self.assertEqual(num, 100000)
    #     re = self.s.run("select * from loadTable('dfs://PTA_test', 'pt') order by timestamp, sym, qty, price")
    #     expected = data.sort_values(by=['timestamp', 'sym', 'qty', 'price'], ascending=[True, True, True, True])
    #     expected.set_index(np.arange(0, 50000), inplace=True)
    #     assert_frame_equal(re, expected, check_dtype=False)





    def test_PartitionedTableAppender_hash_int(self):
        self.s.run('''
        dbPath = "dfs://PTA_test"
        if(existsDatabase(dbPath))
            dropDatabase(dbPath)
        t = table(100:100,`sym`id`qty`price,[SYMBOL,INT,INT,DOUBLE])
        db=database(dbPath,HASH,[INT, 10])
        pt = db.createPartitionedTable(t, `pt, `id)
        ''')
        appender = ddb.PartitionedTableAppender("dfs://PTA_test", "pt", "id", self.pool)
        sym = list(map(str, np.arange(10001, 60001)))
        id = np.repeat(np.arange(1, 5001), 10, axis=0)
        qty = np.random.randint(0, 101, 50000)
        price = np.random.randint(0, 60001, 50000)*0.1
        data = pd.DataFrame({'sym': sym, 'id': id, 'qty': qty, 'price': price})
        num = appender.append(data)
        self.assertEqual(num, 50000)
        re = self.s.run("select * from loadTable('dfs://PTA_test', 'pt') order by id, sym, qty, price")
        expected = data.sort_values(by=['id', 'sym', 'qty', 'price'], ascending=[True, True, True, True])
        expected.set_index(np.arange(0, 50000), inplace=True)
        assert_frame_equal(re, expected, check_dtype=False)
    
    def test_PartitionedTableAppender_hash_short(self):
        self.s.run('''
        dbPath = "dfs://PTA_test"
        if(existsDatabase(dbPath))
            dropDatabase(dbPath)
        t = table(100:100,`sym`id`qty`price,[SYMBOL,SHORT,INT,DOUBLE])
        db=database(dbPath,HASH,[SHORT, 10])
        pt = db.createPartitionedTable(t, `pt, `id)
        ''')
        appender = ddb.PartitionedTableAppender("dfs://PTA_test", "pt", "id", self.pool)
        sym = list(map(str, np.arange(10001, 60001)))
        id = np.repeat(np.arange(1, 5001), 10, axis=0)
        qty = np.random.randint(0, 101, 50000)
        price = np.random.randint(0, 60001, 50000)*0.1
        data = pd.DataFrame({'sym': sym, 'id': id, 'qty': qty, 'price': price})
        num = appender.append(data)
        self.assertEqual(num, 50000)
        re = self.s.run("select * from loadTable('dfs://PTA_test', 'pt') order by id, sym, qty, price")
        expected = data.sort_values(by=['id', 'sym', 'qty', 'price'], ascending=[True, True, True, True])
        expected.set_index(np.arange(0, 50000), inplace=True)
        assert_frame_equal(re, expected, check_dtype=False)

    def test_PartitionedTableAppender_hash_string(self):
        self.s.run('''
        dbPath = "dfs://PTA_test"
        if(existsDatabase(dbPath))
            dropDatabase(dbPath)
        t = table(100:100,`sym`id`qty`price,[STRING,INT,INT,DOUBLE])
        db=database(dbPath,HASH,[STRING, 10])
        pt = db.createPartitionedTable(t, `pt, `sym)
        ''')
        appender = ddb.PartitionedTableAppender("dfs://PTA_test", "pt", "sym", self.pool)
        sym = list(map(str, np.arange(10001, 60001)))
        id = np.random.randint(0, 60001, 50000)
        qty = np.random.randint(0, 101, 50000)
        price = np.random.randint(0, 60001, 50000)*0.1
        data = pd.DataFrame({'sym': sym, 'id': id, 'qty': qty, 'price': price})
        num = appender.append(data)
        self.assertEqual(num, 50000)
        re = self.s.run("select * from loadTable('dfs://PTA_test', 'pt') order by id, sym, qty, price")
        expected = data.sort_values(by=['id', 'sym', 'qty', 'price'], ascending=[True, True, True, True])
        expected.set_index(np.arange(0, 50000), inplace=True)
        assert_frame_equal(re, expected, check_dtype=False)

    def test_PartitionedTableAppender_hash_symbol(self):
        self.s.run('''
        dbPath = "dfs://PTA_test"
        if(existsDatabase(dbPath))
            dropDatabase(dbPath)
        t = table(100:100,`sym`id`qty`price,[SYMBOL,INT,INT,DOUBLE])
        db=database(dbPath,HASH,[SYMBOL, 10])
        pt = db.createPartitionedTable(t, `pt, `sym)
        ''')
        appender = ddb.PartitionedTableAppender("dfs://PTA_test", "pt", "sym", self.pool)
        sym = list(map(str, np.arange(10001, 60001)))
        id = np.random.randint(0, 60001, 50000)
        qty = np.random.randint(0, 101, 50000)
        price = np.random.randint(0, 60001, 50000)*0.1
        data = pd.DataFrame({'sym': sym, 'id': id, 'qty': qty, 'price': price})
        num = appender.append(data)
        self.assertEqual(num, 50000)
        re = self.s.run("select * from loadTable('dfs://PTA_test', 'pt') order by id, sym, qty, price")
        expected = data.sort_values(by=['id', 'sym', 'qty', 'price'], ascending=[True, True, True, True])
        expected.set_index(np.arange(0, 50000), inplace=True)
        assert_frame_equal(re, expected, check_dtype=False)
    
    # def test_PartitionedTableAppender_hash_ipaddr(self):
    #     self.s.run('''
    #     dbPath = "dfs://PTA_test"
    #     if(existsDatabase(dbPath))
    #         dropDatabase(dbPath)
    #     t = table(100:100,`sym`id`qty`price,[SYMBOL,IPADDR,INT,DOUBLE])
    #     db=database(dbPath,HASH,[IPADDR, 3])
    #     pt = db.createPartitionedTable(t, `pt, `id)
    #     ''')
    #     appender = ddb.PartitionedTableAppender("dfs://PTA_test", "pt", "id", self.pool)
    #     sym = list(map(str, np.arange(10001, 60001)))
    #     id = np.repeat(['192.168.1.103', '192.168.1.107', '127.0.0.1', '1dde:4f8a:97c0:4b8e:ff37:40d8:199a:71ee', 'e1eb:744a:6810:eb55:e9d9:b361:e1ee:d96e'], 10000, axis=0)
    #     qty = np.random.randint(0, 101, 50000)
    #     price = np.random.randint(0, 60001, 50000)*0.1
    #     data = pd.DataFrame({'sym': sym, 'id': id, 'qty': qty, 'price': price})
    #     num = appender.append(data)
    #     self.assertEqual(num, 50000)
    #     re = self.s.run("select * from loadTable('dfs://PTA_test', 'pt') order by id, sym, qty, price")
    #     expected = data.sort_values(by=['id', 'sym', 'qty', 'price'], ascending=[True, True, True, True])
    #     expected.set_index(np.arange(0, 50000), inplace=True)
    #     assert_frame_equal(re, expected, check_dtype=False)
    
    # def test_PartitionedTableAppender_hash_int128(self):
    #     self.s.run('''
    #     dbPath = "dfs://PTA_test"
    #     if(existsDatabase(dbPath))
    #         dropDatabase(dbPath)
    #     t = table(100:100,`sym`id`qty`price,[SYMBOL,INT128,INT,DOUBLE])
    #     db=database(dbPath,HASH,[INT128, 3])
    #     pt = db.createPartitionedTable(t, `pt, `id)
    #     ''')
    #     appender = ddb.PartitionedTableAppender("dfs://PTA_test", "pt", "id", self.pool)
    #     sym = list(map(str, np.arange(10001, 60001)))
    #     id = np.repeat(['61b633c44c4270ef1be7290e4806278f','7a570a7f2119b2f0546078477712ffb7','aca1ebb8964388442c5303b074f47d7e','f536636b495fae3eefa45c77ca414070','44e8918787e1200c6e2b8ea21765979b'], 10000, axis=0)
    #     qty = np.random.randint(0, 101, 50000)
    #     price = np.random.randint(0, 60001, 50000)*0.1
    #     data = pd.DataFrame({'sym': sym, 'id': id, 'qty': qty, 'price': price})
    #     num = appender.append(data)
    #     self.assertEqual(num, 50000)
    #     re = self.s.run("select * from loadTable('dfs://PTA_test', 'pt') order by id, sym, qty, price")
    #     expected = data.sort_values(by=['id', 'sym', 'qty', 'price'], ascending=[True, True, True, True])
    #     expected.set_index(np.arange(0, 50000), inplace=True)
    #     assert_frame_equal(re, expected, check_dtype=False)
    
    # TODO
    # def test_PartitionedTableAppender_hash_uuid(self):
    #     self.s.run('''
    #     dbPath = "dfs://PTA_test"
    #     if(existsDatabase(dbPath))
    #         dropDatabase(dbPath)
    #     t = table(100:100,`sym`id`qty`price,[SYMBOL,UUID,INT,DOUBLE])
    #     db=database(dbPath,HASH,[UUID, 3])
    #     pt = db.createPartitionedTable(t, `pt, `id)
    #     ''')
    #     appender = ddb.PartitionedTableAppender("dfs://PTA_test", "pt", "id", self.pool)
    #     sym = list(map(str, np.arange(10001, 60001)))
    #     id = np.repeat(['b42f709e-9ee9-265b-c1dd-8e773a7c4cad','95ee7f7b-d80e-f60d-7c08-77b9af30a9c2','7bf4db45-c06a-5010-3e4c-e66b059dfbe0','3206ced1-679c-195b-4db6-6292d45ad7d1','c1052eaa-3a55-bb68-16ed-fe934f738c75'], 10000, axis=0)
    #     qty = np.random.randint(0, 101, 50000)
    #     price = np.random.randint(0, 60001, 50000)*0.1
    #     data = pd.DataFrame({'sym': sym, 'id': id, 'qty': qty, 'price': price})
    #     num = appender.append(data)
    #     self.assertEqual(num, 50000)
    #     re = self.s.run("select * from loadTable('dfs://PTA_test', 'pt') order by id, sym, qty, price")
    #     expected = data.sort_values(by=['id', 'sym', 'qty', 'price'], ascending=[True, True, True, True])
    #     expected.set_index(np.arange(0, 50000), inplace=True)
    #     assert_frame_equal(re, expected, check_dtype=False)

    # def test_PartitionedTableAppender_hash_date(self):
    #     self.s.run('''
    #     dbPath = "dfs://PTA_test"
    #     if(existsDatabase(dbPath))
    #         dropDatabase(dbPath)
    #     t = table(100:100,`sym`date`qty`price,[SYMBOL,DATE,INT,DOUBLE])
    #     db=database(dbPath,HASH,[DATE, 2])
    #     pt = db.createPartitionedTable(t, `pt, `date)
    #     ''')
    #     appender = ddb.PartitionedTableAppender("dfs://PTA_test", "pt", "date", self.pool)
    #     sym = list(map(str, np.arange(10001, 60001)))
    #     x = np.arange('2012-01-01', '2012-01-06', dtype='datetime64[D]')
    #     date = np.repeat(x, 10000, axis=0)
    #     qty = np.random.randint(0, 101, 50000)
    #     price = np.random.randint(0, 60001, 50000) * 0.1
    #     data = pd.DataFrame({'sym': sym, 'date': date, 'qty': qty, 'price': price})
    #     num = appender.append(data)
    #     self.assertEqual(num, 50000)
    #     re = self.s.run("select * from loadTable('dfs://PTA_test', 'pt') order by date, sym, qty, price")
    #     expected = data.sort_values(by=['date', 'sym', 'qty', 'price'], ascending=[True, True, True, True])
    #     expected.set_index(np.arange(0, 50000), inplace=True)
    #     assert_frame_equal(re, expected, check_dtype=False)

    # def test_PartitionedTableAppender_hash_datetime(self):
    #     self.s.run('''
    #      dbPath = "dfs://PTA_test"
    #      if(existsDatabase(dbPath))
    #          dropDatabase(dbPath)
    #      t = table(100:100,`sym`datetime`qty`price,[SYMBOL,DATETIME,INT,DOUBLE])
    #      db=database(dbPath,HASH,[DATETIME, 2])
    #      pt = db.createPartitionedTable(t, `pt, `datetime)
    #      ''')
    #     appender = ddb.PartitionedTableAppender("dfs://PTA_test", "pt", "datetime", self.pool)
    #     sym = list(map(str, np.arange(10001, 60001)))
    #     x = np.arange('2012-01-01T00:00:01', '2012-01-01T00:00:06', dtype='datetime64[s]')
    #     datetime = np.repeat(x, 10000, axis=0)
    #     qty = np.random.randint(0, 101, 50000)
    #     price = np.random.randint(0, 60001, 50000) * 0.1
    #     data = pd.DataFrame({'sym': sym, 'datetime': datetime, 'qty': qty, 'price': price})
    #     num = appender.append(data)
    #     self.assertEqual(num, 50000)
    #     re = self.s.run("select * from loadTable('dfs://PTA_test', 'pt') order by datetime, sym, qty, price")
    #     expected = data.sort_values(by=['datetime', 'sym', 'qty', 'price'], ascending=[True, True, True, True])
    #     expected.set_index(np.arange(0, 50000), inplace=True)
    #     assert_frame_equal(re, expected, check_dtype=False)


    # def test_PartitionedTableAppender_hash_nanotimestamp(self):
    #     self.s.run('''
    #     dbPath = "dfs://PTA_test"
    #     if(existsDatabase(dbPath))
    #         dropDatabase(dbPath)
    #     t = table(100:100,`sym`nanotimestamp`qty`price,[SYMBOL,NANOTIMESTAMP,INT,DOUBLE])
    #     db=database(dbPath,HASH,[NANOTIMESTAMP, 2])
    #     pt = db.createPartitionedTable(t, `pt, `nanotimestamp)
    #     ''')
    #     appender = ddb.PartitionedTableAppender("dfs://PTA_test", "pt", "nanotimestamp", self.pool)
    #     sym = list(map(str, np.arange(10001, 60001)))
    #     x = np.arange('2012-01-01T00:00:00.000000001', '2012-01-01T00:00:00.000000006', dtype='datetime64[ns]')
    #     date = np.repeat(x, 10000, axis=0)
    #     qty = np.random.randint(0, 101, 50000)
    #     price = np.random.randint(0, 60001, 50000) * 0.1
    #     data = pd.DataFrame({'sym': sym, 'nanotimestamp': nanotimestamp, 'qty': qty, 'price': price})
    #     num = appender.append(data)
    #     self.assertEqual(num, 50000)
    #     re = self.s.run("select * from loadTable('dfs://PTA_test', 'pt') order by nanotimestamp, sym, qty, price")
    #     expected = data.sort_values(by=['date', 'nanotimestamp', 'qty', 'price'], ascending=[True, True, True, True])
    #     expected.set_index(np.arange(0, 50000), inplace=True)
    #     assert_frame_equal(re, expected, check_dtype=False)




    # TODO
    def test_PartitionedTableAppender_list_int(self):
        self.s.run('''
        dbPath = "dfs://PTA_test"
        if(existsDatabase(dbPath))
            dropDatabase(dbPath)
        t = table(100:100,`sym`id`qty`price,[SYMBOL,INT,INT,DOUBLE])
        db=database(dbPath,LIST,[[1, 3, 5], [2, 4, 6], [7, 8, 9, 10]])
        pt = db.createPartitionedTable(t, `pt, `id)
        ''')
        appender = ddb.PartitionedTableAppender("dfs://PTA_test", "pt", "id", self.pool)
        sym = list(map(str, np.arange(10001, 60001)))
        id = np.repeat(np.arange(1, 11), 5000, axis=0)
        qty = np.random.randint(0, 101, 50000)
        price = np.random.randint(0, 60001, 50000)*0.1
        data = pd.DataFrame({'sym': sym, 'id': id, 'qty': qty, 'price': price})
        num = appender.append(data)
        self.assertEqual(num, 50000)
        re = self.s.run("select * from loadTable('dfs://PTA_test', 'pt') order by id, sym, qty, price")
        expected = data.sort_values(by=['id', 'sym', 'qty', 'price'], ascending=[True, True, True, True])
        expected.set_index(np.arange(0, 50000), inplace=True)
        assert_frame_equal(re, expected, check_dtype=False)

    def test_PartitionedTableAppender_list_short(self):
        self.s.run('''
        dbPath = "dfs://PTA_test"
        if(existsDatabase(dbPath))
            dropDatabase(dbPath)
        t = table(100:100,`sym`id`qty`price,[SYMBOL,SHORT,INT,DOUBLE])
        db=database(dbPath,LIST,[short([1, 3, 5]), short([2, 4, 6]), short([7, 8, 9, 10])])
        pt = db.createPartitionedTable(t, `pt, `id)
        ''')
        appender = ddb.PartitionedTableAppender("dfs://PTA_test", "pt", "id", self.pool)
        sym = list(map(str, np.arange(10001, 60001)))
        id = np.repeat(np.arange(1, 11), 5000, axis=0)
        qty = np.random.randint(0, 101, 50000)
        price = np.random.randint(0, 60001, 50000)*0.1
        data = pd.DataFrame({'sym': sym, 'id': id, 'qty': qty, 'price': price})
        num = appender.append(data)
        self.assertEqual(num, 50000)
        re = self.s.run("select * from loadTable('dfs://PTA_test', 'pt') order by id, sym, qty, price")
        expected = data.sort_values(by=['id', 'sym', 'qty', 'price'], ascending=[True, True, True, True])
        expected.set_index(np.arange(0, 50000), inplace=True)
        assert_frame_equal(re, expected, check_dtype=False)
    
    def test_PartitionedTableAppender_list_symbol(self):
        self.s.run('''
        dbPath = "dfs://PTA_test"
        if(existsDatabase(dbPath))
            dropDatabase(dbPath)
        t = table(100:100,`sym`id`qty`price,[SYMBOL,INT,INT,DOUBLE])
        db=database(dbPath,LIST,[symbol(string(10001..20000)), symbol(string(20001..40000)), symbol(string(40001..60000))])
        pt = db.createPartitionedTable(t, `pt, `sym)
        ''')
        appender = ddb.PartitionedTableAppender("dfs://PTA_test", "pt", "sym", self.pool)
        sym = list(map(str, np.arange(10001, 60001)))
        id = np.random.randint(0, 60001, 50000)
        qty = np.random.randint(0, 101, 50000)
        price = np.random.randint(0, 60001, 50000)*0.1
        data = pd.DataFrame({'sym': sym, 'id': id, 'qty': qty, 'price': price})
        num = appender.append(data)
        self.assertEqual(num, 50000)
        re = self.s.run("select * from loadTable('dfs://PTA_test', 'pt') order by id, sym, qty, price")
        expected = data.sort_values(by=['id', 'sym', 'qty', 'price'], ascending=[True, True, True, True])
        expected.set_index(np.arange(0, 50000), inplace=True)
        assert_frame_equal(re, expected, check_dtype=False)
    
    def test_PartitionedTableAppender_list_string(self):
        self.s.run('''
        dbPath = "dfs://PTA_test"
        if(existsDatabase(dbPath))
            dropDatabase(dbPath)
        t = table(100:100,`sym`id`qty`price,[SYMBOL,INT,INT,DOUBLE])
        db=database(dbPath,LIST,[string(10001..20000), string(20001..40000), string(40001..60000)])
        pt = db.createPartitionedTable(t, `pt, `sym)
        ''')
        appender = ddb.PartitionedTableAppender("dfs://PTA_test", "pt", "sym", self.pool)
        sym = list(map(str, np.arange(10001, 60001)))
        id = np.random.randint(0, 60001, 50000)
        qty = np.random.randint(0, 101, 50000)
        price = np.random.randint(0, 60001, 50000)*0.1
        data = pd.DataFrame({'sym': sym, 'id': id, 'qty': qty, 'price': price})
        num = appender.append(data)
        self.assertEqual(num, 50000)
        re = self.s.run("select * from loadTable('dfs://PTA_test', 'pt') order by id, sym, qty, price")
        expected = data.sort_values(by=['id', 'sym', 'qty', 'price'], ascending=[True, True, True, True])
        expected.set_index(np.arange(0, 50000), inplace=True)
        assert_frame_equal(re, expected, check_dtype=False)

    # def test_PartitionedTableAppender_compo_value_range(self):
    #     self.s.run('''
    #             dbPath="dfs://db_compoDB_date"
    #             if (existsDatabase(dbPath))
    #                 dropDatabase(dbPath)
    #             t = table(100:100,`date`val,[DATE,INT])
    #             dbDate = database(,VALUE,2017.08.07..2017.08.11)
    #             dbVal = database(, RANGE, 0 40000 80000 120000)
    #             db = database("dfs://db_compoDB_date", COMPO, [dbDate, dbVal])
    #             pt = db.createPartitionedTable(t, `pt, `date`val)
    #             ''')
    #     appender = ddb.PartitionedTableAppender("dfs://db_compoDB_date", "pt", "date",self.pool)
    #     n = 100000
    #     x = np.array(['2017-08-07', '2017-08-08', '2017-08-09', '2017-08-10','2017-08-11'],dtype = "datetime64[D]")
    #     data = pd.DataFrame({"date":np.repeat(x,20000), "val": range(0, n)})
    #     data["val"] = data["val"].astype("int32")
    #     re = appender.append(data)
    #     self.assertEqual(re, n)
    #     re = self.s.run('''select * from loadTable("dfs://db_compoDB_date",`pt)''')
    #     assert_frame_equal(data, re)


    def test_PartitionedTableAppender_compo_value_list(self):
        self.s.run('''
                dbPath="dfs://db_compoDB_sym"
                if (existsDatabase(dbPath))
                    dropDatabase(dbPath)
                t = table(100:100,`sym`ticker,[SYMBOL,STRING])
                dbSym = database(,VALUE,`aaa`bbb`ccc`ddd)
                dbTic = database(, LIST, [`IBM`ORCL`MSFT, `GOOG`FB] )
                db = database("dfs://db_compoDB_sym", COMPO, [dbSym, dbTic])
                pt = db.createPartitionedTable(t, `pt, `sym`ticker)
                ''')
        appender = ddb.PartitionedTableAppender("dfs://db_compoDB_sym", "pt", "sym", self.pool)
        n = 100000
        x = np.array(['aaa', 'bbb', 'ccc', 'ddd'])
        y = np.array(['IBM', 'ORCL', 'MSFT','GOOG', 'FB'])
        data = pd.DataFrame({"sym":np.repeat(x,25000), "ticker": np.repeat(y,20000)})
        re = appender.append(data)
        self.assertEqual(re, n)
        re = self.s.run('''select * from loadTable("dfs://db_compoDB_sym",`pt)''')
        assert_frame_equal(data, re)


    def test_PartitionedTableAppender_compo_range_list(self):
        self.s.run('''
                dbPath="dfs://db_compoDB_int"
                if (existsDatabase(dbPath))
                    dropDatabase(dbPath)
                t = table(100:100,`id`ticker,[INT,STRING])
                dbId = database(,RANGE,0 40000 80000 120000)
                dbTic = database(, LIST, [`IBM`ORCL`MSFT, `GOOG`FB] )
                db = database("dfs://db_compoDB_int", COMPO, [dbId, dbTic])
                pt = db.createPartitionedTable(t, `pt, `id`ticker)
                ''')
        appender = ddb.PartitionedTableAppender("dfs://db_compoDB_int", "pt", "id", self.pool)
        n = 100000
        y = np.array(['IBM', 'ORCL', 'MSFT','GOOG', 'FB'])
        data = pd.DataFrame({"id":range(0,n), "ticker": np.repeat(y,20000)})
        data['id'] = data["id"].astype("int32")
        re = appender.append(data)
        self.assertEqual(re, n)
        re = self.s.run('''select * from loadTable("dfs://db_compoDB_int",`pt)''')
        assert_frame_equal(data, re)

    def test_PartitionedTableAppender_compo_hash_range(self):
        self.s.run('''
                        dbPath="dfs://db_compoDB_int"
                        if (existsDatabase(dbPath))
                            dropDatabase(dbPath)
                        t = table(100:100,`id`ticker,[INT,STRING])
                        dbId = database(,HASH,[INT,2])
                        sym_range=cutPoints(string(10001..60000), 10)
                        dbTic = database(, RANGE, sym_range )
                        db = database("dfs://db_compoDB_int", COMPO, [dbId, dbTic])
                        pt = db.createPartitionedTable(t, `pt, `id`ticker)
                        ''')
        appender = ddb.PartitionedTableAppender("dfs://db_compoDB_int", "pt", "id", self.pool)
        n = 50000
        id = np.repeat(np.arange(1, 5001), 10, axis=0)
        ticker = list(map(str, np.arange(10001, 60001)))
        data = pd.DataFrame({"id": id, "ticker": ticker})
        data['id'] = data["id"].astype("int32")
        re = appender.append(data)
        self.assertEqual(re, n)
        re = self.s.run('''select * from loadTable("dfs://db_compoDB_int",`pt) order by id,ticker''')
        assert_frame_equal(data, re)

    def test_PartitionedTableAppender_compo_hash_list(self):
        self.s.run('''
                                dbPath="dfs://db_compoDB_sym"
                                if (existsDatabase(dbPath))
                                    dropDatabase(dbPath)
                                t = table(100:100,`sym`ticker,[SYMBOL,STRING])
                                dbSym = database(,HASH,[SYMBOL,2])
                                dbTic = database(, LIST,  [`IBM`ORCL`MSFT, `GOOG`FB] )
                                db = database("dfs://db_compoDB_sym", COMPO, [dbSym, dbTic])
                                pt = db.createPartitionedTable(t, `pt, `sym`ticker)
                                ''')
        appender = ddb.PartitionedTableAppender("dfs://db_compoDB_sym", "pt", "sym", self.pool)
        sym = list(map(str, np.arange(10001, 60001)))
        y = np.array(['IBM', 'ORCL', 'MSFT', 'GOOG', 'FB'])
        ticker = list(map(str, np.arange(10001, 60001)))
        data = pd.DataFrame({"sym": sym, "ticker": np.repeat(y, 10000)})
        re = appender.append(data)
        self.assertEqual(re, 50000)
        re = self.s.run('''select * from loadTable("dfs://db_compoDB_sym",`pt) order by sym,ticker''')
        assert_frame_equal(data, re)



    def test_PartitionedTableAppender_compo_hash_value(self):
        self.s.run('''
                                dbPath="dfs://db_compoDB_str"
                                if (existsDatabase(dbPath))
                                    dropDatabase(dbPath)
                                t = table(100:100,`str`ticker,[STRING,SYMBOL])
                                dbStr = database(,HASH,[STRING,10])
                                dbTic = database(, VALUE,  symbol(['AAPL', 'MSFT', 'IBM', 'GOOG', 'YHOO']) )
                                db = database("dfs://db_compoDB_str", COMPO, [dbStr, dbTic])
                                pt = db.createPartitionedTable(t, `pt, `str`ticker)
                                ''')
        appender = ddb.PartitionedTableAppender("dfs://db_compoDB_str", "pt", "str", self.pool)
        n = 50000
        y = list(map(str, np.arange(10001, 60001)))
        ticker = np.repeat(['AAPL', 'MSFT', 'IBM', 'GOOG', 'YHOO'], 10000)
        data = pd.DataFrame({"str": y, "ticker": ticker})
        re = appender.append(data)
        self.assertEqual(re, n)
        re = self.s.run('''select * from loadTable("dfs://db_compoDB_str",`pt) order by str,ticker''')
        assert_frame_equal(data, re)


    def test_PartitionedTableAppender_compo_value_list_range(self):
        self.s.run('''
                   dbPath="dfs://db_compoDB_sym"
                   if (existsDatabase(dbPath))
                       dropDatabase(dbPath)
                   t = table(100:100,`sym`ticker`id,[SYMBOL,STRING,INT])
                   dbSym = database(,VALUE,`aaa`bbb`ccc`ddd)
                   dbTic = database(, LIST, [`IBM`ORCL`MSFT, `GOOG`FB] )
                   dbId = database(,RANGE,0 40000 80000 120000)
                   db = database("dfs://db_compoDB_sym", COMPO, [dbSym, dbTic,dbId])
                   pt = db.createPartitionedTable(t, `pt, `sym`ticker`id)
                   ''')
        appender = ddb.PartitionedTableAppender("dfs://db_compoDB_sym", "pt", "sym", self.pool)
        n = 100000
        x = np.array(['aaa', 'bbb', 'ccc', 'ddd'])
        y = np.array(['IBM', 'ORCL', 'MSFT', 'GOOG', 'FB'])
        data = pd.DataFrame({"sym": np.repeat(x, 25000), "ticker": np.repeat(y, 20000), 'id': range(0, n)})
        data['id'] = data["id"].astype("int32")
        re = appender.append(data)
        self.assertEqual(re, n)
        re = self.s.run('''select * from loadTable("dfs://db_compoDB_sym",`pt)''')
        assert_frame_equal(data, re)




if __name__ == '__main__':
    unittest.main()

