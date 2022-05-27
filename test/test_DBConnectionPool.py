from datetime import datetime
import unittest
import time

import dolphindb as ddb
from pandas.testing import assert_frame_equal
from setup import HOST, PORT, WORK_DIR, DATA_DIR
import asyncio
import numpy as np
import pandas as pd
from numpy.testing import assert_array_equal, assert_array_almost_equal
from pandas.testing import assert_series_equal
from pandas.testing import assert_frame_equal


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
        price = np.random.randint(0, 60001, 50000) * 0.1
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


    def test_DBConnectionPool_read_dfs_table_runTaskAsyn_Unspecified_time(self):
            create_value_db()
            pool = ddb.DBConnectionPool(HOST, PORT, 10, "admin", "123456")
            pool.startLoop()
            t1 = time.time()
            task1 = pool.runTaskAsyn("sleep(1000);exec count(*) from loadTable('dfs://test_dbConnection', 'pt')");
            task2 = pool.runTaskAsyn("sleep(2000);exec count(*) from loadTable('dfs://test_dbConnection', 'pt')");
            task3 = pool.runTaskAsyn("sleep(4000);exec count(*) from loadTable('dfs://test_dbConnection', 'pt')");
            task4 = pool.runTaskAsyn("sleep(1000);exec count(*) from loadTable('dfs://test_dbConnection', 'pt')");
            t2 = time.time()
            self.assertEqual(task1.result(), 1000000)
            t3 = time.time()
            self.assertEqual(task4.result(), 1000000)
            t4 = time.time()
            self.assertEqual(task2.result(), 1000000)
            t5 = time.time()
            self.assertEqual(task3.result(), 1000000)
            t6 = time.time()
            self.assertTrue(t2 - t1 < 1)
            print(t2 - t1)
            print(t3 - t1)
            print(t4 - t1)
            print(t5 - t1)
            print(t6 - t1)
            pool.shutDown()

    def test_DBConnectionPool_read_dfs_table_runTaskAsyn_set_time(self):
            create_value_db()
            pool = ddb.DBConnectionPool(HOST, PORT, 10, "admin", "123456")
            pool.startLoop()
            t1 = time.time()
            task1 = pool.runTaskAsyn("sleep(1000);exec count(*) from loadTable('dfs://test_dbConnection', 'pt')");
            task2 = pool.runTaskAsyn("sleep(2000);exec count(*) from loadTable('dfs://test_dbConnection', 'pt')");
            task3 = pool.runTaskAsyn("sleep(4000);exec count(*) from loadTable('dfs://test_dbConnection', 'pt')");
            task4 = pool.runTaskAsyn("sleep(1000);exec count(*) from loadTable('dfs://test_dbConnection', 'pt')");
            t2 = time.time()
            self.assertEqual(task1.result(2), 1000000)
            t3 = time.time()
            self.assertEqual(task4.result(2), 1000000)
            t4 = time.time()
            self.assertEqual(task2.result(2), 1000000)
            t5 = time.time()
            self.assertEqual(task3.result(4), 1000000)
            t6 = time.time()
            self.assertTrue(t2 - t1 < 1)
            print(t2 - t1)
            print(t3 - t1)
            print(t4 - t1)
            print(t5 - t1)
            print(t6 - t1)
            pool.shutDown()

    def test_DBConnectionPool_read_dfs_table_runTaskAsyn_unfinished(self):
            create_value_db()
            pool = ddb.DBConnectionPool(HOST, PORT, 10, "admin", "123456")
            pool.startLoop()
            t1 = time.time()
            task1 = pool.runTaskAsyn("sleep(7000);exec count(*) from loadTable('dfs://test_dbConnection', 'pt')");
            t2 = time.time()
            try:
                self.assertEqual(task1.result(1), 1000000)
            except:
                self.assertEqual(task1.result(8), 1000000)
            self.assertTrue(t2 - t1 < 1)
            pool.shutDown()


    def test_PartitionedTableAppender_paramere(self):
        self.s.run('''
        dbPath = "dfs://PTA_test"
        if(existsDatabase(dbPath))
            dropDatabase(dbPath)
        t = table(100:100,`sym`id`qty`price,[SYMBOL,INT,INT,DOUBLE])
        db=database(dbPath,RANGE,[1,10001,20001,30001,40001,50001,60001])
        pt = db.createPartitionedTable(t, `pt, `id)
        ''')
        with self.assertRaises(TypeError):
            ddb.PartitionedTableAppender(dbPath_ERROR="dfs://PTA_test", tableName="pt", partitionColName="id", dbConnectionPool=self.pool)
        with self.assertRaises(TypeError):
            ddb.PartitionedTableAppender(dbPath="dfs://PTA_test", tableName_ERROR="pt", partitionColName="id", dbConnectionPool=self.pool)
        with self.assertRaises(TypeError):
            ddb.PartitionedTableAppender(dbPath="dfs://PTA_test", tableName="pt", partitionColName_ERROR="id", dbConnectionPool=self.pool)
        with self.assertRaises(TypeError):
            ddb.PartitionedTableAppender(dbPath="dfs://PTA_test", tableName="pt", partitionColName="id", dbConnectionPool_ERROR=self.pool)
              
        appender = ddb.PartitionedTableAppender(dbPath="dfs://PTA_test", tableName="pt", partitionColName="id", dbConnectionPool=self.pool)

    def test_DBConnectionPool_paramere(self):
        ddb.DBConnectionPool(host=HOST,port=PORT, threadNum=10, userid="admin", password="123456",loadBalance=False, highAvailability=False, reConnectFlag=True)

    def test_DBConnectionPool_paramere(self):
        script='''
        dbPath = "dfs://PartitionedTableAppender"
        if(existsDatabase(dbPath))
            dropDatabase(dbPath)
        t = table(1000:0, `sym`date`month`time`minute`second`datetime`timestamp`nanotimestamp`qty, [SYMBOL, DATE,MONTH,TIME,MINUTE,SECOND,DATETIME,TIMESTAMP,NANOTIMESTAMP, INT])
        db=database(dbPath,RANGE,100000 200000 300000 400000 600001)
        pt = db.createPartitionedTable(t, `pt, `qty)
        '''
        self.s.run(script)
        appender = ddb.PartitionedTableAppender("dfs://PartitionedTableAppender","pt", "qty",self.pool)
        sym = list(map(str, np.arange(100000, 600000)))
        date = np.array(np.tile(['2012-01-01', 'NaT', '1965-07-25', 'NaT', '2020-12-23', '1970-01-01', 'NaT', 'NaT', 'NaT', '2009-08-05'],50000), dtype="datetime64[D]")
        month = np.array(np.tile(['1965-08', 'NaT','2012-02', '2012-03', 'NaT'],100000), dtype="datetime64")
        time = np.array(np.tile(['2012-01-01T00:00:00.000', '2015-08-26T05:12:48.426', 'NaT', 'NaT', '2015-06-09T23:59:59.999'],100000), dtype="datetime64")
        second = np.array(np.tile(['2012-01-01T00:00:00', '2015-08-26T05:12:48', 'NaT', 'NaT', '2015-06-09T23:59:59'],100000), dtype="datetime64")
        nanotime = np.array(np.tile(['2012-01-01T00:00:00.000000000', '2015-08-26T05:12:48.008007006', 'NaT', 'NaT', '2015-06-09T23:59:59.999008007'],100000), dtype="datetime64")
        qty = np.arange(100000, 600000)
        data = pd.DataFrame({'sym': sym, 'date': date, 'month':month, 'time':time, 'minute':time, 'second':second, 'datetime':second, 'timestamp':time, 'nanotimestamp':nanotime, 'qty': qty})
        num = appender.append(data)
        print(num)
        print(self.s.run("select * from pt"))

    def test_compare(self):
        script='''
        dbPath = "dfs://tableAppender"
        if(existsDatabase(dbPath))
            dropDatabase(dbPath)
        t = table(1000:0, `sym`date`month`time`minute`second`datetime`timestamp`nanotimestamp`qty, [SYMBOL, DATE,MONTH,TIME,MINUTE,SECOND,DATETIME,TIMESTAMP,NANOTIMESTAMP, INT])
        db=database(dbPath,RANGE,100000 200000 300000 400000 600001)
        pt = db.createPartitionedTable(t, `pt, `qty)
        '''
        self.s.run(script)
        appender = ddb.tableAppender("dfs://tableAppender","pt", self.s)
        sym = list(map(str, np.arange(100000, 600000)))
        date = np.array(np.tile(['2012-01-01', 'NaT', '1965-07-25', 'NaT', '2020-12-23', '1970-01-01', 'NaT', 'NaT', 'NaT', '2009-08-05'],50000), dtype="datetime64[D]")
        month = np.array(np.tile(['1965-08', 'NaT','2012-02', '2012-03', 'NaT'],100000), dtype="datetime64")
        time = np.array(np.tile(['2012-01-01T00:00:00.000', '2015-08-26T05:12:48.426', 'NaT', 'NaT', '2015-06-09T23:59:59.999'],100000), dtype="datetime64")
        second = np.array(np.tile(['2012-01-01T00:00:00', '2015-08-26T05:12:48', 'NaT', 'NaT', '2015-06-09T23:59:59'],100000), dtype="datetime64")
        nanotime = np.array(np.tile(['2012-01-01T00:00:00.000000000', '2015-08-26T05:12:48.008007006', 'NaT', 'NaT', '2015-06-09T23:59:59.999008007'],100000), dtype="datetime64")
        qty = np.arange(100000, 600000)
        data = pd.DataFrame({'sym': sym, 'date': date, 'month':month, 'time':time, 'minute':time, 'second':second, 'datetime':second, 'timestamp':time, 'nanotimestamp':nanotime, 'qty': qty})
        num = appender.append(data)
        print(num)
        print(self.s.run("select * from pt"))

#-------------------------------------------------------------------------------------------------
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

    # # TODO
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

    def test_PartitionedTableAppender_all_time_types(self):
        self.s.run('''
        dbPath = "dfs://test_PartitionedTableAppender_all_time_types"
        if(existsDatabase(dbPath))
            dropDatabase(dbPath)
        t = table(1000:0, `sym`date`month`time`minute`second`datetime`timestamp`nanotime`nanotimestamp`qty, [SYMBOL, DATE,MONTH,TIME,MINUTE,SECOND,DATETIME,TIMESTAMP,NANOTIME,NANOTIMESTAMP,INT])
        db=database(dbPath,RANGE,100000 200000 300000 400000 600001)
        pt = db.createPartitionedTable(t, `pt, `qty)
        ''')
        appender = ddb.PartitionedTableAppender("dfs://test_PartitionedTableAppender_all_time_types","pt","qty", self.pool)
        sym = list(map(str, np.arange(100000, 600000)))
        date = np.array(np.tile(['2012-01-01', 'NaT', '1965-07-25', 'NaT', '2020-12-23', '1970-01-01', 'NaT', 'NaT', 'NaT', '2009-08-05'],50000), dtype="datetime64[D]")
        month = np.array(np.tile(['1965-08', 'NaT','2012-02', '2012-03', 'NaT'],100000), dtype="datetime64")
        time = np.array(np.tile(['2012-01-01T00:00:00.000', '2015-08-26T05:12:48.426', 'NaT', 'NaT', '2015-06-09T23:59:59.999'],100000), dtype="datetime64")
        second = np.array(np.tile(['2012-01-01T00:00:00', '2015-08-26T05:12:48', 'NaT', 'NaT', '2015-06-09T23:59:59'],100000), dtype="datetime64")
        nanotime = np.array(np.tile(['2012-01-01T00:00:00.000000000', '2015-08-26T05:12:48.008007006', 'NaT', 'NaT', '2015-06-09T23:59:59.999008007'],100000), dtype="datetime64")
        qty = np.arange(100000, 600000)
        data = pd.DataFrame({'sym': sym, 'date': date, 'month':month, 'time':time, 'minute':time, 'second':second, 'datetime':second, 'timestamp':time, 'nanotime':nanotime, 'nanotimestamp':nanotime, 'qty': qty})
        num = appender.append(data)
        self.assertEqual(num, 500000)
        script='''
        n = 500000
        tmp=table(string(100000..599999) as sym, take([2012.01.01, NULL, 1965.07.25, NULL, 2020.12.23, 1970.01.01, NULL, NULL, NULL, 2009.08.05],n) as date,take([1965.08M, NULL, 2012.02M, 2012.03M, NULL],n) as month,
        take([00:00:00.000, 05:12:48.426, NULL, NULL, 23:59:59.999],n) as time, take([00:00m, 05:12m, NULL, NULL, 23:59m],n) as minute, take([00:00:00, 05:12:48, NULL, NULL, 23:59:59],n) as second,take([2012.01.01T00:00:00, 2015.08.26T05:12:48, NULL, NULL, 2015.06.09T23:59:59],n) as datetime,
        take([2012.01.01T00:00:00.000, 2015.08.26T05:12:48.426, NULL, NULL, 2015.06.09T23:59:59.999],n) as timestamp,take([00:00:00.000000000, 05:12:48.008007006, NULL, NULL, 23:59:59.999008007],n) as nanotime,take([2012.01.01T00:00:00.000000000, 2015.08.26T05:12:48.008007006, NULL, NULL, 2015.06.09T23:59:59.999008007],n) as nanotimestamp,
        100000..599999 as qty)
        re = select * from loadTable("dfs://test_PartitionedTableAppender_all_time_types",`pt)
        each(eqObj, tmp.values(), re.values())
        '''
        re = self.s.run(script)
        assert_array_equal(re, [True, True, True,True, True, True,True, True, True,True, True])

    def test_PartitionedTableAppender_dfs_table_datehour(self):
        self.s.run('''
        dbPath = "dfs://test_PartitionedTableAppender_datehour"
        if(existsDatabase(dbPath))
            dropDatabase(dbPath)
        t = table(datehour(2020.01.01T01:01:01) as time, 1 as qty)
        db=database(dbPath,RANGE,0 100000 200000 300000 400000 600001)
        pt = db.createPartitionedTable(t, `pt, `qty)
        ''')
        appender = ddb.PartitionedTableAppender("dfs://test_PartitionedTableAppender_datehour","pt","qty",self.pool)
        n = 500000
        time = pd.date_range(start='2020-01-01T01',periods=n,freq='h')
        qty = np.arange(1,n+1)
        data = pd.DataFrame({'time':time,'qty':qty})
        num = appender.append(data)
        self.assertEqual(num, n)
        script = '''
        n = 500000
        ex = table((datehour(2020.01.01T00:01:01)+1..n) as time,1..n as qty)
        re = select * from loadTable("dfs://test_PartitionedTableAppender_datehour",`pt)
        each(eqObj, re.values(), ex.values())
        '''
        re = self.s.run(script)
        assert_array_equal(re, [True, True])

    def test_PartitionedTableAppender_dfs_table_to_date(self):
        self.s.run('''
        dbPath = "dfs://test_PartitionedTableAppender_to_date"
        if(existsDatabase(dbPath))
            dropDatabase(dbPath)
        t = table(100:100,`id`date1`date2`date3`date4`date5,[INT,DATE,DATE,DATE,DATE,DATE])
        db=database(dbPath,RANGE,[1,100001,200001,300001,400001,500001,600001])
        pt = db.createPartitionedTable(t, `pt, `id)
        ''')
        appender = ddb.PartitionedTableAppender("dfs://test_PartitionedTableAppender_to_date","pt","id",self.pool)
        n = 500000
        id = np.arange(100000, 600000)
        date = np.array(np.tile(['2012-01-01', 'NaT', '1965-07-25', 'NaT', '1970-01-01'],100000), dtype="datetime64[D]")
        month = np.array(np.tile(['1965-08', 'NaT','2012-02', '2012-03', 'NaT'],100000), dtype="datetime64")
        time = np.array(np.tile(['2012-01-01T00:00:00.000', '2015-08-26T05:12:48.426', 'NaT', 'NaT', '1965-06-09T23:59:59.999'],100000), dtype="datetime64")
        second = np.array(np.tile(['2012-01-01T00:00:00', '2015-08-26T05:12:48', 'NaT', 'NaT', '1965-06-09T23:59:59'],100000), dtype="datetime64")
        nanotime = np.array(np.tile(['2012-01-01T00:00:00.000000000', '2015-08-26T05:12:48.008007006', 'NaT', 'NaT', '1965-06-09T23:59:59.999008007'],100000), dtype="datetime64")
        data = pd.DataFrame({'id':id,'date1':date,'date2':month,'date3':time,'date4':second,'date5':nanotime})
        num = appender.append(data)
        self.assertEqual(num, n)
        script = '''
        n = 500000
        ids = 100000..599999
        time_name = `date
        dates =funcByName(time_name)(take([2012.01.01, NULL,1965.07.25, NULL, 1970.01.01],n))
        months =  funcByName(time_name)(take([1965.08M, NULL,2012.02M, 2012.03M, NULL],n))
        times = funcByName(time_name)(take([2012.01.01T00:00:00.000, 2015.08.26T05:12:48.426, NULL, NULL, 1965.06.09T23:59:59.999],n))
        seconds = funcByName(time_name)(take([2012.01.01T00:00:00, 2015.08.26T05:12:48, NULL, NULL, 1965.06.09T23:59:59],n))
        nanotimes = funcByName(time_name)(take([2012.01.01T00:00:00.000000000, 2015.08.26T05:12:48.008007006,NULL, NULL, 1965.06.09T23:59:59.999008007],n))
        ex = table(ids as id,dates as date1,months as date2,times as date3,seconds as date4,nanotimes as date5)
        re = select * from loadTable("dfs://test_PartitionedTableAppender_to_date",`pt)
        each(eqObj, re.values(), ex.values())
        '''
        re = self.s.run(script)
        assert_array_equal(re, [True, True, True, True, True, True])

    def test_PartitionedTableAppender_dfs_table_to_month(self):
        self.s.run('''
        dbPath = "dfs://test_PartitionedTableAppender_to_month"
        if(existsDatabase(dbPath))
            dropDatabase(dbPath)
        t = table(100:100,`id`month1`month2`month3`month4`month5,[INT,MONTH,MONTH,MONTH,MONTH,MONTH])
        db=database(dbPath,RANGE,[1,100001,200001,300001,400001,500001,600001])
        pt = db.createPartitionedTable(t, `pt, `id)
        ''')
        appender = ddb.PartitionedTableAppender("dfs://test_PartitionedTableAppender_to_month","pt","id",self.pool)
        n = 500000
        id = np.arange(100000, 600000)
        date = np.array(np.tile(['2012-01-01', 'NaT', '1965-07-25', 'NaT', '1970-01-01'],100000), dtype="datetime64[D]")
        month = np.array(np.tile(['1965-08', 'NaT','2012-02', '2012-03', 'NaT'],100000), dtype="datetime64")
        time = np.array(np.tile(['2012-01-01T00:00:00.000', '2015-08-26T05:12:48.426', 'NaT', 'NaT', '1965-06-09T23:59:59.999'],100000), dtype="datetime64")
        second = np.array(np.tile(['2012-01-01T00:00:00', '2015-08-26T05:12:48', 'NaT', 'NaT', '1965-06-09T23:59:59'],100000), dtype="datetime64")
        nanotime = np.array(np.tile(['2012-01-01T00:00:00.000000000', '2015-08-26T05:12:48.008007006', 'NaT', 'NaT', '1965-06-09T23:59:59.999008007'],100000), dtype="datetime64")
        data = pd.DataFrame({'id':id,'month1':date,'month2':month,'month3':time,'month4':second,'month5':nanotime})
        num = appender.append(data)
        self.assertEqual(num, n)
        script = '''
        n = 500000
        ids = 100000..599999
        time_name = `month
        dates =funcByName(time_name)(take([2012.01.01, NULL,1965.07.25, NULL, 1970.01.01],n))
        months =  funcByName(time_name)(take([1965.08M, NULL,2012.02M, 2012.03M, NULL],n))
        times = funcByName(time_name)(take([2012.01.01T00:00:00.000, 2015.08.26T05:12:48.426, NULL, NULL, 1965.06.09T23:59:59.999],n))
        seconds = funcByName(time_name)(take([2012.01.01T00:00:00, 2015.08.26T05:12:48, NULL, NULL, 1965.06.09T23:59:59],n))
        nanotimes = funcByName(time_name)(take([2012.01.01T00:00:00.000000000, 2015.08.26T05:12:48.008007006,NULL, NULL, 1965.06.09T23:59:59.999008007],n))
        ex = table(ids as id,dates as month1,months as month2,times as month3,seconds as month4,nanotimes as month5)
        re = select * from loadTable("dfs://test_PartitionedTableAppender_to_month",`pt)
        each(eqObj, re.values(), ex.values())
        '''
        re = self.s.run(script)
        assert_array_equal(re, [True, True, True, True, True, True])

    def test_PartitionedTableAppender_dfs_table_to_time(self):
        self.s.run('''
        dbPath = "dfs://test_PartitionedTableAppender_to_time"
        if(existsDatabase(dbPath))
            dropDatabase(dbPath)
        t = table(100:100,`id`time1`time2`time3`time4`time5,[INT,TIME,TIME,TIME,TIME,TIME])
        db=database(dbPath,RANGE,[1,100001,200001,300001,400001,500001,600001])
        pt = db.createPartitionedTable(t, `pt, `id)
        ''')
        appender = ddb.PartitionedTableAppender("dfs://test_PartitionedTableAppender_to_time","pt","id",self.pool)
        n = 500000
        id = np.arange(100000, 600000)
        date = np.array(np.tile(['2012-01-01', 'NaT', '1965-07-25', 'NaT', '1970-01-01'],100000), dtype="datetime64[D]")
        month = np.array(np.tile(['1965-08', 'NaT','2012-02', '2012-03', 'NaT'],100000), dtype="datetime64")
        time = np.array(np.tile(['2012-01-01T00:00:00.000', '2015-08-26T05:12:48.426', 'NaT', 'NaT', '1965-06-09T23:59:59.999'],100000), dtype="datetime64")
        second = np.array(np.tile(['2012-01-01T00:00:00', '2015-08-26T05:12:48', 'NaT', 'NaT', '1965-06-09T23:59:59'],100000), dtype="datetime64")
        nanotime = np.array(np.tile(['2012-01-01T00:00:00.000000000', '2015-08-26T05:12:48.008007006', 'NaT', 'NaT', '1965-06-09T23:59:59.999008007'],100000), dtype="datetime64")
        data = pd.DataFrame({'id':id,'time1':date,'time2':month,'time3':time,'time4':second,'time5':nanotime})
        num = appender.append(data)
        self.assertEqual(num, n)
        script = '''
        n = 500000
        ids = 100000..599999
        time_name = `time
        dates =funcByName(time_name)(take([0, NULL,0, NULL, 0],n))
        months =  funcByName(time_name)(take([0, NULL,0, 0, NULL],n))
        times = funcByName(time_name)(take([2012.01.01T00:00:00.000, 2015.08.26T05:12:48.426, NULL, NULL, 1965.06.09T23:59:59.999],n))
        seconds = funcByName(time_name)(take([2012.01.01T00:00:00, 2015.08.26T05:12:48, NULL, NULL, 1965.06.09T23:59:59],n))
        nanotimes = funcByName(time_name)(take([2012.01.01T00:00:00.000000000, 2015.08.26T05:12:48.008007006,NULL, NULL, 1965.06.09T23:59:59.999008007],n))
        ex = table(ids as id,dates as time1,months as time2,times as time3,seconds as time4,nanotimes as time5)
        re = select * from loadTable("dfs://test_PartitionedTableAppender_to_time",`pt)
        each(eqObj, re.values(), ex.values())
        '''
        re = self.s.run(script)
        assert_array_equal(re, [True, True, True, True, True, True])

    def test_PartitionedTableAppender_dfs_table_to_minute(self):
        self.s.run('''
        dbPath = "dfs://test_PartitionedTableAppender_to_minute"
        if(existsDatabase(dbPath))
            dropDatabase(dbPath)
        t = table(100:100,`id`minute1`minute2`minute3`minute4`minute5,[INT,MINUTE,MINUTE,MINUTE,MINUTE,MINUTE])
        db=database(dbPath,RANGE,[1,100001,200001,300001,400001,500001,600001])
        pt = db.createPartitionedTable(t, `pt, `id)
        ''')
        appender = ddb.PartitionedTableAppender("dfs://test_PartitionedTableAppender_to_minute","pt","id",self.pool)
        n = 500000
        id = np.arange(100000, 600000)
        date = np.array(np.tile(['2012-01-01', 'NaT', '1965-07-25', 'NaT', '1970-01-01'],100000), dtype="datetime64[D]")
        month = np.array(np.tile(['1965-08', 'NaT','2012-02', '2012-03', 'NaT'],100000), dtype="datetime64")
        time = np.array(np.tile(['2012-01-01T00:00:00.000', '2015-08-26T05:12:48.426', 'NaT', 'NaT', '1965-06-09T23:59:59.999'],100000), dtype="datetime64")
        second = np.array(np.tile(['2012-01-01T00:00:00', '2015-08-26T05:12:48', 'NaT', 'NaT', '1965-06-09T23:59:59'],100000), dtype="datetime64")
        nanotime = np.array(np.tile(['2012-01-01T00:00:00.000000000', '2015-08-26T05:12:48.008007006', 'NaT', 'NaT', '1965-06-09T23:59:59.999008007'],100000), dtype="datetime64")
        data = pd.DataFrame({'id':id,'minute1':date,'minute2':month,'minute3':time,'minute4':second,'minute5':nanotime})
        num = appender.append(data)
        self.assertEqual(num, n)
        script = '''
        n = 500000
        ids = 100000..599999
        time_name = `minute
        dates =funcByName(time_name)(take([0, NULL,0, NULL, 0],n))
        months =  funcByName(time_name)(take([0, NULL,0, 0, NULL],n))
        times = funcByName(time_name)(take([2012.01.01T00:00:00.000, 2015.08.26T05:12:48.426, NULL, NULL, 1965.06.09T23:59:59.999],n))
        seconds = funcByName(time_name)(take([2012.01.01T00:00:00, 2015.08.26T05:12:48, NULL, NULL, 1965.06.09T23:59:59],n))
        nanotimes = funcByName(time_name)(take([2012.01.01T00:00:00.000000000, 2015.08.26T05:12:48.008007006,NULL, NULL, 1965.06.09T23:59:59.999008007],n))
        ex = table(ids as id,dates as time1,months as time2,times as time3,seconds as time4,nanotimes as time5)
        re = select * from loadTable("dfs://test_PartitionedTableAppender_to_minute",`pt)
        each(eqObj, re.values(), ex.values())
        '''
        re = self.s.run(script)
        assert_array_equal(re, [True, True, True, True, True, True])

    def test_PartitionedTableAppender_dfs_table_to_second(self):
        self.s.run('''
        dbPath = "dfs://test_PartitionedTableAppender_to_second"
        if(existsDatabase(dbPath))
            dropDatabase(dbPath)
        t = table(100:100,`id`second1`second2`second3`second4`second5,[INT,SECOND,SECOND,SECOND,SECOND,SECOND])
        db=database(dbPath,RANGE,[1,100001,200001,300001,400001,500001,600001])
        pt = db.createPartitionedTable(t, `pt, `id)
        ''')
        appender = ddb.PartitionedTableAppender("dfs://test_PartitionedTableAppender_to_second","pt","id",self.pool)
        n = 500000
        id = np.arange(100000, 600000)
        date = np.array(np.tile(['2012-01-01', 'NaT', '1965-07-25', 'NaT', '1970-01-01'],100000), dtype="datetime64[D]")
        month = np.array(np.tile(['1965-08', 'NaT','2012-02', '2012-03', 'NaT'],100000), dtype="datetime64")
        time = np.array(np.tile(['2012-01-01T00:00:00.000', '2015-08-26T05:12:48.426', 'NaT', 'NaT', '1965-06-09T23:59:59.999'],100000), dtype="datetime64")
        second = np.array(np.tile(['2012-01-01T00:00:00', '2015-08-26T05:12:48', 'NaT', 'NaT', '1965-06-09T23:59:59'],100000), dtype="datetime64")
        nanotime = np.array(np.tile(['2012-01-01T00:00:00.000000000', '2015-08-26T05:12:48.008007006', 'NaT', 'NaT', '1965-06-09T23:59:59.999008007'],100000), dtype="datetime64")
        data = pd.DataFrame({'id':id,'second1':date,'second2':month,'second3':time,'second4':second,'second5':nanotime})
        num = appender.append(data)
        self.assertEqual(num, n)
        script = '''
        n = 500000
        ids = 100000..599999
        time_name = `second
        dates =funcByName(time_name)(take([0, NULL,0, NULL, 0],n))
        months =  funcByName(time_name)(take([0, NULL,0, 0, NULL],n))
        times = funcByName(time_name)(take([2012.01.01T00:00:00.000, 2015.08.26T05:12:48.426, NULL, NULL, 1965.06.09T23:59:59.999],n))
        seconds = funcByName(time_name)(take([2012.01.01T00:00:00, 2015.08.26T05:12:48, NULL, NULL, 1965.06.09T23:59:59],n))
        nanotimes = funcByName(time_name)(take([2012.01.01T00:00:00.000000000, 2015.08.26T05:12:48.008007006,NULL, NULL, 1965.06.09T23:59:59.999008007],n))
        ex = table(ids as id,dates as time1,months as time2,times as time3,seconds as time4,nanotimes as time5)
        re = select * from loadTable("dfs://test_PartitionedTableAppender_to_second",`pt)
        each(eqObj, re.values(), ex.values())
        '''
        re = self.s.run(script)
        assert_array_equal(re, [True, True, True, True, True, True])

    def test_PartitionedTableAppender_dfs_table_to_datetime(self):
        self.s.run('''
        dbPath = "dfs://test_PartitionedTableAppender_to_datetime"
        if(existsDatabase(dbPath))
            dropDatabase(dbPath)
        t = table(100:100,`id`datetime1`datetime2`datetime3`datetime4`datetime5,[INT,DATETIME,DATETIME,DATETIME,DATETIME,DATETIME])
        db=database(dbPath,RANGE,[1,100001,200001,300001,400001,500001,600001])
        pt = db.createPartitionedTable(t, `pt, `id)
        ''')
        appender = ddb.PartitionedTableAppender("dfs://test_PartitionedTableAppender_to_datetime","pt","id",self.pool)
        n = 500000
        id = np.arange(100000, 600000)
        date = np.array(np.tile(['2012-01-01', 'NaT', '1965-07-25', 'NaT', '1970-01-01'],100000), dtype="datetime64[D]")
        month = np.array(np.tile(['1965-08', 'NaT','2012-02', '2012-03', 'NaT'],100000), dtype="datetime64")
        time = np.array(np.tile(['2012-01-01T00:00:00.000', '2015-08-26T05:12:48.426', 'NaT', 'NaT', '1965-06-09T23:59:59.999'],100000), dtype="datetime64")
        second = np.array(np.tile(['2012-01-01T00:00:00', '2015-08-26T05:12:48', 'NaT', 'NaT', '1965-06-09T23:59:59'],100000), dtype="datetime64")
        nanotime = np.array(np.tile(['2012-01-01T00:00:00.000000000', '2015-08-26T05:12:48.008007006', 'NaT', 'NaT', '1965-06-09T23:59:59.999008007'],100000), dtype="datetime64")
        data = pd.DataFrame({'id':id,'datetime1':date,'datetime2':month,'datetime3':time,'datetime4':second,'datetime5':nanotime})
        num = appender.append(data)
        self.assertEqual(num, n)
        script = '''
        n = 500000
        ids = 100000..599999
        time_name = `datetime
        dates =funcByName(time_name)(take([2012.01.01, NULL,1965.07.25, NULL, 1970.01.01],n))
        months =  funcByName(time_name)(take([1965.08.01, NULL,2012.02.01, 2012.03.01, NULL],n))
        times = funcByName(time_name)(take([2012.01.01T00:00:00.000, 2015.08.26T05:12:48.426, NULL, NULL, 1965.06.09T23:59:59.999],n))
        seconds = funcByName(time_name)(take([2012.01.01T00:00:00, 2015.08.26T05:12:48, NULL, NULL, 1965.06.09T23:59:59],n))
        nanotimes = funcByName(time_name)(take([2012.01.01T00:00:00.000000000, 2015.08.26T05:12:48.008007006,NULL, NULL, 1965.06.09T23:59:59.999008007],n))
        ex = table(ids as id,dates as month1,months as month2,times as month3,seconds as month4,nanotimes as month5)
        re = select * from loadTable("dfs://test_PartitionedTableAppender_to_datetime",`pt)
        each(eqObj, re.values(), ex.values())
        '''
        re = self.s.run(script)
        assert_array_equal(re, [True, True, True, True, True, True])

    def test_PartitionedTableAppender_dfs_table_to_timestamp(self):
        self.s.run('''
        dbPath = "dfs://test_PartitionedTableAppender_to_timestamp"
        if(existsDatabase(dbPath))
            dropDatabase(dbPath)
        t = table(100:100,`id`timestamp1`timestamp2`timestamp3`timestamp4`timestamp5,[INT,TIMESTAMP,TIMESTAMP,TIMESTAMP,TIMESTAMP,TIMESTAMP])
        db=database(dbPath,RANGE,[1,100001,200001,300001,400001,500001,600001])
        pt = db.createPartitionedTable(t, `pt, `id)
        ''')
        appender = ddb.PartitionedTableAppender("dfs://test_PartitionedTableAppender_to_timestamp","pt","id",self.pool)
        n = 500000
        id = np.arange(100000, 600000)
        date = np.array(np.tile(['2012-01-01', 'NaT', '1965-07-25', 'NaT', '1970-01-01'],100000), dtype="datetime64[D]")
        month = np.array(np.tile(['1965-08', 'NaT','2012-02', '2012-03', 'NaT'],100000), dtype="datetime64")
        time = np.array(np.tile(['2012-01-01T00:00:00.000', '2015-08-26T05:12:48.426', 'NaT', 'NaT', '1965-06-09T23:59:59.999'],100000), dtype="datetime64")
        second = np.array(np.tile(['2012-01-01T00:00:00', '2015-08-26T05:12:48', 'NaT', 'NaT', '1965-06-09T23:59:59'],100000), dtype="datetime64")
        nanotime = np.array(np.tile(['2012-01-01T00:00:00.000000000', '2015-08-26T05:12:48.008007006', 'NaT', 'NaT', '1965-06-09T23:59:59.999008007'],100000), dtype="datetime64")
        data = pd.DataFrame({'id':id,'timestamp1':date,'timestamp2':month,'timestamp3':time,'timestamp4':second,'timestamp5':nanotime})
        num = appender.append(data)
        self.assertEqual(num, n)
        script = '''
        n = 500000
        ids = 100000..599999
        time_name = `timestamp
        dates =funcByName(time_name)(take([2012.01.01, NULL,1965.07.25, NULL, 1970.01.01],n))
        months =  funcByName(time_name)(take([1965.08.01, NULL,2012.02.01, 2012.03.01, NULL],n))
        times = funcByName(time_name)(take([2012.01.01T00:00:00.000, 2015.08.26T05:12:48.426, NULL, NULL, 1965.06.09T23:59:59.999],n))
        seconds = funcByName(time_name)(take([2012.01.01T00:00:00, 2015.08.26T05:12:48, NULL, NULL, 1965.06.09T23:59:59],n))
        nanotimes = funcByName(time_name)(take([2012.01.01T00:00:00.000000000, 2015.08.26T05:12:48.008007006,NULL, NULL, 1965.06.09T23:59:59.999008007],n))
        ex = table(ids as id,dates as month1,months as month2,times as month3,seconds as month4,nanotimes as month5)
        re = select * from loadTable("dfs://test_PartitionedTableAppender_to_timestamp",`pt)
        each(eqObj, re.values(), ex.values())
        '''
        re = self.s.run(script)
        assert_array_equal(re, [True, True, True, True, True, True])

    def test_PartitionedTableAppender_dfs_table_to_nanotime(self):
        self.s.run('''
        dbPath = "dfs://test_PartitionedTableAppender_to_nanotime"
        if(existsDatabase(dbPath))
            dropDatabase(dbPath)
        t = table(100:100,`id`nanotime1`nanotime2`nanotime3`nanotime4`nanotime5,[INT,NANOTIME,NANOTIME,NANOTIME,NANOTIME,NANOTIME])
        db=database(dbPath,RANGE,[1,100001,200001,300001,400001,500001,600001])
        pt = db.createPartitionedTable(t, `pt, `id)
        ''')
        appender = ddb.PartitionedTableAppender("dfs://test_PartitionedTableAppender_to_nanotime","pt","id",self.pool)
        n = 500000
        id = np.arange(100000, 600000)
        date = np.array(np.tile(['2012-01-01', 'NaT', '1965-07-25', 'NaT', '1970-01-01'],100000), dtype="datetime64[D]")
        month = np.array(np.tile(['1965-08', 'NaT','2012-02', '2012-03', 'NaT'],100000), dtype="datetime64")
        time = np.array(np.tile(['2012-01-01T00:00:00.000', '2015-08-26T05:12:48.426', 'NaT', 'NaT', '1965-06-09T23:59:59.999'],100000), dtype="datetime64")
        second = np.array(np.tile(['2012-01-01T00:00:00', '2015-08-26T05:12:48', 'NaT', 'NaT', '1965-06-09T23:59:59'],100000), dtype="datetime64")
        nanotime = np.array(np.tile(['2012-01-01T00:00:00.000000000', '2015-08-26T05:12:48.008007006', 'NaT', 'NaT', '1965-06-09T23:59:59.999008007'],100000), dtype="datetime64")
        data = pd.DataFrame({'id':id,'nanotime1':date,'nanotime2':month,'nanotime3':time,'nanotime4':second,'nanotime5':nanotime})
        num = appender.append(data)
        self.assertEqual(num, n)
        script = '''
        n = 500000
        ids = 100000..599999
        time_name = `nanotime
        dates =funcByName(time_name)(take([2012.01.01T00:00:00, NULL,1965.07.25T00:00:00, NULL, 1970.01.01T00:00:00],n))
        months =  funcByName(time_name)(take([1965.08.01T00:00:00, NULL,2012.02.01T00:00:00, 2012.03.01T00:00:00, NULL],n))
        times = funcByName(time_name)(take([2012.01.01T00:00:00.000, 2015.08.26T05:12:48.426, NULL, NULL, 1965.06.09T23:59:59.999],n))
        seconds = funcByName(time_name)(take([2012.01.01T00:00:00, 2015.08.26T05:12:48, NULL, NULL, 1965.06.09T23:59:59],n))
        nanotimes = funcByName(time_name)(take([2012.01.01T00:00:00.000000000, 2015.08.26T05:12:48.008007006,NULL, NULL, 1965.06.09T23:59:59.999008007],n))
        ex = table(ids as id,dates as month1,months as month2,times as month3,seconds as month4,nanotimes as month5)
        re = select * from loadTable("dfs://test_PartitionedTableAppender_to_nanotime",`pt)
        each(eqObj, re.values(), ex.values())
        '''
        re = self.s.run(script)
        assert_array_equal(re, [True, True, True, True, True, True])

    def test_PartitionedTableAppender_dfs_table_to_nanotimestamp(self):
        self.s.run('''
        dbPath = "dfs://test_PartitionedTableAppender_to_nanotimestamp"
        if(existsDatabase(dbPath))
            dropDatabase(dbPath)
        t = table(100:100,`id`nanotimestamp1`nanotimestamp2`nanotimestamp3`nanotimestamp4`nanotimestamp5,[INT,NANOTIMESTAMP,NANOTIMESTAMP,NANOTIMESTAMP,NANOTIMESTAMP,NANOTIMESTAMP])
        db=database(dbPath,RANGE,[1,100001,200001,300001,400001,500001,600001])
        pt = db.createPartitionedTable(t, `pt, `id)
        ''')
        appender = ddb.PartitionedTableAppender("dfs://test_PartitionedTableAppender_to_nanotimestamp","pt","id",self.pool)
        n = 500000
        id = np.arange(100000, 600000)
        date = np.array(np.tile(['2012-01-01', 'NaT', '1965-07-25', 'NaT', '1970-01-01'],100000), dtype="datetime64[D]")
        month = np.array(np.tile(['1965-08', 'NaT','2012-02', '2012-03', 'NaT'],100000), dtype="datetime64")
        time = np.array(np.tile(['2012-01-01T00:00:00.000', '2015-08-26T05:12:48.426', 'NaT', 'NaT', '1965-06-09T23:59:59.999'],100000), dtype="datetime64")
        second = np.array(np.tile(['2012-01-01T00:00:00', '2015-08-26T05:12:48', 'NaT', 'NaT', '1965-06-09T23:59:59'],100000), dtype="datetime64")
        nanotime = np.array(np.tile(['2012-01-01T00:00:00.000000000', '2015-08-26T05:12:48.008007006', 'NaT', 'NaT', '1965-06-09T23:59:59.999008007'],100000), dtype="datetime64")
        data = pd.DataFrame({'id':id,'nanotimestamp1':date,'nanotimestamp2':month,'nanotimestamp3':time,'nanotimestamp4':second,'nanotimestamp5':nanotime})
        num = appender.append(data)
        self.assertEqual(num, n)
        script = '''
        n = 500000
        ids = 100000..599999
        time_name = `nanotimestamp
        dates =funcByName(time_name)(take([2012.01.01T00:00:00, NULL,1965.07.25T00:00:00, NULL, 1970.01.01T00:00:00],n))
        months =  funcByName(time_name)(take([1965.08.01T00:00:00, NULL,2012.02.01T00:00:00, 2012.03.01T00:00:00, NULL],n))
        times = funcByName(time_name)(take([2012.01.01T00:00:00.000, 2015.08.26T05:12:48.426, NULL, NULL, 1965.06.09T23:59:59.999],n))
        seconds = funcByName(time_name)(take([2012.01.01T00:00:00, 2015.08.26T05:12:48, NULL, NULL, 1965.06.09T23:59:59],n))
        nanotimes = funcByName(time_name)(take([2012.01.01T00:00:00.000000000, 2015.08.26T05:12:48.008007006,NULL, NULL, 1965.06.09T23:59:59.999008007],n))
        ex = table(ids as id,dates as month1,months as month2,times as month3,seconds as month4,nanotimes as month5)
        re = select * from loadTable("dfs://test_PartitionedTableAppender_to_nanotimestamp",`pt)
        each(eqObj, re.values(), ex.values())
        '''
        re = self.s.run(script)
        assert_array_equal(re, [True, True, True, True, True, True])    


    def test_PartitionedTableAppender_dfs_table_to_datehour(self):
        self.s.run('''
        dbPath = "dfs://test_PartitionedTableAppender_to_datehour"
        if(existsDatabase(dbPath))
            dropDatabase(dbPath)
        t = table(100:100,`id`datehour1`datehour2`datehour3`datehour4`datehour5,[INT,DATEHOUR,DATEHOUR,DATEHOUR,DATEHOUR,DATEHOUR])
        db=database(dbPath,RANGE,[1,100001,200001,300001,400001,500001,600001])
        pt = db.createPartitionedTable(t, `pt, `id)
        ''')
        appender = ddb.PartitionedTableAppender("dfs://test_PartitionedTableAppender_to_datehour","pt","id",self.pool)
        n = 500000
        id = np.arange(100000, 600000)
        date = np.array(np.tile(['2012-01-01', 'NaT', '1965-07-25', 'NaT', '1970-01-01'],100000), dtype="datetime64[D]")
        month = np.array(np.tile(['1965-08', 'NaT','2012-02', '2012-03', 'NaT'],100000), dtype="datetime64")
        time = np.array(np.tile(['2012-01-01T00:00:00.000', '2015-08-26T05:12:48.426', 'NaT', 'NaT', '1965-06-09T23:59:59.999'],100000), dtype="datetime64")
        second = np.array(np.tile(['2012-01-01T00:00:00', '2015-08-26T05:12:48', 'NaT', 'NaT', '1965-06-09T23:59:59'],100000), dtype="datetime64")
        nanotime = np.array(np.tile(['2012-01-01T00:00:00.000000000', '2015-08-26T05:12:48.008007006', 'NaT', 'NaT', '1965-06-09T23:59:59.999008007'],100000), dtype="datetime64")
        data = pd.DataFrame({'id':id,'datehour1':date,'datehour2':month,'datehour3':time,'datehour4':second,'datehour5':nanotime})
        num = appender.append(data)
        self.assertEqual(num, n)
        script = '''
        n = 500000
        ids = 100000..599999
        time_name = `datehour
        dates =funcByName(time_name)(take([2012.01.01T00:00:00, NULL,1965.07.25T00:00:00, NULL, 1970.01.01T00:00:00],n))
        months =  funcByName(time_name)(take([1965.08.01T00:00:00, NULL,2012.02.01T00:00:00, 2012.03.01T00:00:00, NULL],n))
        times = funcByName(time_name)(take([2012.01.01T00:00:00.000, 2015.08.26T05:12:48.426, NULL, NULL, 1965.06.09T23:59:59.999],n))
        seconds = funcByName(time_name)(take([2012.01.01T00:00:00, 2015.08.26T05:12:48, NULL, NULL, 1965.06.09T23:59:59],n))
        nanotimes = funcByName(time_name)(take([2012.01.01T00:00:00.000000000, 2015.08.26T05:12:48.008007006,NULL, NULL, 1965.06.09T23:59:59.999008007],n))
        ex = table(ids as id,dates as month1,months as month2,times as month3,seconds as month4,nanotimes as month5)
        re = select * from loadTable("dfs://test_PartitionedTableAppender_to_datehour",`pt)
        each(eqObj, re.values(), ex.values())
        '''
        re = self.s.run(script)
        assert_array_equal(re, [True, True, True, True, True, True])


    def test_PartitionedTableAppender_dfs_table_int_to_date(self):
        self.s.run('''
        dbPath = "dfs://test_PartitionedTableAppender_int_to_date"
        if(existsDatabase(dbPath))
            dropDatabase(dbPath)
        t = table(100:100,`id`date,[INT,DATE])
        db=database(dbPath,RANGE,[1,100001,200001,300001,400001,500001,600001])
        pt = db.createPartitionedTable(t, `pt, `id)
        ''')
        appender = ddb.PartitionedTableAppender("dfs://test_PartitionedTableAppender_int_to_date","pt","id",self.pool)
        n = 500000
        id = np.arange(100000, 600000)
        ints = np.arange(100000, 600000)
        data = pd.DataFrame({'id':id,'date':ints})
        with self.assertRaises(RuntimeError):
            appender.append(data)
 

    def test_PartitionedTableAppender_dfs_table_to_date_partition_col_date(self):
        self.s.run('''
        dbPath = "dfs://test_PartitionedTableAppender"
        if(existsDatabase(dbPath))
            dropDatabase(dbPath)
        t = table(100:100,`id`date,[INT,DATE])
        db=database(dbPath,VALUE,2010.01.01+0..365)
        pt = db.createPartitionedTable(t, `pt, `date)
        ''')
        appender = ddb.PartitionedTableAppender("dfs://test_PartitionedTableAppender","pt","date",self.pool)
        
        id = np.arange(100000, 600000)
        time1 = np.array(np.tile(['2010-01-01T00:00:00.000', '2010-02-01T05:12:48.426', 'NaT', 'NaT', '2010-03-03T23:59:59.999'],100000), dtype="datetime64")
        data = pd.DataFrame({'id':id,'date':time1})
        n1 = appender.append(data)
        # null don`t insert into table
        num = 300000
        self.assertEqual(num, n1)
        script = '''
        n = 500000
        ids = 100000..599999
        time_name = `date
        times = funcByName(time_name)(take([2010.01.01T00:00:00.000, 2010.02.01T05:12:48.426, NULL, NULL, 2010.03.03T23:59:59.999],n))
        t = table(ids as id,times as date)
        ex = select * from t where date!=NULL
        re = select * from loadTable("dfs://test_PartitionedTableAppender",`pt) order by id
        each(eqObj, re.values(), ex.values())
        '''
        re = self.s.run(script)
        assert_array_equal(re, [True, True])

        id = np.arange(100000, 600000)
        month = np.array(np.tile(['2010-04', '2010-05', 'NaT', 'NaT', '2010-06'],100000), dtype="datetime64")
        data = pd.DataFrame({'id':id,'date':month})
        n2 = appender.append(data)
        # null don`t insert into table
        time.sleep(5)
        num = 600000
        self.assertEqual(num, n1+n2)
        script = '''
        n = 500000
        ids = 100000..599999
        time_name = `date
        times = funcByName(time_name)(take([2010.04M, 2010.05M, NULL, NULL, 2010.06M],n))
        t1 = table(ids as id,times as date)
        t.append!(t1)
        ex = select * from t where date!=NULL order by id
        re = select * from loadTable("dfs://test_PartitionedTableAppender",`pt) order by id
        each(eqObj, re.values(), ex.values())
        '''
        re = self.s.run(script)
        assert_array_equal(re, [True, True])

        id = np.arange(100000, 600000)
        second = np.array(np.tile(['2010-07-01T00:00:00', '2010-08-26T05:12:48', 'NaT', 'NaT', '2010-09-01T23:59:59'],100000), dtype="datetime64")        
        data = pd.DataFrame({'id':id,'date':second})
        n3 = appender.append(data)
        time.sleep(5)
        # null don`t insert into table
        num = 900000
        self.assertEqual(num, n1+n2+n3)
        script = '''
        n = 500000
        ids = 100000..599999
        time_name = `date
        times = funcByName(time_name)(take([2010.07.01T00:00:00, 2010.08.26T05:12:48, NULL, NULL, 2010.09.01T23:59:59],n))
        t1 = table(ids as id,times as date)
        t.append!(t1)
        ex = select * from t where date!=NULL order by id
        re = select * from loadTable("dfs://test_PartitionedTableAppender",`pt) order by id
        each(eqObj, re.values(), ex.values())
        '''
        re = self.s.run(script)
        assert_array_equal(re, [True, True])

        id = np.arange(100000, 600000)
        nanotime = np.array(np.tile(['2010-07-01T00:00:00.000000000', '2010-08-26T05:12:48.008007006', 'NaT', 'NaT', '2010-09-01T23:59:59.999008007'],100000), dtype="datetime64")        
        data = pd.DataFrame({'id':id,'date':nanotime})
        n4 = appender.append(data)
        time.sleep(5)
        # null don`t insert into table
        num = 1200000
        self.assertEqual(num, n1+n2+n3+n4)
        script = '''
        n = 500000
        ids = 100000..599999
        time_name = `date
        times = funcByName(time_name)(take([2010.07.01T00:00:00.000000000, 2010.08.26T05:12:48.000000000, NULL, NULL, 2010.09.01T23:59:59.999008007],n))
        t1 = table(ids as id,times as date)
        t.append!(t1)
        ex = select * from t where date!=NULL order by id
        re = select * from loadTable("dfs://test_PartitionedTableAppender",`pt) order by id
        each(eqObj, re.values(), ex.values())
        '''
        re = self.s.run(script)
        assert_array_equal(re, [True, True])

    def test_PartitionedTableAppender_dfs_table_insert_one_row(self):
        script='''
        dbPath = "dfs://Rangedb"
                if(existsDatabase(dbPath))
                    dropDatabase(dbPath)
                t = table(100:100,`id`val1`val2,[INT,DOUBLE,DATE])
                db=database(dbPath,RANGE,  1  100  200  300)
                pt = db.createPartitionedTable(t, `pt, `id)
        '''
        self.s.run(script)
        #self.s.close()

        appender = ddb.PartitionedTableAppender("dfs://Rangedb","pt", "id", self.pool)
        v = np.array('2012-01-01T00:00:00.000', dtype="datetime64")
        data = pd.DataFrame({"id":np.random.randint(1,300,1),"val1":np.random.rand(1),"val2":v})
        re = appender.append(data)
        print(re)

if __name__ == '__main__':
    unittest.main()

