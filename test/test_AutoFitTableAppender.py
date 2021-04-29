
import unittest
import dolphindb as ddb
import numpy as np
import pandas as pd
from setup import HOST, PORT, WORK_DIR, DATA_DIR
from numpy.testing import assert_array_equal, assert_array_almost_equal
from pandas.testing import assert_series_equal
from pandas.testing import assert_frame_equal

class TestAutoFitTableAppender(unittest.TestCase):
    @classmethod
    def setUp(cls):
        cls.s = ddb.session()
        cls.s.connect(HOST, PORT, "admin", "123456")


    @classmethod
    def tearDownClass(cls):
        pass

    def test_AutoFitTableAppender_in_memory_table_date(self):
        self.s.run("share table(1000:0, `sym`date`qty, [SYMBOL, DATE, INT]) as t")
        appender = ddb.tableAppender(tableName="t", ddbSession=self.s)
        sym = np.repeat(['AAPL', 'GOOG', 'MSFT', 'IBM', 'YHOO'], 2, axis=0)
        date = np.array(['2012-01-01', 'NaT', '1965-07-25', 'NaT', '2020-12-23', '1970-01-01', 'NaT', 'NaT', 'NaT', '2009-08-05'], dtype="datetime64[D]")
        qty = np.arange(1, 11)
        data = pd.DataFrame({'sym': sym, 'date': date, 'qty': qty})
        num = appender.append(data)
        self.assertEqual(num, 10)
        script='''
        tmp=table(`AAPL`AAPL`GOOG`GOOG`MSFT`MSFT`IBM`IBM`YHOO`YHOO as sym, [2012.01.01, NULL, 1965.07.25, NULL, 2020.12.23, 1970.01.01, NULL, NULL, NULL, 2009.08.05] as date, 1..10 as qty)
        each(eqObj, tmp.values(), t.values())
        '''
        re = self.s.run(script)
        assert_array_equal(re, [True, True, True])
        re = self.s.run("t")
        self.s.run("undef(`t, SHARED)")
    
    def test_AutoFitTableAppender_in_memory_table_month(self):
        self.s.run("share table(1000:0, `sym`month`qty, [SYMBOL, MONTH, INT]) as t")
        appender = ddb.tableAppender(tableName="t", ddbSession=self.s)
        sym = ['A1', 'A2', 'A3', 'A4', 'A5']
        month = np.array(['1965-08', 'NaT','2012-02', '2012-03', 'NaT'], dtype="datetime64")
        qty = np.arange(1, 6)
        data = pd.DataFrame({'sym': sym, 'month': month, 'qty': qty})
        num = appender.append(data)
        self.assertEqual(num, 5)
        script = '''
        tmp=table(`A1`A2`A3`A4`A5 as sym, [1965.08M, NULL, 2012.02M, 2012.03M, NULL] as month, 1..5 as qty)
        each(eqObj, tmp.values(), t.values())
        '''
        re = self.s.run(script)
        assert_array_equal(re, [True, True, True])
        re = self.s.run("t")
        self.s.run("undef(`t, SHARED)")
        
    def test_AutoFitTableAppender_in_memory_table_time(self):
        self.s.run("share table(1000:0, `sym`time`qty, [SYMBOL, TIME, INT]) as t")
        appender = ddb.tableAppender(tableName="t", ddbSession=self.s)
        sym = ['A1', 'A2', 'A3', 'A4', 'A5']
        time = np.array(['2012-01-01T00:00:00.000', '2015-08-26T05:12:48.426', 'NaT', 'NaT', '2015-06-09T23:59:59.999'], dtype="datetime64")
        qty = np.arange(1, 6)
        data = pd.DataFrame({'sym': sym, 'time': time, 'qty': qty})
        num = appender.append(data)
        script='''
        tmp=table(`A1`A2`A3`A4`A5 as sym, [00:00:00.000, 05:12:48.426, NULL, NULL, 23:59:59.999] as time, 1..5 as qty)
        each(eqObj, tmp.values(), t.values())
        '''
        re = self.s.run(script)
        assert_array_equal(re, [True, True, True])
        re = self.s.run("t")
        self.s.run("undef(`t, SHARED)")

    def test_AutoFitTableAppender_in_memory_table_minute(self):
        self.s.run("share table(1000:0, `sym`time`qty, [SYMBOL, MINUTE, INT]) as t")
        appender = ddb.tableAppender(tableName="t", ddbSession=self.s)
        sym = ['A1', 'A2', 'A3', 'A4', 'A5']
        time = np.array(['2012-01-01T00:00:00.000', '2015-08-26T05:12:48.426', 'NaT', 'NaT', '2015-06-09T23:59:59.999'], dtype="datetime64")
        qty = np.arange(1, 6)
        data = pd.DataFrame({'sym': sym, 'time': time, 'qty': qty})
        num = appender.append(data)
        script='''
        tmp=table(`A1`A2`A3`A4`A5 as sym, [00:00m, 05:12m, NULL, NULL, 23:59m] as time, 1..5 as qty)
        each(eqObj, tmp.values(), t.values())
        '''
        re = self.s.run(script)
        assert_array_equal(re, [True, True, True])
        re = self.s.run("t")
        self.s.run("undef(`t, SHARED)")
    
    def test_AutoFitTableAppender_in_memory_table_second(self):
        self.s.run("share table(1000:0, `sym`time`qty, [SYMBOL, SECOND, INT]) as t")
        appender = ddb.tableAppender(tableName="t", ddbSession=self.s)
        sym = ['A1', 'A2', 'A3', 'A4', 'A5']
        time = np.array(['2012-01-01T00:00:00', '2015-08-26T05:12:48', 'NaT', 'NaT', '2015-06-09T23:59:59'], dtype="datetime64")
        qty = np.arange(1, 6)
        data = pd.DataFrame({'sym': sym, 'time': time, 'qty': qty})
        num = appender.append(data)
        script='''
        tmp=table(`A1`A2`A3`A4`A5 as sym, [00:00:00, 05:12:48, NULL, NULL, 23:59:59] as time, 1..5 as qty)
        each(eqObj, tmp.values(), t.values())
        '''
        re = self.s.run(script)
        assert_array_equal(re, [True, True, True])
        re = self.s.run("t")
        self.s.run("undef(`t, SHARED)")

    def test_AutoFitTableAppender_in_memory_table_datetime(self):
        self.s.run("share table(1000:0, `sym`time`qty, [SYMBOL, DATETIME, INT]) as t")
        appender = ddb.tableAppender(tableName="t", ddbSession=self.s)
        sym = ['A1', 'A2', 'A3', 'A4', 'A5']
        time = np.array(['2012-01-01T00:00:00', '2015-08-26T05:12:48', 'NaT', 'NaT', '2015-06-09T23:59:59'], dtype="datetime64")
        qty = np.arange(1, 6)
        data = pd.DataFrame({'sym': sym, 'time': time, 'qty': qty})
        num = appender.append(data)
        script='''
        tmp=table(`A1`A2`A3`A4`A5 as sym, [2012.01.01T00:00:00, 2015.08.26T05:12:48, NULL, NULL, 2015.06.09T23:59:59] as time, 1..5 as qty)
        each(eqObj, tmp.values(), t.values())
        '''
        re = self.s.run(script)
        assert_array_equal(re, [True, True, True])
        re = self.s.run("t")
        self.s.run("undef(`t, SHARED)")
    
    def test_AutoFitTableAppender_in_memory_table_timestamp(self):
        self.s.run("share table(1000:0, `sym`time`qty, [SYMBOL, TIMESTAMP, INT]) as t")
        appender = ddb.tableAppender(tableName="t", ddbSession=self.s)
        sym = ['A1', 'A2', 'A3', 'A4', 'A5']
        time = np.array(['2012-01-01T00:00:00.000', '2015-08-26T05:12:48.008', 'NaT', 'NaT', '2015-06-09T23:59:59.999'], dtype="datetime64")
        qty = np.arange(1, 6)
        data = pd.DataFrame({'sym': sym, 'time': time, 'qty': qty})
        num = appender.append(data)
        script='''
        tmp=table(`A1`A2`A3`A4`A5 as sym, [2012.01.01T00:00:00.000, 2015.08.26T05:12:48.008, NULL, NULL, 2015.06.09T23:59:59.999] as time, 1..5 as qty)
        each(eqObj, tmp.values(), t.values())
        '''
        re = self.s.run(script)
        assert_array_equal(re, [True, True, True])
        re = self.s.run("t")
        self.s.run("undef(`t, SHARED)")
    
    def test_AutoFitTableAppender_in_memory_table_nanotime(self):
        self.s.run("share table(1000:0, `sym`time`qty, [SYMBOL, NANOTIME, INT]) as t")
        appender = ddb.tableAppender(tableName="t", ddbSession=self.s)
        sym = ['A1', 'A2', 'A3', 'A4', 'A5']
        time = np.array(['2012-01-01T00:00:00.000000000', '2015-08-26T05:12:48.008007006', 'NaT', 'NaT', '2015-06-09T23:59:59.999008007'], dtype="datetime64")
        qty = np.arange(1, 6)
        data = pd.DataFrame({'sym': sym, 'time': time, 'qty': qty})
        num = appender.append(data)
        script='''
        tmp=table(`A1`A2`A3`A4`A5 as sym, [00:00:00.000000000, 05:12:48.008007006, NULL, NULL, 23:59:59.999008007] as time, 1..5 as qty)
        each(eqObj, tmp.values(), t.values())
        '''
        re = self.s.run(script)
        assert_array_equal(re, [True, True, True])
        re = self.s.run("t")
        self.s.run("undef(`t, SHARED)")
    
    def test_AutoFitTableAppender_in_memory_table_nanotimestamp(self):
        self.s.run("share table(1000:0, `sym`time`qty, [SYMBOL, NANOTIMESTAMP, INT]) as t")
        appender = ddb.tableAppender(tableName="t", ddbSession=self.s)
        sym = ['A1', 'A2', 'A3', 'A4', 'A5']
        time = np.array(['2012-01-01T00:00:00.000000000', '2015-08-26T05:12:48.008007006', 'NaT', 'NaT', '2015-06-09T23:59:59.999008007'], dtype="datetime64")
        qty = np.arange(1, 6)
        data = pd.DataFrame({'sym': sym, 'time': time, 'qty': qty})
        num = appender.append(data)
        script='''
        tmp=table(`A1`A2`A3`A4`A5 as sym, [2012.01.01T00:00:00.000000000, 2015.08.26T05:12:48.008007006, NULL, NULL, 2015.06.09T23:59:59.999008007] as time, 1..5 as qty)
        each(eqObj, tmp.values(), t.values())
        '''
        re = self.s.run(script)
        assert_array_equal(re, [True, True, True])
        re = self.s.run("t")
        self.s.run("undef(`t, SHARED)")

    def test_AutoFitTableAppender_in_memory_table_date_null(self):
        self.s.run("share table(1000:0, `sym`date`qty, [SYMBOL, DATE, INT]) as t")
        appender = ddb.tableAppender(tableName="t", ddbSession=self.s)
        sym = np.repeat(['AAPL', 'GOOG', 'MSFT', 'IBM', 'YHOO'], 2, axis=0)
        date = np.array(np.repeat('Nat',10), dtype="datetime64[D]")
        qty = np.arange(1, 11)
        data = pd.DataFrame({'sym': sym, 'date': date, 'qty': qty})
        num = appender.append(data)
        self.assertEqual(num, 10)
        script='''
        tmp=table(`AAPL`AAPL`GOOG`GOOG`MSFT`MSFT`IBM`IBM`YHOO`YHOO as sym, take(date(),10) as date, 1..10 as qty)
        each(eqObj, tmp.values(), t.values())
        '''
        re = self.s.run(script)
        assert_array_equal(re, [True, True, True])
        re = self.s.run("t")
        self.s.run("undef(`t, SHARED)")

    def test_AutoFitTableAppender_in_memory_table_nanotimestamp_null(self):
        self.s.run("share table(1000:0, `sym`time`qty, [SYMBOL, NANOTIMESTAMP, INT]) as t")
        appender = ddb.tableAppender(tableName="t", ddbSession=self.s)
        sym = ['A1', 'A2', 'A3', 'A4', 'A5']
        time = np.array(np.repeat('Nat',5), dtype="datetime64")
        qty = np.arange(1, 6)
        data = pd.DataFrame({'sym': sym, 'time': time, 'qty': qty})
        num = appender.append(data)
        script='''
        tmp=table(`A1`A2`A3`A4`A5 as sym, take(nanotimestamp(),5) as time, 1..5 as qty)
        each(eqObj, tmp.values(), t.values())
        '''
        re = self.s.run(script)
        assert_array_equal(re, [True, True, True])
        re = self.s.run("t")
        self.s.run("undef(`t, SHARED)")

    def test_AutoFitTableAppender_in_memory_table_all_time_type(self):
        self.s.run("share table(1000:0, `sym`date`month`time`minute`second`datetime`timestamp`nanotime`nanotimestamp`qty, [SYMBOL, DATE,MONTH,TIME,MINUTE,SECOND,DATETIME,TIMESTAMP,NANOTIME,NANOTIMESTAMP, INT]) as t")
        appender = ddb.tableAppender(tableName="t", ddbSession=self.s)
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
        each(eqObj, tmp.values(), t.values())
        '''
        re = self.s.run(script)
        assert_array_equal(re, [True, True, True,True, True, True,True, True, True,True, True])
        self.s.run("undef(`t, SHARED)")

    def test_AutoFitTableAppender_dfs_table_all_time_types(self):
        self.s.run('''
        dbPath = "dfs://AutoFitTableAppender_test"
        if(existsDatabase(dbPath))
            dropDatabase(dbPath)
        t = table(1000:0, `sym`date`month`time`minute`second`datetime`timestamp`nanotime`nanotimestamp`qty, [SYMBOL, DATE,MONTH,TIME,MINUTE,SECOND,DATETIME,TIMESTAMP,NANOTIME,NANOTIMESTAMP, INT])
        db=database(dbPath,RANGE,100000 200000 300000 400000 600001)
        pt = db.createPartitionedTable(t, `pt, `qty)
        ''')
        appender = ddb.tableAppender("dfs://AutoFitTableAppender_test","pt", self.s)
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
        re = select * from loadTable("dfs://AutoFitTableAppender_test",`pt)
        each(eqObj, tmp.values(), re.values())
        '''
        re = self.s.run(script)
        assert_array_equal(re, [True, True, True,True, True, True,True, True, True,True, True])

    def test_AutoFitTableAppender_in_memory_table_all_time_type_early_1970(self):
        self.s.run("share table(1000:0, `date`month`datetime `timestamp`nanotimestamp`qty, [DATE,MONTH,DATETIME,TIMESTAMP,NANOTIMESTAMP, INT]) as t")
        appender = ddb.tableAppender(tableName="t", ddbSession=self.s)
        n = 500000
        date = np.array(np.repeat('1960-01-01',n),dtype="datetime64[D]")
        month = np.array(np.repeat('1960-01',n),dtype="datetime64")
        datetime =  np.array(np.repeat('1960-01-01T13:30:10',n),dtype="datetime64")
        timestamp = np.array(np.repeat('1960-01-01T13:30:10.008',n),dtype="datetime64")
        nanotimestamp =  np.array(np.repeat('1960-01-01 13:30:10.008007006',n),dtype="datetime64")
        qty = np.arange(100000, 600000)
        data = pd.DataFrame({'date': date, 'month':month,  'datetime':datetime, 'timestamp':timestamp, 'nanotimestamp':nanotimestamp, 'qty': qty})
        num = appender.append(data)
        self.assertEqual(num, n)
        script='''
        n = 500000
        tmp=table(take(1960.01.01,n) as date,take(1960.01M,n) as month,take(1960.01.01T13:30:10,n) as datetime,
        take(1960.01.01T13:30:10.008,n) as timestamp,take(1960.01.01 13:30:10.008007006,n) as nanotimestamp,
        100000..599999 as qty)
        each(eqObj, tmp.values(), t.values())
        '''
        re = self.s.run(script)
        assert_array_equal(re, [True, True, True,True, True,True])
        self.s.run("undef(`t, SHARED)")

    def test_AutoFitTableAppender_dfs_table_all_time_types_early_1970(self):
        self.s.run('''
        dbPath = "dfs://AutoFitTableAppender_test"
        if(existsDatabase(dbPath))
            dropDatabase(dbPath)
        t = table(1000:0, `date`month`datetime`timestamp`nanotimestamp`qty, [DATE,MONTH,DATETIME,TIMESTAMP,NANOTIMESTAMP, INT])
        db=database(dbPath,RANGE,100000 200000 300000 400000 600001)
        pt = db.createPartitionedTable(t, `pt, `qty)
        ''')
        appender = ddb.tableAppender("dfs://AutoFitTableAppender_test","pt", self.s)
        n = 500000
        date = np.array(np.repeat('1960-01-01',n),dtype="datetime64[D]")
        month = np.array(np.repeat('1960-01',n),dtype="datetime64")
        datetime =  np.array(np.repeat('1960-01-01T13:30:10',n),dtype="datetime64")
        timestamp = np.array(np.repeat('1960-01-01T13:30:10.008',n),dtype="datetime64")
        nanotimestamp =  np.array(np.repeat('1960-01-01 13:30:10.008007006',n),dtype="datetime64")
        qty = np.arange(100000, 600000)
        data = pd.DataFrame({'date': date, 'month':month,  'datetime':datetime, 'timestamp':timestamp, 'nanotimestamp':nanotimestamp, 'qty': qty})
        num = appender.append(data)
        self.assertEqual(num, n)
        script='''
        n = 500000
        ex = table(take(1960.01.01,n) as date,take(1960.01M,n) as month,take(1960.01.01T13:30:10,n) as datetime,
        take(1960.01.01T13:30:10.008,n) as timestamp,take(1960.01.01 13:30:10.008007006,n) as nanotimestamp,
        100000..599999 as qty)
        re = select * from loadTable("dfs://AutoFitTableAppender_test",`pt)
        each(eqObj, re.values(), ex.values())
        '''
        re = self.s.run(script)
        assert_array_equal(re, [True, True, True,True, True,True])

    def test_AutoFitTableAppender_in_memory_table_datehour(self):
        self.s.run("try{undef(`t);undef(`t, SHARED)}catch(ex){};share table('A1' 'A2' as sym,datehour([2021.01.01T01:01:01,2021.01.01T02:01:01])  as time,1 2 as qty) as t")
        appender = ddb.tableAppender(tableName="t", ddbSession=self.s)
        sym = ['A3', 'A4', 'A5', 'A6', 'A7']
        time = np.array(['2021-01-01T03', '2021-01-01T04', 'NaT', 'NaT', '1960-01-01T03'], dtype="datetime64")
        qty = np.arange(3, 8)
        data = pd.DataFrame({'sym': sym, 'time': time, 'qty': qty})
        num = appender.append(data)
        script='''
        tmp=table("A"+string(1..7) as sym, datehour([2021.01.01T01:01:01,2021.01.01T02:01:01,2021.01.01T03:01:01,2021.01.01T04:01:01,NULL,NULL,1960.01.01T03:01:01])  as time, 1..7 as qty)
        each(eqObj, tmp.values(), t.values())
        '''
        re = self.s.run(script)
        assert_array_equal(re, [True, True, True])

    def test_AutoFitTableAppender_in_memory_table_datehour_null(self):
        self.s.run("t = table(datehour([2021.01.01T01:01:01,2021.01.01T02:01:01])  as time,1 2 as qty)")
        appender = ddb.tableAppender(tableName="t", ddbSession=self.s)
        n = 100000
        time = np.array(np.repeat('Nat',n), dtype="datetime64")
        qty = np.arange(3, n+3)
        data = pd.DataFrame({'time': time, 'qty': qty})
        num = appender.append(data)
        script='''
        n = 100000
        tmp=table(datehour([2021.01.01T01:01:01,2021.01.01T02:01:01].join(take(datehour(),n)))  as time, 1..(n+2) as qty)
        each(eqObj, tmp.values(), t.values())
        '''
        re = self.s.run(script)
        assert_array_equal(re, [True, True])

    def test_AutoFitTableAppender_dfs_table_datehour(self):
        self.s.run('''
        dbPath = "dfs://AutoFitTableAppender_test"
        if(existsDatabase(dbPath))
            dropDatabase(dbPath)
        t = table(datehour(2020.01.01T01:01:01) as time, 1 as qty)
        db=database(dbPath,RANGE,0 100000 200000 300000 400000 600001)
        pt = db.createPartitionedTable(t, `pt, `qty)
        ''')
        appender = ddb.tableAppender("dfs://AutoFitTableAppender_test","pt", self.s)
        n = 500000
        time = pd.date_range(start='2020-01-01T01',periods=n,freq='h')
        qty = np.arange(1,n+1)
        data = pd.DataFrame({'time':time,'qty':qty})
        num = appender.append(data)
        self.assertEqual(num, n)
        script = '''
        n = 500000
        ex = table((datehour(2020.01.01T00:01:01)+1..n) as time,1..n as qty)
        re = select * from loadTable("dfs://AutoFitTableAppender_test",`pt)
        each(eqObj, re.values(), ex.values())
        '''
        re = self.s.run(script)
        assert_array_equal(re, [True, True])



if __name__ == '__main__':
    unittest.main()