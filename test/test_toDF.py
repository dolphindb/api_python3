import unittest
import dolphindb as ddb
import numpy as np
import pandas as pd
from numpy.testing import assert_array_equal, assert_array_almost_equal
from pandas.testing import assert_frame_equal, assert_series_equal
from setup import HOST, PORT, WORK_DIR, DATA_DIR

class TestToDF(unittest.TestCase):
    @classmethod
    def setUp(cls):
        cls.s = ddb.session()
        cls.s.connect(HOST, PORT, "admin", "123456")

    @classmethod
    def tearDownClass(cls):
        pass

    def test_python_table_toDF(self):
        df = pd.DataFrame({'cid': np.array([1, 2, 3], dtype=np.int32),
            'cbool': np.array([True, False, None], dtype=np.bool),
            'cchar': np.array([1, 2, 3], dtype=np.int8),
            'cshort': np.array([1, 2, 3], dtype=np.int16),
            'cint': np.array([1, 2, 3], dtype=np.int32),
            'clong': np.array([0, 1, 2], dtype=np.int64),
            'cdate': np.array(['2019-02-04', '2019-02-05', ''], dtype='datetime64[D]'),
            'cmonth': np.array(['2019-01', '2019-02', ''], dtype='datetime64[M]'),
            'ctime': np.array(['2019-01-01 15:00:00.706', '2019-01-01 15:30:00.706', ''], dtype='datetime64[ms]'),
            'cminute': np.array(['2019-01-01 15:25', '2019-01-01 15:30', ''], dtype='datetime64[m]'),
            'csecond': np.array(['2019-01-01 15:00:30', '2019-01-01 15:30:33', ''], dtype='datetime64[s]'),
            'cdatetime': np.array(['2019-01-01 15:00:30', '2019-01-02 15:30:33', ''], dtype='datetime64[s]'),
            'ctimestamp': np.array(['2019-01-01 15:00:00.706', '2019-01-01 15:30:00.706', ''], dtype='datetime64[ms]'),
            'cnanotime': np.array(['2019-01-01 15:00:00.80706', '2019-01-01 15:30:00.80706', ''], dtype='datetime64[ns]'),
            'cnanotimestamp': np.array(['2019-01-01 15:00:00.80706', '2019-01-01 15:30:00.80706', ''], dtype='datetime64[ns]'),
            'cfloat': np.array([2.1, 2.658956, np.NaN], dtype=np.float32),
            'cdouble': np.array([0., 47.456213, np.NaN], dtype=np.float64),
            'csymbol': np.array(['A', 'B', '']),
            'cstring': np.array(['abc', 'def', ''])})
        tmp = self.s.table(data=df, tableAliasName="t")
        res = tmp.toDF()
        assert_array_equal(res["cid"], [1, 2, 3])
        assert_array_equal(res["cbool"], [True, False, False])
        assert_array_equal(res["cchar"], [1, 2, 3])
        assert_array_equal(res["cshort"], [1, 2, 3])
        assert_array_equal(res["cint"], [1, 2, 3])
        assert_array_equal(res["clong"], [0, 1, 2])
        assert_array_equal(res["cdate"], np.array(['2019-02-04', '2019-02-05', ''], dtype="datetime64"))
        assert_array_equal(res["cmonth"], np.array(['2019-01-01', '2019-02-01', ''], dtype="datetime64"))
        assert_array_equal(res["ctime"], np.array(['2019-01-01 15:00:00.706', '2019-01-01 15:30:00.706', ''], dtype="datetime64"))
        assert_array_equal(res["cminute"], np.array(['2019-01-01 15:25:00', '2019-01-01 15:30:00', ''], dtype="datetime64"))
        assert_array_equal(res["csecond"], np.array(['2019-01-01 15:00:30', '2019-01-01 15:30:33', ''], dtype='datetime64'))
        assert_array_equal(res["cdatetime"], np.array(['2019-01-01 15:00:30', '2019-01-02 15:30:33', ''], dtype="datetime64"))
        assert_array_equal(res["ctimestamp"], np.array(['2019-01-01 15:00:00.706', '2019-01-01 15:30:00.706', ''], dtype="datetime64"))
        assert_array_equal(res["cnanotime"], np.array(['2019-01-01 15:00:00.807060', '2019-01-01 15:30:00.807060', ''], dtype="datetime64"))
        assert_array_equal(res["cnanotimestamp"], np.array(['2019-01-01 15:00:00.807060', '2019-01-01 15:30:00.807060', ''], dtype="datetime64"))
        assert_array_almost_equal(res["cfloat"], np.array([2.1, 2.658956, np.NaN]))
        assert_array_almost_equal(res["cdouble"], np.array([0., 47.456213, np.NaN]))
        assert_array_equal(res["csymbol"], np.array(['A', 'B', '']))
        assert_array_equal(res["cstring"], np.array(['abc', 'def', '']))

    def test_run_table(self):
        script='''
        t=table(100:0, `cbool`cchar`cshort`cint`clong`cdate`cmonth`ctime`cminute`csecond`cdatetime`ctimestamp`cnanotime`cnanotimestamp`cfloat`cdouble`csymbol`cstring`cuuid`cipaddr`cint128, [BOOL,CHAR,SHORT,INT,LONG,DATE,MONTH,TIME,MINUTE,SECOND,DATETIME,TIMESTAMP,NANOTIME,NANOTIMESTAMP,FLOAT,DOUBLE,SYMBOL,STRING,UUID,IPADDR,INT128])

        insert into t values(true, 'a', -5h, -5, -5l, 1960.06.01, 1960.06M, 00:00:00.000, 00:00m, 00:00:00, 1960.06.01T00:00:00, 1960.06.01T00:00:00.000, 00:00:00.000000000, 1960.06.01T00:00:00.000000000, -0.5f, 0.5, "hello", "world", uuid("5d212a78-cc48-e3b1-4235-b4d91473ee87"), ipaddr("192.168.1.13"), int128("e1671797c52e15f763380b45e841ec32"))
        insert into t values(false, 'b', 0h, 0, 0l, 1970.01.01, 1970.01M, 01:00:00.000, 01:00m, 01:00:00, 1970.01.01T01:00:00, 1970.01.01T01:00:00.000, 00:00:00.000000000, 1970.01.01T01:00:00.000000000, -0.0f, 0.0, "hi", "sea", uuid("5d212a78-cc48-e3b1-4235-b4d91473ee87"), ipaddr("192.168.1.13"), int128("e1671797c52e15f763380b45e841ec32"))
        insert into t values(true, 'c', 15h, 15, 15l, 2012.01.01, 2012.01M, 12:00:00.000, 12:00m, 12:00:00, 2012.01.01T01:00:00, 2012.01.01T01:00:00.000, 12:00:00.000000000, 2012.01.01T01:00:00.000000000, -15.5f, -15.5, "bye", "bye", uuid("5d212a78-cc48-e3b1-4235-b4d91473ee87"), ipaddr("192.168.1.13"), int128("e1671797c52e15f763380b45e841ec32"))
        insert into t values(bool(), char(), short(), int(), long(), date(), month(), time(), minute(), second(), datetime(), timestamp(), nanotime(), nanotimestamp(), float(), double(), "", "", uuid(), ipaddr(), int128())
        t
        '''
        res=self.s.run(script)
        assert_array_equal(res["cbool"], [True, False, True, None])
        assert_array_almost_equal(res["cchar"], [97, 98, 99, np.NaN])
        assert_array_almost_equal(res["cshort"], [-5, 0, 15, np.NaN])
        assert_array_almost_equal(res["cint"], [-5, 0, 15, np.NaN])
        assert_array_almost_equal(res["clong"], [-5, 0, 15, np.NaN])
        assert_array_equal(res["csymbol"], ["hello", "hi", "bye", ""])
        assert_array_equal(res["cstring"], ["world", "sea", "bye", ""])
        assert_array_equal(res["cuuid"], ["5d212a78-cc48-e3b1-4235-b4d91473ee87","5d212a78-cc48-e3b1-4235-b4d91473ee87","5d212a78-cc48-e3b1-4235-b4d91473ee87","00000000-0000-0000-0000-000000000000"])
        assert_array_equal(res["cipaddr"], ["192.168.1.13","192.168.1.13","192.168.1.13","0.0.0.0"])
        assert_array_equal(res["cint128"], ["e1671797c52e15f763380b45e841ec32","e1671797c52e15f763380b45e841ec32","e1671797c52e15f763380b45e841ec32","00000000000000000000000000000000"])
    
    def test_dfs_huge_dataSet_toDF(self):
        script='''
        login("admin","123456")
        dbName="dfs://test_toDF"
        tableName="pt"
        if(existsDatabase(dbName)){
            dropDatabase(dbName)
        }
        n=10000000
        id_range=cutPoints(1..n, 20)
        db=database(dbName, RANGE, id_range)
        colNames=`id`cchar`cbool`cshort`cint`clong`cdate`cmonth`ctime`cminute`csecond`cdatetime`ctimestamp`cnanotime`cnanotimestamp`cfloat`cdouble`csymbol`cstring
        colTypes=[INT,CHAR,BOOL,SHORT,INT,LONG,DATE,MONTH,TIME,MINUTE,SECOND,DATETIME,TIMESTAMP,NANOTIME,NANOTIMESTAMP,FLOAT,DOUBLE,SYMBOL,STRING]
        t=table(1:0, colNames, colTypes)
        pt=db.createPartitionedTable(t, tableName, `id)
        tmp=table(n:n, colNames, colTypes)
        tmp[`id]=1..n
        tmp[`cchar]=loop(take{, n/10}, char('a'..('a'+9))).flatten()
        tmp[`cbool]=loop(take{, n/4}, [true, false, true, false]).flatten()
        tmp[`cshort]=loop(take{, n/10}, short(-4..5)).flatten()
        tmp[`cint]=1..n
        tmp[`clong]=long(1..n)
        tmp[`cdate]=loop(take{, n/10}, 1969.12.27..1970.01.05).flatten()
        tmp[`cmonth]=loop(take{, n/10}, 1969.07M..1970.04M).flatten()
        tmp[`ctime]=loop(take{, n/10}, temporalAdd(00:00:00.000, 0..9, "h")).flatten()
        tmp[`cminute]=loop(take{, n/10}, 00:00m..00:09m).flatten()
        tmp[`csecond]=loop(take{, n/10}, temporalAdd(00:00:00, 0..9, "h")).flatten()
        tmp[`cdatetime]=loop(take{, n/10}, temporalAdd(1969.12.27T12:00:01, 0..9, 'd')).flatten()
        tmp[`ctimestamp]=loop(take{, n/10}, temporalAdd(1969.12.27T12:00:00.001, 0..9, 'd')).flatten()
        tmp[`cnanotime]=loop(take{, n/10}, temporalAdd(00:00:00.000000001, 0..9, 'h')).flatten()
        tmp[`cnanotimestamp]=loop(take{, n/10}, temporalAdd(1969.12.27T12:00:00.000000001, 0..9, 'd')).flatten()
        tmp[`cfloat]=loop(take{,n/10}, float(0.02f+1..10)).flatten()
        tmp[`cdouble]=double(0.02+1..(n))
        tmp[`csymbol]=loop(take{, n/10}, symbol("A"+string(0..9))).flatten()
        tmp[`cstring]="B"+string(1..n)
        pt.append!(tmp)
        '''
        self.s.run(script)
        tmp=self.s.loadTable(tableName="pt",dbPath="dfs://test_toDF")
        res=tmp.toDF()
        id_expected=np.arange(1, 10000001)
        batch=1000000
        cchar_expected=np.concatenate((np.repeat(97, batch), np.repeat(98, batch), np.repeat(99, batch), np.repeat(100, batch), np.repeat(101, batch), np.repeat(102, batch), np.repeat(103, batch), np.repeat(104, batch), np.repeat(105, batch), np.repeat(106, batch)), axis=0)
        cbool_expected=np.concatenate((np.repeat(True, 2500000), np.repeat(False, 2500000), np.repeat(True, 2500000), np.repeat(False, 2500000)), axis=0)
        cshort_expected=np.concatenate((np.repeat(-4, batch), np.repeat(-3, batch), np.repeat(-2, batch), np.repeat(-1, batch), np.repeat(0, batch), np.repeat(1, batch), np.repeat(2, batch), np.repeat(3, batch), np.repeat(4, batch), np.repeat(5, batch)), axis=0)
        cint_expected=np.arange(1, 10000001)
        clong_expected=np.arange(1, 10000001)
        cdate_expected=np.array(np.concatenate((np.repeat("1969-12-27", batch), np.repeat("1969-12-28", batch), np.repeat("1969-12-29", batch), np.repeat("1969-12-30", batch), np.repeat("1969-12-31", batch), np.repeat("1970-01-01", batch), np.repeat("1970-01-02", batch), np.repeat("1970-01-03", batch), np.repeat("1970-01-04", batch), np.repeat("1970-01-05", batch)), axis=0), dtype="datetime64")
        cmonth_expected=np.array(np.concatenate((np.repeat("1969-07-01", batch), np.repeat("1969-08-01", batch), np.repeat("1969-09-01", batch), np.repeat("1969-10-01", batch), np.repeat("1969-11-01", batch), np.repeat("1969-12-01", batch), np.repeat("1970-01-01", batch), np.repeat("1970-02-01", batch), np.repeat("1970-03-01", batch), np.repeat("1970-04-01", batch)), axis=0), dtype="datetime64")
        ctime_expected=np.array(np.concatenate((np.repeat("1970-01-01T00:00:00", batch), np.repeat("1970-01-01T01:00:00", batch), np.repeat("1970-01-01T02:00:00", batch), np.repeat("1970-01-01T03:00:00", batch), np.repeat("1970-01-01T04:00:00", batch), np.repeat("1970-01-01T05:00:00", batch), np.repeat("1970-01-01T06:00:00", batch), np.repeat("1970-01-01T07:00:00", batch), np.repeat("1970-01-01T08:00:00", batch), np.repeat("1970-01-01T09:00:00", batch)), axis=0), dtype="datetime64")
        cminute_expected=np.array(np.concatenate((np.repeat("1970-01-01T00:00:00", batch), np.repeat("1970-01-01T00:01:00", batch), np.repeat("1970-01-01T00:02:00", batch), np.repeat("1970-01-01T00:03:00", batch), np.repeat("1970-01-01T00:04:00", batch), np.repeat("1970-01-01T00:05:00", batch), np.repeat("1970-01-01T00:06:00", batch), np.repeat("1970-01-01T00:07:00", batch), np.repeat("1970-01-01T00:08:00", batch), np.repeat("1970-01-01T00:09:00", batch)), axis=0), dtype="datetime64")
        csecond_expected=np.array(np.concatenate((np.repeat("1970-01-01T00:00:00", batch), np.repeat("1970-01-01T01:00:00", batch), np.repeat("1970-01-01T02:00:00", batch), np.repeat("1970-01-01T03:00:00", batch), np.repeat("1970-01-01T04:00:00", batch), np.repeat("1970-01-01T05:00:00", batch), np.repeat("1970-01-01T06:00:00", batch), np.repeat("1970-01-01T07:00:00", batch), np.repeat("1970-01-01T08:00:00", batch), np.repeat("1970-01-01T09:00:00", batch)), axis=0), dtype="datetime64")
        cdatetime_expected=np.array(np.concatenate((np.repeat("1969-12-27T12:00:01", batch), np.repeat("1969-12-28T12:00:01", batch), np.repeat("1969-12-29T12:00:01", batch), np.repeat("1969-12-30T12:00:01", batch), np.repeat("1969-12-31T12:00:01", batch), np.repeat("1970-01-01T12:00:01", batch), np.repeat("1970-01-02T12:00:01", batch), np.repeat("1970-01-03T12:00:01", batch), np.repeat("1970-01-04T12:00:01", batch), np.repeat("1970-01-05T12:00:01", batch)), axis=0), dtype="datetime64")
        ctimestamp_expected=np.array(np.concatenate((np.repeat("1969-12-27T12:00:00.001", batch), np.repeat("1969-12-28T12:00:00.001", batch), np.repeat("1969-12-29T12:00:00.001", batch), np.repeat("1969-12-30T12:00:00.001", batch), np.repeat("1969-12-31T12:00:00.001", batch), np.repeat("1970-01-01T12:00:00.001", batch), np.repeat("1970-01-02T12:00:00.001", batch), np.repeat("1970-01-03T12:00:00.001", batch), np.repeat("1970-01-04T12:00:00.001", batch), np.repeat("1970-01-05T12:00:00.001", batch)), axis=0), dtype="datetime64")
        cnanotime_expected=np.array(np.concatenate((np.repeat("1970-01-01T00:00:00.000000001", batch), np.repeat("1970-01-01T01:00:00.000000001", batch), np.repeat("1970-01-01T02:00:00.000000001", batch), np.repeat("1970-01-01T03:00:00.000000001", batch), np.repeat("1970-01-01T04:00:00.000000001", batch), np.repeat("1970-01-01T05:00:00.000000001", batch), np.repeat("1970-01-01T06:00:00.000000001", batch), np.repeat("1970-01-01T07:00:00.000000001", batch), np.repeat("1970-01-01T08:00:00.000000001", batch), np.repeat("1970-01-01T09:00:00.000000001", batch)), axis=0), dtype="datetime64")
        cnanotimestamp_expected=np.array(np.concatenate((np.repeat("1969-12-27T12:00:00.000000001", batch), np.repeat("1969-12-28T12:00:00.000000001", batch), np.repeat("1969-12-29T12:00:00.000000001", batch), np.repeat("1969-12-30T12:00:00.000000001", batch), np.repeat("1969-12-31T12:00:00.000000001", batch), np.repeat("1970-01-01T12:00:00.000000001", batch), np.repeat("1970-01-02T12:00:00.000000001", batch), np.repeat("1970-01-03T12:00:00.000000001", batch), np.repeat("1970-01-04T12:00:00.000000001", batch), np.repeat("1970-01-05T12:00:00.000000001", batch)), axis=0), dtype="datetime64")
        cfloat_expected=np.concatenate((np.repeat(1.02, batch), np.repeat(2.02, batch), np.repeat(3.02, batch), np.repeat(4.02, batch), np.repeat(5.02, batch), np.repeat(6.02, batch), np.repeat(7.02, batch), np.repeat(8.02, batch), np.repeat(9.02, batch), np.repeat(10.02, batch)), axis=0)
        cdouble_expected=np.arange(1, 10000001)+0.02
        csymbol_expected=np.concatenate((np.repeat("A0", batch), np.repeat("A1", batch), np.repeat("A2", batch), np.repeat("A3", batch), np.repeat("A4", batch), np.repeat("A5", batch), np.repeat("A6", batch), np.repeat("A7", batch), np.repeat("A8", batch), np.repeat("A9", batch)), axis=0)
        cstring_expected=["B"+b for b in [str(x) for x in np.arange(1, 10000001)]]

        assert_array_equal(res["id"], id_expected)
        assert_array_equal(res["cbool"], cbool_expected)
        assert_array_equal(res["cchar"], cchar_expected)
        assert_array_equal(res["cshort"], cshort_expected)
        assert_array_equal(res["cint"], cint_expected)
        assert_array_equal(res["clong"], clong_expected)
        assert_array_equal(res["cdate"], cdate_expected)
        assert_array_equal(res["cmonth"], cmonth_expected)
        assert_array_equal(res["ctime"], ctime_expected)
        assert_array_equal(res["cminute"], cminute_expected)
        assert_array_equal(res["csecond"], csecond_expected)
        assert_array_equal(res["cdatetime"], cdatetime_expected)
        assert_array_equal(res["ctimestamp"], ctimestamp_expected)
        assert_array_equal(res["cnanotime"], cnanotime_expected)
        assert_array_equal(res["cnanotimestamp"], cnanotimestamp_expected)
        assert_array_almost_equal(res["cfloat"], cfloat_expected)
        assert_array_almost_equal(res["cdouble"], cdouble_expected)
        assert_array_equal(res["csymbol"], csymbol_expected)
        assert_array_equal(res["cstring"], cstring_expected)

    def test_small_dataSet_loadTable(self):
        script='''
        login("admin","123456")
        dbName="dfs://test_toDF"
        tableName="pt"
        if(existsDatabase(dbName)){
            dropDatabase(dbName)
        }
        db=database(dbName, VALUE, 2012.01.01..2012.01.03)
        date=take(2012.01.01, 10) join take(2012.01.02, 5) join take(2012.01.03, 15)
        sym1=["SH600005", "SH600002", "SH600006", "SH600001", "SH600010", "SH600007", "SH600021", "SH600011", "SH600006", "SH600006"]
        sym2=["SH600001", "SH600031", "SH600021", "SH600007", "SH600009"]
        sym3=["SH600041", "SH600011", "SH600021", "SH600017", "SH600009", "SH600008", "SH600023", "SH600001", "SH600036", "SH600016", "SH600001", "", "SH600021", "SH600007", ""]
        sym=array(SYMBOL, 0, 30).append!(sym1).append!(sym2).append!(sym3)
        val=1..30
        t=table(date, sym, val)
        pt=db.createPartitionedTable(t, `pt, `date).append!(t)
        '''
        self.s.run(script)
        tmp=self.s.run("select * from loadTable('dfs://test_toDF', 'pt') where date=2012.01.02")
        assert_array_equal(tmp["date"], np.array(np.repeat("2012-01-02",5), dtype="datetime64"))
        assert_array_equal(tmp["sym"], ["SH600001", "SH600031", "SH600021", "SH600007", "SH600009"])
        assert_array_equal(tmp["val"], np.arange(11, 16))

        tmp=self.s.loadTableBySQL(tableName="pt", dbPath="dfs://test_toDF", sql="select * from pt where date>2012.01.02")
        res=tmp.toDF()
        assert_array_equal(res["date"], np.array(np.repeat("2012-01-03", 15), dtype="datetime64"))
        assert_array_equal(res["sym"], ["SH600041", "SH600011", "SH600021", "SH600017", "SH600009", "SH600008", "SH600023", "SH600001", "SH600036", "SH600016", "SH600001", "", "SH600021", "SH600007", ""])
        assert_array_equal(res["val"], np.arange(16, 31))
    
    def test_dropTable_paramete(self):    

if __name__ == '__main__':
    unittest.main()
