import unittest
import dolphindb as ddb
import numpy as np
import pandas as pd
from numpy.testing import assert_array_equal, assert_array_almost_equal
from pandas.testing import assert_frame_equal, assert_series_equal
from setup import HOST, PORT, WORK_DIR, DATA_DIR


class TestRunFunction(unittest.TestCase):
    @classmethod
    def setUp(cls):
        cls.s = ddb.session()
        cls.s.connect(HOST, PORT, "admin", "123456")

    @classmethod
    def tearDownClass(cls):
        pass
    
    def test_run_bool_scalar(self):
        re = self.s.run("true")
        self.assertEqual(re, True)
        re = self.s.run("bool()")
        self.assertIsNone(re, True)

    def test_run_char_scalar(self):
        re = self.s.run("'a'")
        self.assertEqual(re, 97)
        re = self.s.run("char()")
        self.assertIsNone(re, True)

    def test_run_short_scalar(self):
        re = self.s.run("22h")
        self.assertEqual(re, 22)
        re = self.s.run("short()")
        self.assertIsNone(re, True)

    def test_run_int_scalar(self):
        re = self.s.run("22")
        self.assertEqual(re, 22)
        re = self.s.run("int()")
        self.assertIsNone(re, True)

    def test_run_long_scalar(self):
        re = self.s.run("22l")
        self.assertEqual(re, 22)
        re = self.s.run("long()")
        self.assertIsNone(re, True)
    
    def test_run_date_scalar(self):
        re = self.s.run("2012.06.12")
        self.assertEqual(re, np.datetime64("2012-06-12"))
        re = self.s.run("date()")
        self.assertIsNone(re, True)

    def test_run_month_scalar(self):
        re = self.s.run("2012.06M")
        self.assertEqual(re, np.datetime64("2012-06"))
        re = self.s.run("month()")
        self.assertIsNone(re, True)
    
    def test_run_time_scalar(self):
        re = self.s.run("13:30:10.008")
        self.assertEqual(re, np.datetime64("1970-01-01T13:30:10.008"))
        re = self.s.run("time()")
        self.assertIsNone(re, True)

    def test_run_minute_scalar(self):
        re = self.s.run("13:30m")
        self.assertEqual(re, np.datetime64("1970-01-01T13:30"))
        re = self.s.run("minute()")
        self.assertIsNone(re, True)

    def test_run_second_scalar(self):
        re = self.s.run("13:30:10")
        self.assertEqual(re, np.datetime64("1970-01-01T13:30:10"))
        re = self.s.run("second()")
        self.assertIsNone(re, True)

    def test_run_datetime_scalar(self):
        re = self.s.run("2012.06.13T13:30:10")
        self.assertEqual(re, np.datetime64("2012-06-13T13:30:10"))
        re = self.s.run("datetime()")
        self.assertIsNone(re, True)

    def test_run_timestamp_scalar(self):
        re = self.s.run("2012.06.13 13:30:10.008")
        self.assertEqual(re, np.datetime64("2012-06-13 13:30:10.008"))
        re = self.s.run("timestamp()")
        self.assertIsNone(re, True)

    def test_run_nanotime_salar(self):
        re = self.s.run("13:30:10.008007006")
        self.assertEqual(re, np.datetime64("1970-01-01T13:30:10.008007006"))
        re = self.s.run("nanotime()")
        self.assertIsNone(re, True)

    def test_run_nanotimestamp_salar(self):
        re = self.s.run("2012.06.13T13:30:10.008007006")
        self.assertEqual(re, np.datetime64("2012-06-13T13:30:10.008007006"))
        re = self.s.run("nanotimestamp()")
        self.assertIsNone(re, True)

    def test_run_float_scalar(self):
        re = self.s.run("2.1f")
        self.assertAlmostEqual(re, 2.1, places=2)
        re = self.s.run("float()")
        self.assertIsNone(re, True)
    
    def test_run_double_scalar(self):
        re = self.s.run("2.1")
        self.assertAlmostEqual(re, 2.1, places=2)
        re = self.s.run("double()")
        self.assertIsNone(re, True)

    def test_run_string_scalar(self):
        re = self.s.run("`aaaa")
        self.assertEqual(re, "aaaa")
        re = self.s.run("string()")
        self.assertIsNone(re, True)

    def test_run_uuid_scalar(self):
        re = self.s.run("uuid('5d212a78-cc48-e3b1-4235-b4d91473ee87')")
        self.assertEqual(re, "5d212a78-cc48-e3b1-4235-b4d91473ee87")
        re = self.s.run("uuid()")
        self.assertIsNone(re, True)

    def test_run_ipaddr_scalar(self):
        re = self.s.run("ipaddr('192.168.1.135')")
        self.assertEqual(re, "192.168.1.135")
        re = self.s.run("ipaddr()")
        self.assertIsNone(re, True)

    def test_run_int128_scalar(self):
        re = self.s.run("int128('e1671797c52e15f763380b45e841ec32')")
        self.assertEqual(re, "e1671797c52e15f763380b45e841ec32")
        re = self.s.run("int128()")
        self.assertIsNone(re, True)

    def test_run_bool_vector(self):
        re = self.s.run("true false false true")
        assert_array_equal(re, [True, False, False, True])
        #re = self.s.run("take(bool(), 5)")
        #self.assertIsNone(re, True)
        
    def test_run_char_vector(self):
        re = self.s.run("['a', 'b', 'c']")
        assert_array_equal(re, [97, 98, 99])
        re = self.s.run("take(char(), 5)")
        self.assertTrue(np.isnan(re).all())

    def test_run_short_vector(self):
        re = self.s.run("[10h, 20h, 30h, 40h]")
        assert_array_equal(re, [10, 20, 30, 40])
    
    def test_run_int_vector(self):
        re = self.s.run("1..5")
        assert_array_equal(re, [1, 2, 3, 4, 5])
    
    def test_run_long_vector(self):
        re = self.s.run("long(11..15)")
        assert_array_equal(re, [11, 12, 13, 14, 15])

    def test_run_date_vector(self):
        re = self.s.run("2012.06.01..2012.06.05")
        assert_array_equal(re, np.array(["2012-06-01", "2012-06-02", "2012-06-03", "2012-06-04", "2012-06-05"], dtype="datetime64[D]"))
    
    def test_run_month_vector(self):
        re = self.s.run("2012.06M..2012.10M")
        assert_array_equal(re, np.array(["2012-06", "2012-07", "2012-08", "2012-09", "2012-10"], dtype="datetime64[M]"))
    
    def test_run_time_vector(self):
        re = self.s.run("13:30:10.001 13:30:10.002")
        assert_array_equal(re, np.array(["1970-01-01T13:30:10.001", "1970-01-01T13:30:10.002"], dtype="datetime64[ms]"))
    
    def test_run_minute_vector(self):
        re = self.s.run("13:30m 13:31m")
        assert_array_equal(re, np.array(["1970-01-01T13:30", "1970-01-01T13:31"], dtype="datetime64[m]"))
    
    def test_run_second_vector(self):
        re = self.s.run("13:30:10 13:30:11")
        assert_array_equal(re, np.array(["1970-01-01T13:30:10", "1970-01-01T13:30:11"], dtype="datetime64[s]"))

    def test_run_datetime_vector(self):
        re = self.s.run(" 2012.06.13T13:30:10  2012.06.13T13:30:11")
        assert_array_equal(re, np.array([" 2012-06-13T13:30:10", " 2012-06-13T13:30:11"], dtype="datetime64[s]"))
    
    def test_run_timestamp_vector(self):
        re = self.s.run("2012.06.13T13:30:10.008 2012.06.13T13:30:10.009")
        assert_array_equal(re, np.array(["2012-06-13T13:30:10.008", "2012-06-13T13:30:10.009"], dtype="datetime64[ms]"))
    
    def test_run_nanotime_vector(self):
        re = self.s.run("13:30:10.008007006 13:30:10.008007007")
        assert_array_equal(re, np.array(["1970-01-01T13:30:10.008007006", "1970-01-01T13:30:10.008007007"], dtype="datetime64[ns]"))
    
    def test_run_nanotimestamp_vector(self):
        re = self.s.run("2012.06.13T13:30:10.008007006 2012.06.13T13:30:10.008007007")
        assert_array_equal(re, np.array(["2012-06-13T13:30:10.008007006", "2012-06-13T13:30:10.008007007"], dtype="datetime64[ns]"))

    def test_run_floar_vector(self):
        re = self.s.run("float(2.1 2.2)")
        assert_array_almost_equal(re, [2.1, 2.2], decimal=1)
    
    def test_run_double_vector(self):
        re = self.s.run("2.1 2.1")
        assert_array_almost_equal(re, [2.1, 2.2], decimal=1)

    def test_run_string_vector(self):
        re = self.s.run("`a`b`c")
        assert_array_equal(re, ["a", "b", "c"])

    def test_run_symbol_vector(self):
        re = self.s.run("symbol(`a`b`c)")
        assert_array_equal(re, ["a", "b", "c"])

    def test_run_int_set(self):
        re = self.s.run("set(1..5)")
        self.assertSetEqual(re, set([1,2,3,4,5]))

    def test_run_int_matrix(self):
        re = self.s.run("1..4$2:2")
        assert_array_equal(re[0], [[1,3], [2, 4]])

    def test_run_tuple(self):
        re = self.s.run("[1, `a, 2]")
        assert_array_equal(re, ["1","a","2"])
    
    def test_run_vector_vector(self):
        re = self.s.run("[[1,2,3],`a`b]")
        assert_array_equal(re[0], [1,2,3])
        assert_array_equal(re[1], ["a","b"])

    def test_run_dict_value_scalar(self):
        re = self.s.run("dict(`a`b`c,1 2 3)")
        self.assertDictEqual(re, {'b': 2, 'c': 3, 'a': 1})

    def test_run_dict_value_vector(self):
        re = self.s.run("dict(`a`b`c, [1..3, 4..6, 7..9])")
        assert_array_equal(re["a"], [1, 2, 3])
        assert_array_equal(re["b"], [4, 5, 6])
        assert_array_equal(re["c"], [7, 8, 9])

    def test_run_table(self):
        re = self.s.run("table(`AAPL`MS`C`IBM as sym, 45 48 52 56 as vol)")
        tmp = {"sym": ['AAPL', 'MS', 'C', 'IBM'],
               "vol": np.array([45, 48, 52, 56], dtype="int32")}
        assert_frame_equal(re, pd.DataFrame(tmp))

    def test_function_add_int(self):
        re = self.s.run('add', 3, 4)
        self.assertEqual(re, 7)

    def test_function_add_string(self):
        re = self.s.run('add', 'hello', 'world')
        self.assertMultiLineEqual(re, 'helloworld')

    def test_function_sum_list(self):
        re = self.s.run('sum', [1.0, 2.0, 3.0])
        self.assertAlmostEqual(re, 6.0)

    def test_function_sum_numpy_array_int32(self):
        re = self.s.run('sum', np.array([100000, 200000, 300000]))
        self.assertEqual(re, 600000)

    def test_function_sum_numpy_array_int64(self):
        pass
        # re=self.s.run('sum',np.int64([1e15, 2e15, 3e15])

    def test_function_sum_numpy_array_float64(self):
        re = self.s.run('sum', np.array([100000.0, 200000.0, 300000.0]))
        self.assertAlmostEqual(re, 600000.0)

    def test_function_reverse_str_array(self):
        re = self.s.run('reverse', np.array(["1", "2", "3"], dtype="str"))
        self.assertMultiLineEqual(re[0], '3')
        self.assertMultiLineEqual(re[1], '2')
        self.assertMultiLineEqual(re[2], '1')

    def test_function_flatten_matrix(self):
        re = self.s.run('flatten', np.int32([[1, 2, 3], [4, 5, 6]]))
        self.assertEqual((re == np.array([1, 4, 2, 5, 3, 6])).all(), True)

    def test_function_case_matrix(self):
        pass
        # TOOD: matrix bug
        # self.s.run("cast", np.double([[1.0, 2.0, 3.0], [4.0, 5.0, 6.0]]),[2,3])

    def test_function_wavg(self):
        col1 = [100, 30, 300]
        col2 = [1.0, 1.5, 2.0]
        re = self.s.run("wavg", col1, col2)
        self.assertAlmostEqual(re, 165.5556, places=4)

    def test_function_wsum(self):
        col1 = [100, 30, 300]
        col2 = [1.0, 1.5, 2.0]
        re = self.s.run("wsum", col1, col2)
        self.assertAlmostEqual(re, 745.0, places=1)

    def test_function_wavg_partial(self):
        col1 = [100, 30, 300]
        re = self.s.run("wavg{, [1, 1.5, 2]}", col1)
        self.assertAlmostEqual(re, 165.5556, places=4)

    def test_user_defined_function(self):
        re = self.s.run("login('admin','123456')")
        self.s.run("def foo(a,b){return a+b-1}")
        re = self.s.run('foo', 3, 4)
        self.assertEqual(re, 6)

    def test_clear_variable(self):
        self.s.run('''t = table(1..10 as id,rand(10,10) as val1)
                                select * from t''',clearMemory = True)
        def secondRun():
            self.s.run("t")
        self.assertRaises(RuntimeError, secondRun)

    def test_BlockReader_Table(self):
        self.s.run('''
        rows=10000;
        testblock=table(take(1..rows,rows) as id,take(`A,rows) as symbol,take(2020.08.01..2020.10.01,rows) as date, rand(50,rows) as size,rand(50.5,rows) as price)''')
        br = self.s.run("select * from testblock",fetchSize=10000)
        self.assertTrue(br.hasNext())
        re = br.read()
        self.assertFalse(br.hasNext())
        expected = self.s.run("select  * from testblock")
        assert_frame_equal(re,expected)
        re = br.read()
        self.assertIsNone(re)

        br = self.s.run("select * from testblock",fetchSize=8200)
        self.assertTrue(br.hasNext())
        tem = br.read()
        self.assertTrue(br.hasNext())
        self.assertEqual(len(tem),8200)
        re = br.read()
        self.assertFalse(br.hasNext())
        expected = self.s.run("select  * from testblock where id>8200")
        assert_frame_equal(re,expected)

        br = self.s.run("select * from testblock",fetchSize=10001)
        self.assertTrue(br.hasNext())
        re = br.read()
        self.assertFalse(br.hasNext())
        expected = self.s.run("select  * from testblock")
        assert_frame_equal(re,expected)

        def errFetchSize():
            self.s.run("select * from testblock",fetchSize=8191)
        self.assertRaises(RuntimeError, errFetchSize)

        def fetchSizeZero():
            self.s.run("select * from testblock",fetchSize=0)
        self.assertRaises(RuntimeError, fetchSizeZero)


    def test_Block_Reader_DFStable(self):
        self.s.run('''
        n = 10000
        t = table(take(1..n,n) as id,take(2010.01.01,n) as date,rand(30,n) as price)
        dbPath = "dfs://TEST_BLOCK"
        if(existsDatabase(dbPath)){
            dropDatabase(dbPath)
        }
        db = database(dbPath,VALUE,2010.01.01..2010.01.30)
        pt = db.createPartitionedTable(t,`pt,`date)
        pt.append!(t)
        '''
         )
        br = self.s.run("select * from loadTable(dbPath,`pt)",fetchSize=10001)
        self.assertTrue(br.hasNext())
        re = br.read()
        expected = self.s.run("select * from loadTable(dbPath,`pt)")
        assert_frame_equal(re,expected)
        self.assertFalse(br.hasNext())
        re = br.read()
        self.assertIsNone(re)

        br = self.s.run("select * from loadTable(dbPath,`pt)",fetchSize=8200)
        temp = br.read()
        self.assertTrue(br.hasNext())
        self.assertEqual(len(temp),8200)
        re = br.read()
        self.assertFalse(br.hasNext())
        expected = self.s.run("select * from loadTable(dbPath,`pt) where id>8200")
        assert_frame_equal(re,expected)

        def errFetchSize():
            self.s.run("select * from loadTable(dbPath,`pt)",fetchSize=8191)
        self.assertRaises(RuntimeError, errFetchSize)

        def fetchSizeZero():
            self.s.run("select * from loadTable(dbPath,`pt)",fetchSize=0)
        self.assertRaises(RuntimeError, fetchSizeZero)

    def test_Block_Reader_skipALL(self):
        br = self.s.run('''select * from loadTable("dfs://TEST_BLOCK",`pt)''',fetchSize=10001)
        br.skipAll()
        re = br.read()
        re = br.read()
        self.assertIsNone(re)

        self.s.run('''
        rows=10000
        testblock=table(take(1..rows,rows) as id,take(`A,rows) as symbol,take(2020.08.01..2020.10.01,rows) as date, rand(50,rows) as size,rand(50.5,rows) as price)''')
        br = self.s.run("select * from testblock",fetchSize=10000)
        self.assertTrue(br.hasNext())
        br.skipAll()
        self.assertFalse(br.hasNext())
        self.assertIsNone(re)

    def test_Block_Reader_huge_table(self):
        self.s.run('''
        rows = 20000000
        testblock=table(1..rows as id,take(string('A'..'Z'),rows) as symbol,take(2020.08.01..2020.10.01,rows) as date, rand(50,rows) as size,rand(50.5,rows) as price)
        ''')
        fetchSize=10000000
        br = self.s.run("select * from testblock",fetchSize=fetchSize)
        temp = br.read()
        self.assertEqual(len(temp),fetchSize)
        self.assertTrue(br.hasNext())
        re = br.read()
        self.assertEqual(len(temp),fetchSize)
        self.assertFalse(br.hasNext())

    def test_Block_Reader_huge_Dfs(self):
        self.s.run('''
        n = 20000000
        t = table(1..n as id,take(2010.01.01,n) as date,take(string('A'..'Z'),n) as symbol,rand(30,n) as price)
        dbPath = "dfs://Test_Huge_Block"
        if(existsDatabase(dbPath)){
            dropDatabase(dbPath)
        }
        db = database(dbPath,VALUE,2010.01.01..2010.01.30)
        pt = db.createPartitionedTable(t,`pt,`date)
        pt.append!(t)
        ''')
        fetchSize=10000000
        br = self.s.run("select * from loadTable(dbPath,`pt)",fetchSize=fetchSize)
        temp = br.read()
        self.assertTrue(br.hasNext())
        self.assertEqual(len(temp),fetchSize)
        re = br.read()
        self.assertFalse(br.hasNext())
        self.assertEqual(len(temp),fetchSize)



if __name__ == '__main__':
    unittest.main()
