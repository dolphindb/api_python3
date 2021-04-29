import unittest
import dolphindb as ddb
import numpy as np
import pandas as pd
from setup import HOST, PORT, WORK_DIR, DATA_DIR
from numpy.testing import assert_array_equal, assert_array_almost_equal
from pandas.testing import assert_series_equal
from pandas.testing import assert_frame_equal

class TestBasicDataTypes(unittest.TestCase):
    @classmethod
    def setUp(cls):
        cls.s = ddb.session()
        cls.s.connect(HOST, PORT, "admin", "123456")


    @classmethod
    def tearDownClass(cls):
        pass

    def test_int_scalar(self):
        re = self.s.run("100")
        self.assertEqual(re, 100)
        re = self.s.run("int()")
        self.assertIsNone(re)

    def test_bool_scalar(self):
        re = self.s.run("true")
        self.assertEqual(re, True)
        re = self.s.run("bool()")
        self.assertIsNone(re)

    def test_char_scalar(self):
        re = self.s.run("'a'")
        self.assertEqual(re, 97)
        re = self.s.run("char()")
        self.assertIsNone(re)

    def test_short_scalar(self):
        re = self.s.run("112h")
        self.assertEqual(re, 112)
        re = self.s.run("short()")
        self.assertIsNone(re)

    def test_long_scalar(self):
        re = self.s.run("22l")
        self.assertEqual(re, 22)
        re = self.s.run("long()")
        self.assertIsNone(re)

    def test_date_scalar(self):
        re = self.s.run("2012.06.12")
        self.assertEqual(re, np.datetime64('2012-06-12'))
        re = self.s.run("date()")
        self.assertIsNone(re)

    def test_month_scalar(self):
        re = self.s.run("2012.06M")
        self.assertEqual(re, np.datetime64('2012-06'))
        re = self.s.run("month()")
        self.assertIsNone(re)

    def test_time_scalar(self):
        re = self.s.run("12:30:00.008")
        self.assertEqual(re, np.datetime64('1970-01-01T12:30:00.008'))
        re = self.s.run("time()")
        self.assertIsNone(re)

    def test_minute_scalar(self):
        re = self.s.run("12:30m")
        self.assertEqual(re, np.datetime64('1970-01-01T12:30'))
        re = self.s.run("minute()")
        self.assertIsNone(re)

    def test_second_scalar(self):
        re = self.s.run("12:30:10")
        self.assertEqual(re, np.datetime64('1970-01-01T12:30:10'))
        re = self.s.run("second()")
        self.assertIsNone(re)

    def test_datetime_scalar(self):
        re = self.s.run('2012.06.13 13:30:10')
        self.assertEqual(re, np.datetime64('2012-06-13T13:30:10'))
        re = self.s.run("datetime()")
        self.assertIsNone(re)

    def test_timestamp_scalar(self):
        re = self.s.run('2012.06.13 13:30:10.008')
        self.assertEqual(re, np.datetime64('2012-06-13T13:30:10.008'))
        re = self.s.run("timestamp()")
        self.assertIsNone(re)

    def test_nanotime_scalar(self):
        re = self.s.run('13:30:10.008007006')
        self.assertEqual(re, np.datetime64('1970-01-01T13:30:10.008007006'))
        re = self.s.run("nanotime()")
        self.assertIsNone(re)

    def test_nanotimestamp_scalar(self):
        re = self.s.run('2012.06.13 13:30:10.008007006')
        self.assertEqual(re, np.datetime64('2012-06-13T13:30:10.008007006'))
        re = self.s.run("nanotimestamp()")
        self.assertIsNone(re)

    def test_float_scalar(self):
        re = self.s.run('2.1f')
        self.assertEqual(round(re), 2)
        re = self.s.run("float()")
        self.assertIsNone(re)

    def test_double_scalar(self):
        re = self.s.run('2.1')
        self.assertEqual(re, 2.1)
        re = self.s.run("double()")
        self.assertIsNone(re)

    def test_string_scalar(self):
        re = self.s.run('"abc"')
        self.assertEqual(re, 'abc')
        re = self.s.run("string()")
        self.assertIsNone(re)

    def test_uuid_scalar(self):
        re = self.s.run("uuid('5d212a78-cc48-e3b1-4235-b4d91473ee87')")
        self.assertEqual(re, '5d212a78-cc48-e3b1-4235-b4d91473ee87')
        re = self.s.run("uuid()")
        self.assertIsNone(re)

    def test_ipaddr_sclar(self):
        re = self.s.run("ipaddr('192.168.1.135')")
        self.assertEqual(re, '192.168.1.135')
        re = self.s.run("ipaddr()")
        self.assertIsNone(re)

    def test_int128_scalar(self):
        re = self.s.run("int128('e1671797c52e15f763380b45e841ec32')")
        self.assertEqual(re, 'e1671797c52e15f763380b45e841ec32')
        re = self.s.run("int128()")
        self.assertIsNone(re)

    def test_python_datetime64_dolphindb_date_scalar(self):
        ts = np.datetime64('2019-01-01T20:01:01.1223461')
        self.assertEqual(self.s.run('date', ts), np.datetime64('2019-01-01'))

    def test_python_datetime64_dolphindb_month_scalar(self):
        ts = np.datetime64('2019-01-01T20:01:01.1223461')
        self.assertEqual(self.s.run('month', ts), np.datetime64('2019-01'))

    def test_python_datetime64_dolphindb_time_scalar(self):
        ts = np.datetime64('2019-01-01T20:01:01.1223461')
        self.assertEqual(self.s.run('time', ts), np.datetime64('1970-01-01T20:01:01.122'))

    def test_python_datetime64_dolphindb_minute_scalar(self):
        ts = np.datetime64('2019-01-01T20:01:01.1223461')
        self.assertEqual(self.s.run('minute', ts), np.datetime64('1970-01-01T20:01'))

    def test_python_datetime64_dolphindb_second_scalar(self):
        ts = np.datetime64('2019-01-01T20:01:01.1223461')
        self.assertEqual(self.s.run('second', ts), np.datetime64('1970-01-01T20:01:01'))

    def test_python_datetime64_dolphindb_datetime_scalar(self):
        ts = np.datetime64('2019-01-01T20:01:01.1223461')
        self.assertEqual(self.s.run('datetime', ts), np.datetime64('2019-01-01T20:01:01'))

    def test_python_datetime64_dolphindb_timestamp_scalar(self):
        ts = np.datetime64('2019-01-01T20:01:01.1223461')
        self.assertEqual(self.s.run('timestamp', ts), np.datetime64('2019-01-01T20:01:01.122'))

    def test_python_datetime64_dolphindb_nanotime_scalar(self):
        ts = np.datetime64('2019-01-01T20:01:01.1223461')
        self.assertEqual(self.s.run('nanotime', ts), np.datetime64('1970-01-01T20:01:01.122346100'))

    def test_python_datetime64_dolphindb_nanotimestamp_scalar(self):
        ts = np.datetime64('2019-01-01T20:01:01.1223461')
        self.assertEqual(self.s.run('nanotimestamp', ts), np.datetime64('2019-01-01T20:01:01.122346100'))

    def test_string_vector(self):
        re = self.s.run("`IBM`GOOG`YHOO")
        self.assertEqual((re == ['IBM', 'GOOG', 'YHOO']).all(), True)
        re = self.s.run("['IBM', string(), 'GOOG']")
        self.assertEqual((re==['IBM', '', 'GOOG']).all(), True)
        re = self.s.run("[string(), string(), string()]")
        self.assertEqual((re==['','','']).all(), True)

    def test_function_def(self):
        re = self.s.run("def f(a,b){return a+b}")
        re = self.s.run("f(1, 2)")
        self.assertEqual(re, 3)

    def test_symbol_vector(self):
        re = self.s.run("symbol(`IBM`MSFT`GOOG`BIDU)")
        self.assertEqual((re == ['IBM', 'MSFT', 'GOOG', 'BIDU']).all(), True)
        re = self.s.run("symbol(['IBM', '', 'GOOG'])")
        self.assertEqual((re==['IBM', '', 'GOOG']).all(), True)
        re = self.s.run("symbol(['', '', ''])")
        self.assertEqual((re==['', '', '']).all(), True)

    def test_char_vector(self):
        re = self.s.run("['a', 'b', 'c']")
        expected = [97, 98, 99]
        self.assertEqual((re==expected).all(), True)
        re = self.s.run("['a', char(), 'c']")
        expected = [97.0, np.nan, 99.0]
        assert_array_almost_equal(re, expected)
    
    def test_bool_vector(self):
        re = self.s.run("[true, false, true]")
        expected = [True, False, True]
        assert_array_equal(re, expected)
        re = self.s.run("[true, false, bool()]")
        assert_array_equal(re[0:2], [True, False])
        self.assertTrue(np.isnan(re[2]))
        re = self.s.run("[bool(), bool(), bool()]")
        self.assertTrue(np.isnan(re[0]))
        self.assertTrue(np.isnan(re[1]))
        self.assertTrue(np.isnan(re[2]))

    def test_int_vector(self):
        re = self.s.run("2938 2920 54938 1999 2333")
        self.assertEqual((re == [2938, 2920, 54938, 1999, 2333]).all(), True)
        re = self.s.run("[2938, int(), 6552]")
        expected = [2938.0, np.nan, 6552.0]
        assert_array_almost_equal(re, expected, 1)
        re = self.s.run("[int(), int(), int()]")
        expected = [np.nan, np.nan, np.nan]
        assert_array_almost_equal(re, expected)

    def test_short_vector(self):
        re = self.s.run("[10h, 11h, 12h]")
        expected = [10, 11, 12]
        assert_array_equal(re, expected)
        re = self.s.run("[10h, short(), 12h]")
        expected = [10.0, np.nan, 12.0]
        assert_array_almost_equal(re, expected)
        re = self.s.run("[short(), short(), short()]")
        expected = [np.nan, np.nan, np.nan]
        assert_array_almost_equal(re, expected)
    
    def test_long_vector(self):
        re = self.s.run("[10l, 11l, 12l]")
        expected = [10, 11, 12]
        assert_array_equal(re, expected)
        re = self.s.run("[10l, long(), 12l]")
        expected = [10.0, np.nan, 12.0]
        assert_array_almost_equal(re, expected)
        re = self.s.run("[long(), long(), long()]")
        expected = [np.nan, np.nan, np.nan]
        assert_array_almost_equal(re, expected)

    def test_double_vector(self):
        re = self.s.run("rand(10.0,10)")
        self.assertEqual(len(re), 10)
        re = self.s.run("[12.5, 26.0, double()]")
        expected = [12.5, 26.0, np.nan]
        assert_array_almost_equal(re, expected)
        re = self.s.run("[double(), double(), double()]")
        expected = [np.nan, np.nan, np.nan]
        assert_array_almost_equal(re, expected)

    def test_float_vector(self):
        re = self.s.run("[12.5f, 26.34f, 25.896f]")
        expected = [12.5, 26.34, 25.896]
        assert_array_almost_equal(re, expected, 3)
        re = self.s.run("[12.5f, float(), 25.896f]")
        expected = [12.5, np.nan, 25.896]
        assert_array_almost_equal(re, expected, 3)
        re = self.s.run("[float(), float(), float()]")
        expected = [np.nan, np.nan, np.nan]
        assert_array_almost_equal(re, expected)

    def test_date_vector(self):
        re = self.s.run("2012.10.01 +1..3")
        expected = np.array(['2012-10-02','2012-10-03','2012-10-04'], dtype="datetime64")
        self.assertEqual((re == expected).all(), True)
        re = self.s.run("[2012.06.01, date(), 2012.06.03]")
        expected = np.array(['2012-06-01', 'NaT', '2012-06-03'], dtype="datetime64")
        assert_array_equal(re, expected)
        re = self.s.run("[date(), date(), date()]")
        expected = [np.datetime64('NaT'), np.datetime64('NaT'), np.datetime64('NaT')]
        assert_array_equal(re, expected)

    def test_month_vector(self):
        re = self.s.run("[2012.06M, 2012.07M, 2012.08M]")
        expected = [np.datetime64('2012-06'), np.datetime64('2012-07'), np.datetime64('2012-08')]
        assert_array_equal(re, expected)
        re = self.s.run("[2012.06M, month(), 2012.08M]")
        expected = [np.datetime64('2012-06'), np.datetime64('NaT'), np.datetime64('2012-08')]
        assert_array_equal(re, expected)
        re = self.s.run("take(month(), 3)")
        expected = [np.datetime64('NaT'), np.datetime64('NaT'), np.datetime64('NaT')]
        assert_array_equal(re, expected)

    def test_time_vector(self):
        re = self.s.run("[12:30:10.008, 12:30:10.009, 12:30:10.010]")
        expected = [np.datetime64('1970-01-01T12:30:10.008'), np.datetime64('1970-01-01T12:30:10.009'), np.datetime64('1970-01-01T12:30:10.010')]
        assert_array_equal(re, expected)
        re = self.s.run("[12:30:10.008, NULL, 12:30:10.010]")
        expected = [np.datetime64('1970-01-01T12:30:10.008'), np.datetime64('NaT'), np.datetime64('1970-01-01T12:30:10.010')]
        assert_array_equal(re, expected)
        re = self.s.run("take(time(), 3)")
        expected = [np.datetime64('NaT'), np.datetime64('NaT'), np.datetime64('NaT')]
        assert_array_equal(re, expected)
    
    def test_minute_vector(self):
        re = self.s.run("[13:30m, 13:34m, 13:35m]")
        expected = [np.datetime64('1970-01-01T13:30'), np.datetime64('1970-01-01T13:34'), np.datetime64('1970-01-01T13:35')]
        assert_array_equal(re, expected)
        re = self.s.run("[13:30m, minute(), 13:35m]")
        expected = [np.datetime64('1970-01-01T13:30'), np.datetime64('NaT'), np.datetime64('1970-01-01T13:35')]
        assert_array_equal(re, expected)
        re = self.s.run("take(minute(), 3)")
        expected = [np.datetime64('NaT'), np.datetime64('NaT'), np.datetime64('NaT')]
        assert_array_equal(re, expected)
    
    def test_second_vector(self):
        re = self.s.run("[13:30:10, 13:30:11, 13:30:12]")
        expected = [np.datetime64('1970-01-01T13:30:10'), np.datetime64('1970-01-01T13:30:11'), np.datetime64('1970-01-01T13:30:12')]
        assert_array_equal(re, expected)
        re = self.s.run("[13:30:10, second(), 13:30:12]")
        expected = [np.datetime64('1970-01-01T13:30:10'), np.datetime64('NaT'), np.datetime64('1970-01-01T13:30:12')]
        assert_array_equal(re, expected)
        re = self.s.run("take(second(), 3)")
        expected = [np.datetime64('NaT'), np.datetime64('NaT'), np.datetime64('NaT')]
        assert_array_equal(re, expected)
    
    def test_datetime_vector(self):
        re = self.s.run("2012.10.01T15:00:04 + 2009..2011")
        expected = np.array(['2012-10-01T15:33:33', '2012-10-01T15:33:34', '2012-10-01T15:33:35'], dtype="datetime64")
        self.assertEqual((re == expected).all(), True)
        re = self.s.run("[2012.06.01T12:30:00, datetime(), 2012.06.02T12:30:00]")
        expected = np.array(['2012-06-01T12:30:00', 'NaT', '2012-06-02T12:30:00'], dtype="datetime64")
        assert_array_equal(re, expected)

    def test_timestamp_vector(self):
        re = self.s.run("[2012.06.13T13:30:10.008, 2012.06.13T13:30:10.009, 2012.06.13T13:30:10.010]")
        expected = [np.datetime64('2012-06-13T13:30:10.008'), np.datetime64('2012-06-13T13:30:10.009'), np.datetime64('2012-06-13T13:30:10.010')]
        assert_array_equal(re, expected)
        re = self.s.run("[2012.06.13T13:30:10.008, NULL, 2012.06.13T13:30:10.010]")
        expected = [np.datetime64('2012-06-13T13:30:10.008'), np.datetime64('NaT'), np.datetime64('2012-06-13T13:30:10.010')]
        assert_array_equal(re, expected)
        re = self.s.run("take(timestamp(), 3)")
        expected = [np.datetime64('NaT'), np.datetime64('NaT'), np.datetime64('NaT')]
        assert_array_equal(re, expected)

    def test_nanotime_vector(self):
        re = self.s.run("[13:30:10.008007006, 13:30:10.008007007, 13:30:10.008007008]")
        expected = [np.datetime64('1970-01-01T13:30:10.008007006'), np.datetime64('1970-01-01T13:30:10.008007007'), np.datetime64('1970-01-01T13:30:10.008007008')]
        assert_array_equal(re, expected)
        re = self.s.run("[13:30:10.008007006, NULL, 13:30:10.008007008]")
        expected = [np.datetime64('1970-01-01T13:30:10.008007006'), np.datetime64('NaT'), np.datetime64('1970-01-01T13:30:10.008007008')]
        assert_array_equal(re, expected)
        re = self.s.run("take(nanotime(), 3)")
        expected = [np.datetime64('NaT'), np.datetime64('NaT'), np.datetime64('NaT')]
        assert_array_equal(re, expected)
    
    def test_nanotimestamp_vector(self):
        re = self.s.run("[2012.06.13T13:30:10.008007006, 2012.06.13T13:30:10.008007007, 2012.06.13T13:30:10.008007008]")
        expected = [np.datetime64('2012-06-13T13:30:10.008007006'), np.datetime64('2012-06-13T13:30:10.008007007'), np.datetime64('2012-06-13T13:30:10.008007008')]
        assert_array_equal(re, expected)
        re = self.s.run("[2012.06.13T13:30:10.008007006, NULL, 2012.06.13T13:30:10.008007008]")
        expected = [np.datetime64('2012-06-13T13:30:10.008007006'), np.datetime64('NaT'), np.datetime64('2012-06-13T13:30:10.008007008')]
        assert_array_equal(re, expected)
        re = self.s.run("take(nanotimestamp(), 3)")
        expected = [np.datetime64('NaT'), np.datetime64('NaT'), np.datetime64('NaT')]
        assert_array_equal(re, expected)
    
    def test_uuid_vector(self):
        re = self.s.run("uuid(['5d212a78-cc48-e3b1-4235-b4d91473ee87', '5d212a78-cc48-e3b1-4235-b4d91473ee88', '5d212a78-cc48-e3b1-4235-b4d91473ee89'])")
        expected = ['5d212a78-cc48-e3b1-4235-b4d91473ee87', '5d212a78-cc48-e3b1-4235-b4d91473ee88', '5d212a78-cc48-e3b1-4235-b4d91473ee89']
        assert_array_equal(re, expected)
        re = self.s.run("uuid(['5d212a78-cc48-e3b1-4235-b4d91473ee87', '', '5d212a78-cc48-e3b1-4235-b4d91473ee89'])")
        expected = ['5d212a78-cc48-e3b1-4235-b4d91473ee87', '00000000-0000-0000-0000-000000000000', '5d212a78-cc48-e3b1-4235-b4d91473ee89']
        assert_array_equal(re, expected)
        re = self.s.run("uuid(['', '', ''])")
        expected = ['00000000-0000-0000-0000-000000000000', '00000000-0000-0000-0000-000000000000', '00000000-0000-0000-0000-000000000000']
        assert_array_equal(re, expected)
    
    def test_ipaddr_vector(self):
        re = self.s.run("ipaddr(['192.168.1.135', '192.168.1.124', '192.168.1.14'])")
        expected = ['192.168.1.135', '192.168.1.124', '192.168.1.14']
        assert_array_equal(re, expected)
        re = self.s.run("ipaddr(['192.168.1.135', '', '192.168.1.14'])")
        expected = ['192.168.1.135', '0.0.0.0', '192.168.1.14']
        assert_array_equal(re, expected)
        re = self.s.run("ipaddr(['', '', ''])")
        expected = ['0.0.0.0', '0.0.0.0', '0.0.0.0']
        assert_array_equal(re, expected)
    
    def test_int128_vector(self):
        re = self.s.run("int128(['e1671797c52e15f763380b45e841ec32', 'e1671797c52e15f763380b45e841ec33', 'e1671797c52e15f763380b45e841ec34'])")
        expected = ['e1671797c52e15f763380b45e841ec32', 'e1671797c52e15f763380b45e841ec33', 'e1671797c52e15f763380b45e841ec34']
        assert_array_equal(re, expected)
        re = self.s.run("int128(['e1671797c52e15f763380b45e841ec32', '', 'e1671797c52e15f763380b45e841ec34'])")
        expected = ['e1671797c52e15f763380b45e841ec32', '00000000000000000000000000000000', 'e1671797c52e15f763380b45e841ec34']
        assert_array_equal(re, expected)
        re = self.s.run("int128(['', '', ''])")
        expected = ['00000000000000000000000000000000', '00000000000000000000000000000000', '00000000000000000000000000000000']
        assert_array_equal(re, expected)
        
    def test_int_matrix(self):
        re = self.s.run("1..6$3:2")
        expected = np.array([[1, 4], [2, 5], [3, 6]])
        assert_array_equal(re[0], expected)
        self.assertIsNone(re[1])
        self.assertIsNone(re[2])

    def test_short_matrix(self):
        re = self.s.run("short(1..6)$3:2")
        expected = np.array([[1, 4], [2, 5], [3, 6]])
        assert_array_equal(re[0], expected)
        self.assertIsNone(re[1])
        self.assertIsNone(re[2])

    def test_long_matrix(self):
        re = self.s.run("long(1..6)$3:2")
        expected = np.array([[1, 4], [2, 5], [3, 6]])
        assert_array_equal(re[0], expected)
        self.assertIsNone(re[1])
        self.assertIsNone(re[2])

    def test_double_matrix(self):
        re = self.s.run("[1.1, 1.2, 1.3, 1.4, 1.5, 1.6]$3:2")
        expected = [[1.1, 1.4], [1.2, 1.5], [1.3, 1.6]]
        assert_array_almost_equal(re[0], expected)
        self.assertIsNone(re[1])
        self.assertIsNone(re[2])
    
    def test_float_matrix(self):
        re = self.s.run("[1.1f, 1.2f, 1.3f, 1.4f, 1.5f, 1.6f]$3:2")
        expected = [[1.1, 1.4], [1.2, 1.5], [1.3, 1.6]]
        assert_array_almost_equal(re[0], expected)
        self.assertIsNone(re[1])
        self.assertIsNone(re[2])

    def test_symbol_matrix(self):
        re = self.s.run('symbol("A"+string(1..9))$3:3')
        expected = np.array([["A1","A4","A7"], ["A2","A5","A8"], ["A3","A6","A9"]])
        assert_array_equal(re[0], expected)
        self.assertIsNone(re[1])
        self.assertIsNone(re[2])

    def test_huge_matrix(self):
        re = self.s.run('matrix(loop(take{, 3000}, 1..3000))')
        expected = np.arange(1, 3001)
        for i in np.arange(0, 3000):
            assert_array_equal(re[0][i], expected)

        self.assertIsNone(re[1])
        self.assertIsNone(re[2])

    def test_one_column_matrix(self):
        re = self.s.run('matrix(1..3000000)')
        for i in np.arange(0, 3000000):
            assert_array_equal(re[0][i], [i+1])
        
        self.assertIsNone(re[1])
        self.assertIsNone(re[2])
    
    def test_one_row_matrix(self):
        re = self.s.run("matrix(take(1, 5000)).transpose()")
        assert_array_equal(re[0], [np.repeat(1, 5000)])
        self.assertIsNone(re[1])
        self.assertIsNone(re[2])

    def test_zero_column_matrix(self):
        re = self.s.run("matrix(INT, 3, 0)")
        expected = [[] for i in range(3)]
        assert_array_equal(re[0], expected)
        re = self.s.run("matrix(BOOL,3,0)")
        assert_array_equal(re[0], expected)
        self.assertEqual(re[0].dtype,bool )
        re = self.s.run("matrix(CHAR,3,0)")
        assert_array_equal(re[0], expected)
        self.assertEqual(re[0].dtype,'int8')
        re = self.s.run("matrix(SHORT,3,0)")
        assert_array_equal(re[0], expected)
        self.assertEqual(re[0].dtype,'int16')
        re = self.s.run("matrix(LONG,3,0)")
        assert_array_equal(re[0], expected)
        self.assertEqual(re[0].dtype,'int64')
        re = self.s.run("matrix(DATE,3,0)")
        expected = np.empty((3,0),dtype="datetime64[ns]")
        assert_array_equal(re[0], expected)
        self.assertEqual(re[0].dtype,'datetime64[ns]')
        re = self.s.run("matrix(MONTH,3,0)")
        expected = np.empty((3,0),dtype="datetime64[M]")
        assert_array_equal(re[0], expected)
        self.assertEqual(re[0].dtype,'datetime64[M]')
        re = self.s.run("matrix(TIME,3,0)")
        expected = np.empty((3,0),dtype="datetime64[ns]")
        assert_array_equal(re[0], expected)
        self.assertEqual(re[0].dtype,'datetime64[ns]')
        re = self.s.run("matrix(MINUTE,3,0)")
        expected = np.empty((3,0),dtype="datetime64[ns]")
        assert_array_equal(re[0], expected)
        self.assertEqual(re[0].dtype,'datetime64[ns]')
        re = self.s.run("matrix(SECOND,3,0)")
        expected = np.empty((3,0),dtype="datetime64[ns]")
        assert_array_equal(re[0], expected)
        re = self.s.run("matrix(DATETIME,3,0)")
        expected = np.empty((3,0),dtype="datetime64[ns]")
        assert_array_equal(re[0], expected)
        self.assertEqual(re[0].dtype,'datetime64[ns]')
        re = self.s.run("matrix(TIMESTAMP,3,0)")
        assert_array_equal(re[0], expected)
        self.assertEqual(re[0].dtype,'datetime64[ns]')
        re = self.s.run("matrix(NANOTIME,3,0)")
        assert_array_equal(re[0], expected)
        re = self.s.run("matrix(NANOTIMESTAMP,3,0)")
        assert_array_equal(re[0], expected)
        re = self.s.run("matrix(FLOAT,3,0)")
        expected = np.empty((3,0),dtype="float32")
        assert_array_equal(re[0], expected)
        re = self.s.run("matrix(DOUBLE,3,0)")
        assert_array_equal(re[0], expected)
        self.assertEqual(re[0].dtype,"float64")
        re = self.s.run("matrix(SYMBOL,3,0)")
        assert_array_equal(re[0], expected)


    def test_zero_row_matrix(self):
        re = self.s.run("matrix(INT, 0, 3)")
        expected = np.empty((0,3),dtype="int32")
        assert_array_equal(re[0], expected)
        re = self.s.run("matrix(BOOL,0,3)")
        expected = np.empty((0,3),dtype="bool")
        assert_array_equal(re[0], expected)
        re = self.s.run("matrix(CHAR,0,3)")
        assert_array_equal(re[0], expected)
        re = self.s.run("matrix(SHORT,0,3)")
        assert_array_equal(re[0], expected)
        re = self.s.run("matrix(LONG,0,3)")
        assert_array_equal(re[0], expected)
        re = self.s.run("matrix(DATE,0,3)")
        expected = np.empty((0,3),dtype="datetime64[ns]")
        assert_array_equal(re[0], expected)
        re = self.s.run("matrix(MONTH,0,3)")
        assert_array_equal(re[0], expected)
        re = self.s.run("matrix(TIME,0,3)")
        assert_array_equal(re[0], expected)
        re = self.s.run("matrix(MINUTE,0,3)")
        assert_array_equal(re[0], expected)
        re = self.s.run("matrix(SECOND,0,3)")
        assert_array_equal(re[0], expected)
        re = self.s.run("matrix(DATETIME,0,3)")
        assert_array_equal(re[0], expected)
        re = self.s.run("matrix(TIMESTAMP,0,3)")
        assert_array_equal(re[0], expected)
        re = self.s.run("matrix(NANOTIME,0,3)")
        assert_array_equal(re[0], expected)
        re = self.s.run("matrix(NANOTIMESTAMP,0,3)")
        assert_array_equal(re[0], expected)
        re = self.s.run("matrix(FLOAT,0,3)")
        expected = np.empty((0,3),dtype="float32")
        assert_array_equal(re[0], expected)
        re = self.s.run("matrix(DOUBLE,0,3)")
        assert_array_equal(re[0], expected)
        re = self.s.run("matrix(SYMBOL,0,3)")
        assert_array_equal(re[0], expected)

    def test_all_null_matrix(self):
        re = self.s.run("take(int(), 12)$3:4")
        expected=[[np.NaN, np.NaN, np.NaN, np.NaN], [np.NaN, np.NaN, np.NaN, np.NaN], [np.NaN, np.NaN, np.NaN, np.NaN]]
        assert_array_equal(re[0], expected)

        re = self.s.run("[1, 2, NULL, 3, NULL, 4]$2:3")
        expected=[[1, np.NaN, np.NaN], [2., 3., 4.]]
        assert_array_equal(re[0], expected)

        re = self.s.run("symbol(take(string(), 12))$3:4")
        assert_array_equal(re[0][0], ['','','',''])
        assert_array_equal(re[0][1], ['','','',''])
        assert_array_equal(re[0][2], ['','','',''])

        re = self.s.run("symbol(['AA', 'BB', NULL, 'CC', NULL, 'DD'])$2:3")
        assert_array_equal(re[0][0], ['AA','',''])
        assert_array_equal(re[0][1], ['BB','CC','DD'])


    def test_huge_symbol_matrix(self):
        re = self.s.run("m = symbol(string(1..1000000))$200:5000;m.rename!(1..200,1..5000);m")
        assert_array_equal(re[1], np.arange(1, 201))
        assert_array_equal(re[2], np.arange(1, 5001))
        
        re = self.s.run("m = symbol(string(1..1000000))$200:5000;m.rename!(1..200,1..5000);table(m.rowNames() as label, m)")
        assert_array_equal(re["label"], np.arange(1, 201))
        j=1
        for i in np.arange(1, 5001):
            assert_series_equal(re.iloc[:,i], pd.Series([str(x) for x in np.arange(j, j+200)], index=np.arange(0, 200)),check_names=False)
            j+=200

    def test_int_matrix_with_label(self):
        re = self.s.run("cross(add,1..5,1..10)")
        expected = np.array(
            [[2, 3, 4, 5, 6, 7, 8, 9, 10, 11], [3, 4, 5, 6, 7, 8, 9, 10, 11, 12], [4, 5, 6, 7, 8, 9, 10, 11, 12, 13],
             [5, 6, 7, 8, 9, 10, 11, 12, 13, 14], [6, 7, 8, 9, 10, 11, 12, 13, 14, 15]])
        #self.assertEqual((re == expected).all(), True)
        assert_array_equal(re[0], expected)
        assert_array_equal(re[1], np.array([1, 2, 3, 4, 5]))
        assert_array_equal(re[2], np.array([1, 2, 3, 4, 5, 6, 7, 8, 9, 10]))
    
    def test_matrix_only_with_row_label(self):
        re = self.s.run("m=1..6$3:2;m.rename!([0, 1, 2],);m")
        expected = [[1, 4], [2, 5], [3, 6]]
        assert_array_equal(re[0], expected)
        assert_array_equal(re[1], [0, 1, 2])
        self.assertIsNone(re[2])

    def test_matrix_only_with_col_label(self):
        re = self.s.run("m=1..6$3:2;m.rename!([0, 1]);m")
        expected = [[1, 4], [2, 5], [3, 6]]
        assert_array_equal(re[0], expected)
        self.assertIsNone(re[1])
        assert_array_equal(re[2], [0, 1])
    
    def test_matrix_label_date_symbol(self):
        script='''
        m=matrix([2200, 1300, 2500, 8800], [6800, 5400, NULL, NULL], [1900, 2100, 3200, NULL]).rename!(2012.01.01..2012.01.04, symbol(`C`IBM`MS));
        m
        '''
        re = self.s.run(script)
        expected=[[2200., 6800.,1900.],[1300.,5400.,2100.],[2500.,np.NaN,3200.],[8800.,np.NaN, np.NaN]]
        assert_array_almost_equal(re[0], expected)
        assert_array_equal(re[1], np.array(['2012-01-01T00:00:00.000000000', '2012-01-02T00:00:00.000000000','2012-01-03T00:00:00.000000000', '2012-01-04T00:00:00.000000000'], dtype="datetime64"))
        assert_array_equal(re[2], ['C', 'IBM', 'MS'])

    def test_matrix_label_second_symbol(self):
        script='''
        m=matrix([2200, 1300, 2500, 8800], [6800, 5400, NULL, NULL], [1900, 2100, 3200, NULL]).rename!([09:30:00, 10:00:00, 10:30:00, 11:00:00], `C`IBM`MS)
        m
        '''
        re = self.s.run(script)
        expected=[[2200., 6800.,1900.],[1300.,5400.,2100.],[2500.,np.NaN,3200.],[8800.,np.NaN, np.NaN]]
        assert_array_almost_equal(re[0], expected)
        assert_array_equal(re[1], np.array(['1970-01-01T09:30:00.000000000', '1970-01-01T10:00:00.000000000','1970-01-01T10:30:00.000000000', '1970-01-01T11:00:00.000000000'], dtype="datetime64"))
        assert_array_equal(re[2], ['C', 'IBM', 'MS'])
    
    def test_matrix_label_symbol_date(self):
        script='''
        m=matrix([2200, 1300, 2500, 8800], [6800, 5400, NULL, NULL], [1900, 2100, 3200, NULL]).rename!(`C`IBM`MS`ZZ, 2012.01.01..2012.01.03)
        m
        '''
        re = self.s.run(script)
        expected=[[2200., 6800.,1900.],[1300.,5400.,2100.],[2500.,np.NaN,3200.],[8800.,np.NaN, np.NaN]]
        assert_array_almost_equal(re[0], expected)
        assert_array_equal(re[1], ['C', 'IBM', 'MS', 'ZZ'])
        assert_array_equal(re[2], np.array(['2012-01-01T00:00:00.000000000', '2012-01-02T00:00:00.000000000',
       '2012-01-03T00:00:00.000000000'],dtype="datetime64"))




    def test_table(self):
        script = '''n=20;
		syms=`IBM`C`MS`MSFT`JPM`ORCL`BIDU`SOHU`GE`EBAY`GOOG`FORD`GS`PEP`USO`GLD`GDX`EEM`FXI`SLV`SINA`BAC`AAPL`PALL`YHOO`KOH`TSLA`CS`CISO`SUN;
		mytrades=table(09:30:00+rand(18000,n) as timestamp,rand(syms,n) as sym, 10*(1+rand(100,n)) as qty,5.0+rand(100.0,n) as price);
		select qty,price from mytrades where sym==`IBM;'''
        re = self.s.run(script)
        self.assertEqual(re.shape[1], 2)

    def test_dictionary(self):
        script = '''dict(1 2 3,`IBM`MSFT`GOOG)'''
        re = self.s.run(script)
        expected = {2: 'MSFT', 3: 'GOOG', 1: 'IBM'}
        self.assertDictEqual(re, expected)

    def test_any_vector(self):
        re = self.s.run("([1], [2],[1,3, 5],[0.9, 0.8])")
        self.assertEqual((re[0] == [1]).all(), True)
        self.assertEqual((re[1] == [2]).all(), True)
        self.assertEqual((re[2] == [1, 3, 5]).all(), True)

    def test_set(self):
        re = self.s.run("set(1+3*1..3)")
        self.assertSetEqual(re, {10, 4, 7})

    def test_pair(self):
        re = self.s.run("3:4")
        self.assertListEqual(re, list([3, 4]))

    def test_any_dictionary(self):
        re = self.s.run("{a:1,b:2}")
        expected = {'a': 1, 'b': 2}
        self.assertDictEqual(re, expected)

    def test_upload_matrix(self):
        a = self.s.run("cross(+, 1..5, 1..5)")
        b = self.s.run("1..25$5:5")
        self.s.upload({'a': a, 'b': b})
        re = self.s.run('a+b')
        # print(re)
        # self.assertEqual((re[0] == [3, 9, 15, 21, 27]).all(), True)
        # self.assertEqual((re[1] == [5, 11, 17, 23, 29]).all(), True)
        # self.assertEqual((re[2] == [7, 13, 19, 25, 31]).all(), True)
        # self.assertEqual((re[3] == [9, 15, 21, 27, 33]).all(), True)
        # self.assertEqual((re[4] == [11, 17, 23, 29, 35]).all(), True)

    def test_run_plot(self):
        script = '''
                x=1..10
                t = table(x as sin, x+100 as cos)
                plot(t)
        '''
        re = self.s.run(script)
        assert_array_equal(re['data'][0], np.array([[1, 101], [2, 102], [3, 103], [4, 104], [5, 105], [6, 106], [7, 107], [8, 108], [9, 109], [10, 110]]))
        self.assertIsNone(re['data'][1])
        assert_array_equal(re['data'][2], np.array(['sin', 'cos']))
        assert_array_equal(re['title'], np.array(['', '', '']))

    def test_table_datatypes(self):
        script='''
        n = 200
        a = 100
        v1 = string(1..n)
        v2 = string(1..n)
        v3 = take(int128("fcc69bca9885b51962660c23d08c124a"),n-a).join(take(int128("a428d55098d8e41e8adc4b7d04d8ede1"),a))
        v4 = take(uuid("407c628e-d319-25c1-17ee-e5a73500a010"),n-a).join(take(uuid("d7a39280-1b18-8f56-160c-beabd428c934"),a))
        v5 = take(ipaddr("4139:719:4233:fce:61a2:438b:4ff6:970b"),n-a).join(take(ipaddr("349a:93a4:c11:d8ae:8ba5:48a6:dc81:20d7"),a))
        t = table(n:n,`val1`val2`val3`val4`val5,[SYMBOL,STRING,INT128,UUID,IPADDR])
        t[`val1] = v1
        t[`val2] = v2
        t[`val3] = v3
        t[`val4] = v4
        t[`val5] = v5
        '''
        self.s.run(script)
        df1 = self.s.run("select val1 from t")
        df2 =  self.s.run("select val2 from t")
        df3 = self.s.run("select val3 from t")
        df4 =  self.s.run("select val4 from t")
        df5 = self.s.run("select val5 from t")
        df = self.s.run("select * from t")
        n = 200
        a = 100
        data1 = np.array(range(1,n+1),dtype="str")
        data2 = np.append(np.repeat("fcc69bca9885b51962660c23d08c124a",n-a),np.repeat("a428d55098d8e41e8adc4b7d04d8ede1",a))
        data3 = np.append(np.repeat("407c628e-d319-25c1-17ee-e5a73500a010",n-a),np.repeat("d7a39280-1b18-8f56-160c-beabd428c934",a))
        data4 = np.append(np.repeat("4139:719:4233:fce:61a2:438b:4ff6:970b",n-a),np.repeat("349a:93a4:c11:d8ae:8ba5:48a6:dc81:20d7",a))
        ex1 = pd.DataFrame({"val1":data1})
        ex2 = pd.DataFrame({"val2":data1})
        ex3 = pd.DataFrame({"val3":data2})
        ex4 = pd.DataFrame({"val4":data3})
        ex5 = pd.DataFrame({"val5":data4})
        ex = pd.DataFrame({"val1":data1,"val2":data1,"val3":data2,"val4":data3,"val5":data4})
        assert_frame_equal(df1, ex1)
        assert_frame_equal(df2, ex2)
        assert_frame_equal(df3, ex3)
        assert_frame_equal(df4, ex4)
        assert_frame_equal(df5, ex5)
        assert_frame_equal(df, ex)
    
    def test_table_datatypes_with_null(self):
        script='''
        n = 100
        a = 50
        v1=string(1..(n-a)).join(take(string(),a))
        v2 = v1
        v3 = take(int128("fcc69bca9885b51962660c23d08c124a"),n-a).join(take(int128(),a))
        v4 = take(uuid("407c628e-d319-25c1-17ee-e5a73500a010"),n-a).join(take(uuid(),a))
        v5 = take(ipaddr("4139:719:4233:fce:61a2:438b:4ff6:970b"),n-a).join(take(ipaddr(),a))
        t = table(n:n,`val1`val2`val3`val4`val5,[SYMBOL,STRING,INT128,UUID,IPADDR])
        t[`val1] = v1
        t[`val2] = v2
        t[`val3] = v3
        t[`val4] = v4
        t[`val5] = v5
        '''
        self.s.run(script)
        df1 = self.s.run("select val1 from t")
        df2 =  self.s.run("select val2 from t")
        df3 =  self.s.run("select val3 from t")
        df4 =  self.s.run("select val4 from t")
        df5 = self.s.run("select val5 from t")
        df = self.s.run("select * from t")
        n = 100
        a = 50
        arr1 = np.append(np.array(range(1,n-a+1),dtype="str"),np.repeat("",a))
        arr2 = np.append(np.repeat("fcc69bca9885b51962660c23d08c124a",n-a),np.repeat("00000000000000000000000000000000",a))
        arr3 = np.append(np.repeat("407c628e-d319-25c1-17ee-e5a73500a010",n-a),np.repeat("00000000-0000-0000-0000-000000000000",a))
        arr4 = np.append(np.repeat("4139:719:4233:fce:61a2:438b:4ff6:970b",n-a),np.repeat("0.0.0.0",a))
        ex1 = pd.DataFrame(arr1,columns=["val1"])
        ex2 = pd.DataFrame(arr1,columns=["val2"])  
        ex3 = pd.DataFrame(arr2,columns=["val3"])
        ex4 = pd.DataFrame(arr3,columns=["val4"])  
        ex5 = pd.DataFrame(arr4,columns=["val5"])  
        ex = pd.DataFrame({"val1":arr1,"val2":arr1,"val3":arr2,"val4":arr3,"val5":arr4})
        assert_frame_equal(df1, ex1)
        assert_frame_equal(df2, ex2)
        assert_frame_equal(df3, ex3)
        assert_frame_equal(df4, ex4)
        assert_frame_equal(df5, ex5)
        assert_frame_equal(df, ex)
     
    def test_table_datatypes_all_null(self):
        script='''
        n = 10000 
        t = table(n:n,`val1`val2`val3`val4`val5,[SYMBOL,STRING,INT128,UUID,IPADDR])
        '''
        self.s.run(script)
        df1 = self.s.run("select val1 from t")
        df2 =  self.s.run("select val2 from t")
        df3 = self.s.run("select val3 from t")
        df4 =  self.s.run("select val4 from t")
        df5 = self.s.run("select val5 from t")
        df = self.s.run("select * from t")
        data = np.repeat("",10000)
        data1 = np.repeat("00000000000000000000000000000000",10000)
        data2 = np.repeat("00000000-0000-0000-0000-000000000000",10000)
        data3 =  np.repeat("0.0.0.0",10000)
        ex1 = pd.DataFrame({"val1":data})
        ex2 = pd.DataFrame({"val2":data})
        ex3 = pd.DataFrame({"val3":data1})
        ex4 = pd.DataFrame({"val4":data2})
        ex5 = pd.DataFrame({"val5":data3})
        ex = pd.DataFrame({"val1":data,"val2":data,"val3":data1,"val4":data2,"val5":data3})
        assert_frame_equal(df1, ex1)
        assert_frame_equal(df2, ex2)
        assert_frame_equal(df3, ex3)
        assert_frame_equal(df4, ex4)
        assert_frame_equal(df5, ex5)
        assert_frame_equal(df, ex)

    def test_table_datatypes_big_data(self):
        script='''
        n = 2000000
        a = 1000000
        v1 = string(1..n)
        v2 = string(1..n)
        v3 = take(int128("fcc69bca9885b51962660c23d08c124a"),n-a).join(take(int128("a428d55098d8e41e8adc4b7d04d8ede1"),a))
        v4 = take(uuid("407c628e-d319-25c1-17ee-e5a73500a010"),n-a).join(take(uuid("d7a39280-1b18-8f56-160c-beabd428c934"),a))
        v5 = take(ipaddr("4139:719:4233:fce:61a2:438b:4ff6:970b"),n-a).join(take(ipaddr("349a:93a4:c11:d8ae:8ba5:48a6:dc81:20d7"),a))
        t = table(n:n,`val1`val2`val3`val4`val5,[SYMBOL,STRING,INT128,UUID,IPADDR])
        t[`val1] = v1
        t[`val2] = v2
        t[`val3] = v3
        t[`val4] = v4
        t[`val5] = v5
        '''
        self.s.run(script)
        df1 = self.s.run("select val1 from t")
        df2 =  self.s.run("select val2 from t")
        df3 = self.s.run("select val3 from t")
        df4 =  self.s.run("select val4 from t")
        df5 = self.s.run("select val5 from t")
        df = self.s.run("select * from t")
        n = 2000000
        a = 1000000
        data1 = np.array(range(1,n+1),dtype="str")
        data2 = np.append(np.repeat("fcc69bca9885b51962660c23d08c124a",n-a),np.repeat("a428d55098d8e41e8adc4b7d04d8ede1",a))
        data3 = np.append(np.repeat("407c628e-d319-25c1-17ee-e5a73500a010",n-a),np.repeat("d7a39280-1b18-8f56-160c-beabd428c934",a))
        data4 = np.append(np.repeat("4139:719:4233:fce:61a2:438b:4ff6:970b",n-a),np.repeat("349a:93a4:c11:d8ae:8ba5:48a6:dc81:20d7",a))
        ex1 = pd.DataFrame({"val1":data1})
        ex2 = pd.DataFrame({"val2":data1})
        ex3 = pd.DataFrame({"val3":data2})
        ex4 = pd.DataFrame({"val4":data3})
        ex5 = pd.DataFrame({"val5":data4})
        ex = pd.DataFrame({"val1":data1,"val2":data1,"val3":data2,"val4":data3,"val5":data4})
        assert_frame_equal(df1, ex1)
        assert_frame_equal(df2, ex2)
        assert_frame_equal(df3, ex3)
        assert_frame_equal(df4, ex4)
        assert_frame_equal(df5, ex5)
        assert_frame_equal(df, ex)

    def test_table_zero_row(self):
        CREATE_TABLE = """
        if(existsDatabase("dfs://ticks")){
            dropDatabase("dfs://ticks")
        }
        ticks_db = database("dfs://ticks", VALUE, 2020.06.23..2020.06.24)
        ticks_table = table(
            array(DATE, 0) as Date,
            array(TIME, 0) as Time,
            array(SYMBOL, 0) as Code,
            array(DOUBLE, 0) as Price,
            array(LONG, 0) as Volume,
            array(DOUBLE, 0) as AskPrice,
            array(LONG, 0) as AskVolume,
            array(DOUBLE, 0) as BidPrice,
            array(LONG, 0) as BidVolume
        )
        ticks_db.createPartitionedTable(ticks_table, `ticks_table, `Date)
        """
        re = self.s.run(CREATE_TABLE)
        expected = pd.DataFrame(columns=['Date','Time','Code','Price','Volume','AskPrice','AskVolume','BidPrice','BidVolume'])
        expected['Date'] = expected[['Date']].astype("datetime64[ns]")
        expected['Time'] = expected[['Time']].astype("datetime64[ns]")
        expected['Code'] = expected[['Code']].astype(object)
        expected['Price'] = expected[['Price']].astype("float64")
        expected['Volume'] = expected[['Volume']].astype("int64")
        expected['AskPrice'] = expected[['AskPrice']].astype("float64")
        expected['AskVolume'] = expected[['AskVolume']].astype("int64")
        expected['BidPrice'] = expected[['BidPrice']].astype("float64")
        expected['BidVolume'] = expected[['BidVolume']].astype("int64")
        re.index=[]
        assert_frame_equal(re, expected)

    def test_table_all_datatypes_zero_row(self):
        script='''
        if(existsDatabase("dfs://ticks")){
            dropDatabase("dfs://ticks")
        }
        ticks_db = database("dfs://ticks", VALUE, 2020.06.23..2020.06.24)
        ticks_table = table(50:0,`val1`val2`val3`val4`val5`val6`val7`val8`val9`val10`val11`val12`val13`val14  `val15`val16`val17`val18`val19`val20`val21,[BOOL,CHAR,SHORT,INT,LONG,DATE,MONTH,TIME,MINUTE,SECOND,DATETIME,TIMESTAMP,
        NANOTIME,NANOTIMESTAMP,FLOAT,DOUBLE,SYMBOL,STRING,UUID,IPADDR,INT128])
        ticks_db.createPartitionedTable(ticks_table, `ticks_table, `val6)
        '''
        re = self.s.run(script)
        re.index=[]
        expected = pd.DataFrame(columns=['val1','val2','val3','val4','val5','val6','val7','val8','val9','val10','val11','val12','val13','val14','val15','val16','val17','val18','val19','val20','val21'])
        dtype = {'val1':'bool','val2':'int8','val3':'int16','val4':'int32','val5':'int64','val6':'datetime64[ns]','val7':'datetime64[ns]','val8':'datetime64[ns]','val9':'datetime64[ns]','val10':'datetime64[ns]','val11':'datetime64[ns]','val12':'datetime64[ns]',
        'val13':'datetime64[ns]','val14':'datetime64[ns]','val15':'float32','val16':'float64','val17':'object','val18':'object','val19':'object','val20':'object','val21':'object'}
        for k, v in dtype.items():
            expected[k] = expected[k].astype(v)
        assert_frame_equal(re, expected)

    def test_time_dtype_4_byte(self):
        self.s.run(
            '''
            n = 500000
            t = table(take(2013.06.13, n) as date,take(2012.06.13T13:30:10,n) as datetime,take(2012.06M,n) as month,take(13:30:10.008,n) as time,take(13:30m,n) as minute,take(13:30:10,n) as second)
            '''
        )
        re = self.s.run("t")
        n = 500000
        ex = pd.DataFrame({"date":np.repeat('2013-06-13',n), "datetime":np.repeat('2012-06-13T13:30:10',n), "month":np.repeat('2012-06',n), "time":np.repeat('1970-01-01T13:30:10.008',n), "minute":np.repeat('1970-01-01T13:30',n), "second":np.repeat('1970-01-01T13:30:10',n)},dtype="datetime64[ns]")
        assert_frame_equal(re, ex)
    
    def test_time_dtype_early_1970(self):
        self.s.run(
            '''
            n = 500000
            t = table(take(1960.01.01,n) as date,take(1960.01M,n) as month,take(1960.01.01 13:30:10,n) as datetime,take(1960.01.01T13:30:10.008,n) as timestamp,take(1960.01.01 13:30:10.008007006,n) as  nanotimestamp)
            '''
        )
        re = self.s.run("t")
        n = 500000
        ex =  pd.DataFrame({"date":np.repeat('1960-01-01',n), "month":np.repeat('1960-01',n), "datetime":np.repeat('1960-01-01T13:30:10',n), "timestamp":np.repeat('1960-01-01T13:30:10.008',n), "nanotimestamp":np.repeat('1960-01-01T13:30:10.008007006',n)},dtype="datetime64[ns]")
        assert_frame_equal(re, ex)
    
    def test_datehour_scalar(self):
        re = self.s.run("datehour(2012.06.13 13:30:10)")
        self.assertEqual(re, np.datetime64('2012-06-13T13','h'))
        re = self.s.run("datehour(1960.06.13 13:30:10)")
        self.assertEqual(re, np.datetime64('1960-06-13T13','h'))
        re = self.s.run("datehour()")
        self.assertIsNone(re)
    
    def test_python_datetime64_dolphindb_datehour_scalar(self):
        ts = np.datetime64('2019-01-01T20:01:01.1223461')
        self.assertEqual(self.s.run('datehour', ts), np.datetime64('2019-01-01T20','h'))
        ts = np.datetime64('1960-01-01T20:01:01.1223461')
        self.assertEqual(self.s.run('datehour', ts), np.datetime64('1960-01-01T20','h'))

    def test_datehour_vector(self):
        re = self.s.run("datehour(2012.10.01T20:01:01) +1..3")
        expected = np.array(['2012-10-01T21','2012-10-01T22','2012-10-01T23'], dtype="datetime64[h]")
        self.assertEqual((re == expected).all(), True)
        re = self.s.run("[datehour(2012.06.01T20:01:01), datehour(), datehour(2012.06.03T20:01:01)]")
        expected = np.array(['2012-06-01T20', 'NaT', '2012-06-03T20'], dtype="datetime64[h]")
        assert_array_equal(re, expected)
        re = self.s.run("[datehour(), datehour(), datehour()]")
        expected = [np.datetime64('NaT'), np.datetime64('NaT'), np.datetime64('NaT')]
        assert_array_equal(re, expected)
        re = self.s.run("v = take(datehour(2012.10.01T20:01:01),500000)")
        re = self.s.run("v")
        expected = np.repeat(np.datetime64('2012-10-01T20','h'),500000)
        assert_array_equal(re, expected)
        re = self.s.run("v = take(datehour(1960.10.01T20:01:01),500000)")
        re = self.s.run("v")
        expected = np.repeat(np.datetime64('1960-10-01T20','h'),500000)
        assert_array_equal(re, expected)

    def test_datehour_table(self):
        self.s.run('''
        n = 500000
        t = table(datehour(2021.06.13T13:30:10)+1..n as datehour ,1..n as id)
        ''')
        re = self.s.run("t")
        n = 500000
        datehour = np.array(np.datetime64("2021-06-13T13")+np.arange(1,n+1))
        id = np.array(np.arange(1,n+1),dtype='int32')
        ex = pd.DataFrame({"datehour":datehour, "id":id})
        assert_frame_equal(re, ex)
        self.s.run('''
        n = 500000
        t = table(take(datehour(),n) as datehour,1..n as id)
        ''')
        re = self.s.run("t")
        datehour = np.array(np.repeat(np.nan,n),dtype="datetime64[h]")
        ex = pd.DataFrame({"datehour":datehour, "id":id})
        assert_frame_equal(re, ex)
        self.s.run(
            '''
            n = 500000
            t = table(take(datehour(2021.06.13T13:30:10),n-10000).join(take(datehour(),10000)) as datehour,1..n as id)
            '''
        )
        re = self.s.run("t")
        n = 500000
        datehour = np.append(np.repeat(np.datetime64("2021-06-13T13"),n-10000),np.repeat(np.datetime64("NaT"),10000))
        ex = pd.DataFrame({"datehour":datehour, "id":id})
        assert_frame_equal(re, ex)
        
if __name__ == '__main__':
    unittest.main()
