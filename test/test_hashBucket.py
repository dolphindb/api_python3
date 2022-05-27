
import unittest
import dolphindb as ddb
import numpy as np
import pandas as pd
from setup import HOST, PORT, WORK_DIR, DATA_DIR
from numpy.testing import assert_array_equal, assert_array_almost_equal

class TestFunctionHashBucket(unittest.TestCase):
    @classmethod
    def setUp(cls):
        cls.s = ddb.session()
        cls.s.connect(HOST, PORT, "admin", "123456")
    @classmethod
    def tearDownClass(cls):
        pass
    
    def test_hashBucket_bucketNum_double(self):
        with self.assertRaises(ValueError):
            self.s.hashBucket("a", 3.5)

    def test_hashBucket_bucketNum_negative(self):
        pass
        # print(self.s.hashBucket("a", -3))

    def test_hashBucket_bucketNum_zero(self):
        pass
        # print(self.s.hashBucket("a", 0))
    
    def test_hashBucket_input_bool(self):
        pass
        # print(self.s.hashBucket([True, False], 3))

    def test_hashBucket_input_int(self):
        re = self.s.hashBucket(np.arange(1, 51), 7)
        expected = self.s.run("hashBucket(1..50, 7)")
        assert_array_equal(re, expected)
        re = self.s.hashBucket([11, 58, 23, 59, np.nan, np.nan], 5)
        expected = self.s.run("hashBucket([11, 58, 23, 59, NULL, NULL], 5)")
        assert_array_equal(re, expected)
        re = self.s.hashBucket(1, 3)
        expected = self.s.run("hashBucket(1, 3)")
        self.assertEqual(re, expected)
        # re = self.s.hashBucket(np.nan, 3)
        # expected = self.s.run("hashBucket(int(), 3)")
        # self.assertEqual(re, expected)

    def test_hashBucket_input_string(self):
        re = self.s.hashBucket(["aaa", "", "ccc"], 3)
        expected = self.s.run('hashBucket(["aaa", "", "ccc"], 3)')
        assert_array_equal(re, expected)
        re = self.s.hashBucket(["A","B","C","D","E","F","G","H","I","J","K","L","M","N","O","P","Q","R","S","T","U","V","W","X","Y","Z"], 7)
        expected = self.s.run('hashBucket(["A","B","C","D","E","F","G","H","I","J","K","L","M","N","O","P","Q","R","S","T","U","V","W","X","Y","Z"], 7)')
        assert_array_equal(re, expected)
        re = self.s.hashBucket("AAPL", 3)
        expected = self.s.run('hashBucket("AAPL", 3)')
        self.assertEqual(re, expected)
        re = self.s.hashBucket("", 5)
        expected = self.s.run('hashBucket("", 5)')
        self.assertEqual(re, expected)
        
    def test_hashBucket_input_date(self):
        # re = self.s.hashBucket(np.array(pd.date_range(start='2012-1-1', end='2012-12-31', freq='D'), dtype='datetime64[D]'), 50)
        re = self.s.hashBucket(np.array(pd.date_range(start='1/1/2012', end='12/31/2012', freq='D'), dtype='datetime64[D]'), 50)
        expected = self.s.run('hashBucket(2012.01.01..2012.12.31, 50)')
        assert_array_equal(re, expected)
        # re = self.s.hashBucket([np.datetime64('2012-06-08', dtype='datetime64[D]'), np.datetime64('NaT', dtype='datetime64[D]'), np.datetime64('2012-06-11', dtype='datetime64[D]')], 5)
        # expected = self.s.run("hashBucket([2012.06.08, NULL, 2012.06.11], 5)")
        # assert_array_equal(re, expected)
        re = self.s.hashBucket(np.datetime64('2013-01-01'), 5)
        # re = self.s.hashBucket(np.datetime64('2013-01-01', dtype='datetime64[D]'), 5)
        expected = self.s.run('hashBucket(2013.01.01, 5)')
        self.assertEqual(re, expected)
        # re = self.s.hashBucket(np.datetime64('NaT', dtype='datetime64[D]'), 5)
        # expected = self.s.run('hashBucket(date(), 5)')
        # self.assertEqual(re, expected)

    def test_hashBucket_input_month(self):
        re = self.s.hashBucket(np.array(pd.date_range(start='2005-1', end='2007-01', freq='M'), dtype='datetime64[M]'), 13)
        expected = self.s.run('hashBucket(2005.01M..2006.12M, 13)')
        assert_array_equal(re, expected)
        # re = self.s.hashBucket(np.array(['2012-06', 'NaT'], dtype='datetime64[M]'), 3)
        # expected = self.s.run('hashBucket([2012.06M, month()], 3)')
        # assert_array_equal(re, expected)
        re = self.s.hashBucket(np.datetime64('2012-06'), 5)
        expected = self.s.run('hashBucket(2012.06M, 5)')
        self.assertEqual(re, expected)
        # re = self.s.hashBucket(np.datetime64('NaT', dtype='datetime64[M]'), 3)
        # expected = self.s.run('hashBucket(month(), 3)')
        # self.assertEqual(re, expected)
        
    def test_hashBucket_input_second(self):
        re = self.s.hashBucket(np.array(pd.date_range(start='2002-12-01T00:25:36', end='2002-12-02T00:25:36', freq='h'), dtype='datetime64[s]'), 7)
        expected = self.s.run("hashBucket(temporalAdd(2002.12.01T00:25:36, 0..24, 'h'), 7)")
        assert_array_equal(re, expected)
        # re = self.s.hashBucket(np.array(['2002-12-01T00:25:36', 'NaT', '2002-12-02T00:25:36'], dtype='datetime64[s]'), 7)
        # expected = self.s.run("hashBucket([2002.12.01T00:25:36, NULL, 2002.12.02T00:25:36], 7)")
        # assert_array_equal(re, expected)
        re = self.s.hashBucket(np.datetime64('2002-12-02T00:25:36'), 3)
        expected = self.s.run("hashBucket(2002.12.02T00:25:36, 3)")
        self.assertEqual(re, expected)
        # re = self.s.hashBucket(np.datetime64('NaT', dtype='datetime64[s]'), 11)
        # expected = self.s.run("hashBucket(datetime(), 11)")
        # self.assertEqual(re, expected)
    
    def test_hashBucket_input_millisecond(self):
        re = self.s.hashBucket(np.array(pd.date_range(start='2002-12-01T00:25:36.009', end='2002-12-02T00:25:36.009', freq='h'), dtype='datetime64[ms]'), 7)
        expected = self.s.run("hashBucket(temporalAdd(2002.12.01T00:25:36.009, 0..24, 'h'), 7)")
        assert_array_equal(re, expected)
        # re = self.s.hashBucket(np.array(['2002-12-01T00:25:36.009', 'NaT', '2002-12-02T00:25:36.825'], dtype='datetime64[ms]'), 3)
        # expected = self.s.run("hashBucket([2002.12.01T00:25:36.009, NULL, 2002.12.02T00:25:36.825], 3)")
        # assert_array_equal(re, expected)
        re = self.s.hashBucket(np.datetime64('2002-12-01T00:25:36.009'), 7)
        expected = self.s.run("hashBucket(2002.12.01T00:25:36.009, 7)")
        self.assertEqual(re, expected)
        # re = self.s.hashBucket(np.datetime64('NaT', dtype='datetime64[ms]'), 5)
        # expected = self.s.run('hashBucket(timestamp(), 5)')
        # self.assertEqual(re, expected)
    
    def test_hashBucket_input_nanosecond(self):
        re = self.s.hashBucket(np.array(pd.date_range(start='2002-12-01T00:25:36.009008007', end='2002-12-02T00:25:36.009008007', freq='h'), dtype='datetime64[ns]'), 7)
        expected = self.s.run("hashBucket(temporalAdd(2002.12.01T00:25:36.009008007, 0..24, 'h'), 7)")
        assert_array_equal(re, expected)
        # re = self.s.hashBucket(np.array(['2002-12-01T00:25:36.009008007', 'NaT', '2002-12-02T00:25:36.825008007'], dtype='datetime64[ns]'), 3)
        # expected = self.s.run("hashBucket([2002.12.01T00:25:36.009008007, NULL, 2002.12.02T00:25:36.825008007], 3)")
        # assert_array_equal(re, expected)
        re = self.s.hashBucket(np.datetime64('2002-12-01T00:25:36.009485695'), 7)
        expected = self.s.run("hashBucket(2002.12.01T00:25:36.009485695, 7)")
        self.assertEqual(re, expected)
        # re = self.s.hashBucket(np.datetime64('NaT', dtype='datetime64[ns]'), 5)
        # expected = self.s.run('hashBucket(nanotimestamp(), 5)')
        # self.assertEqual(re, expected)

    def test_hashBucket_peremata(self):
        with self.assertRaises(TypeError): 
            self.s.hashBucket(obj_ERROR=["aaa", "", "ccc"], nBucket=3)
        with self.assertRaises(TypeError): 
            self.s.hashBucket(obj=["aaa", "", "ccc"], nBucket_ERROR=3)
        self.s.hashBucket(obj=["aaa", "", "ccc"], nBucket=3)
if __name__ == '__main__':
    unittest.main()
