import unittest
import dolphindb as ddb
import pandas as pd
import numpy as np
from numpy.testing import assert_array_equal
from pandas.testing import assert_frame_equal, assert_series_equal
from setup import HOST, PORT, WORK_DIR, DATA_DIR


class TestUploadObject(unittest.TestCase):
    @classmethod
    def setUp(cls):
        cls.s = ddb.session()
        cls.s.connect(HOST, PORT, "admin", "123456")

    @classmethod
    def tearDownClass(cls):
        pass

    def test_upload_int_scalar(self):
        a = 1
        self.s.upload({"a": a})
        self.assertEqual(self.s.run("eqObj(a, 1)"), True)
        re = self.s.run("a")
        self.assertEqual(re, 1)

    def test_upload_bool_scalar(self):
        a = True
        self.s.upload({"a": a})
        self.assertEqual(self.s.run("eqObj(a, true)"), True)
        re = self.s.run("a")
        self.assertEqual(re, True)

    def test_upload_float_scalar(self):
        a = 5.5
        self.s.upload({"a": a})
        self.assertEqual(self.s.run("eqObj(a, 5.5, 1)"), True)
        re = self.s.run("a")
        self.assertEqual(re, 5.5)

    def test_upload_complex_scalar(self):
        pass

    def test_upload_string_scalar(self):
        a = 'Runoob'
        self.s.upload({'a': a})
        self.assertEqual(self.s.run("eqObj(a, 'Runoob')"), True)
        re = self.s.run("a")
        self.assertEqual(re, 'Runoob')

    def test_upload_mix_list(self):
        list = ['abcd', 786, 2.23, 'runoob', 70.2]
        self.s.upload({'list': list})
        self.assertEqual(self.s.run("eqObj(list, ['abcd', 786, 2.23, 'runoob', 70.2])"), True)
        re = self.s.run("list")
        self.assertEqual((re == list).all(), True)

    def test_upload_int_list(self):
        a = [4, 5, 7, -3]
        self.s.upload({"a": a})
        self.assertEqual(self.s.run("eqObj(a, [4, 5, 7, -3])"), True)
        re = self.s.run("a")
        self.assertEqual((re == a).all(), True)
    
    def test_upload_string_list(self):
        a = ['aaa', 'bbb', 'ccc']
        self.s.upload({"a": a})
        self.assertEqual(self.s.run("eqObj(a, ['aaa', 'bbb', 'ccc'])"), True)
        re = self.s.run("a")
        self.assertEqual((re == a).all(), True)

    def test_upload_bool_list(self):
        a = [True, False, False, True]
        self.s.upload({"a": a})
        self.assertEqual(self.s.run("eqObj(a, [true, false, false, true])"), True)
        re = self.s.run("a")
        self.assertEqual((re == a).all(), True)

    def test_upload_list_list(self):
        a = [[1, 2, 3], [4, 5, 6]]
        self.s.upload({"a": a})
        self.assertEqual(self.s.run("eqObj(a[0], [1, 2, 3])"), True)
        self.assertEqual(self.s.run("eqObj(a[1], [4, 5, 6])"), True)
        re = self.s.run("a")
        assert_array_equal(re, [[1, 2, 3], [4, 5, 6]])
        
    def test_upload_tuple(self):
        tuple = ('abcd', 786 , 2.23, 'runoob', 70.2)
        self.s.upload({"tuple": tuple})
        self.assertEqual(self.s.run("eqObj(tuple, ['abcd', 786, 2.23, 'runoob', 70.2])"), True)
        re = self.s.run("tuple")
        self.assertEqual((re==tuple).all(), True)

    def test_upload_set(self):
        a = set('abracadabra')
        self.s.upload({'a': a})
        self.assertEqual(self.s.run("eqObj(sort(a.keys()), `a`b`c`d`r)"), True)
        re = self.s.run("a")
        self.assertSetEqual(re, a)

    def test_upload_pandas_series_without_index(self):
        a = pd.Series([4, 7, -5, 3])
        self.s.upload({"a": a})
        self.assertEqual(self.s.run("eqObj(a, [4,7,-5,3])"), True)
        re = self.s.run("a")
        assert_array_equal(re, [4, 7, -5, 3])

    def test_upload_pandas_series_dtype_object(self):
        a = pd.Series(['a', 'b', 'c', 'd'], dtype = "object")
        self.s.upload({"a": a})
        self.assertEqual(self.s.run("eqObj(a, `a`b`c`d)"), True)
        re = self.s.run("a")
        assert_array_equal(re, ['a', 'b', 'c', 'd'])

    def test_upload_pandas_series_dtype_int32(self):
        a = pd.Series([1, 2, 3], dtype="int32")
        self.s.upload({"a": a})
        self.assertEqual(self.s.run("eqObj(a, [1, 2, 3])"), True)
        re = self.s.run("a")
        assert_array_equal(re, [1, 2, 3])
    
    def test_upload_pandas_series_dtype_int64(self):
        a = pd.Series([1, 2, 3], dtype="int64")
        self.s.upload({"a": a})
        self.assertEqual(self.s.run("eqObj(a, [1, 2, 3])"), True)
        re = self.s.run("a")
        assert_array_equal(re, [1, 2, 3])

    def test_upload_pandas_series_dtype_float32(self):
        a = pd.Series([1, 2, np.nan], dtype="float32")
        self.s.upload({"a": a})
        self.assertEqual(self.s.run("eqObj(a, [1.0, 2.0, NULL])"), True)
        re = self.s.run("a")
        assert_array_equal(re, [1, 2, np.nan])

    def test_upload_pandas_series_dtype_float64(self):
        a = pd.Series([1, 2, np.nan], dtype="float64")
        self.s.upload({"a": a})
        self.assertEqual(self.s.run("eqObj(a, [1.0, 2.0, NULL])"), True)
        re = self.s.run("a")
        assert_array_equal(re, [1, 2, np.nan])

    def test_upload_pandas_series_dtype_datetime64(self):
        a = pd.Series(['2018-07-01', '2019-07-01', '2019-10-01'], dtype="datetime64[ns]")
        self.s.upload({"a": a})
        self.assertEqual(self.s.run("eqObj(a, [2018.07.01T00:00:00.000000000, 2019.07.01T00:00:00.000000000, 2019.10.01T00:00:00.000000000])"), True)
        re = self.s.run("a")
        assert_array_equal(re, np.array(['2018-07-01T00:00:00.000000000','2019-07-01T00:00:00.000000000','2019-10-01T00:00:00.000000000'], dtype="datetime64[ns]"))

    def test_upload_pandas_series_with_index(self):
        a = pd.Series([4, 7, -5, 3], index=['a', 'b', 'c', 'd'])
        self.s.upload({"a": a})
        self.assertEqual(self.s.run("eqObj(a, [4,7,-5,3])"), True)
        re = self.s.run("a")
        assert_array_equal(re, [4, 7, -5, 3])  # index aborted

    def test_upload_nan(self):
        a = np.nan
        self.s.upload({"a": a})
        self.assertEqual(self.s.run("eqObj(a, int())"), True)
        re = self.s.run("a")
        self.assertEqual(pd.isnull(re), True)

    def test_upload_array_with_nan(self):
        a = [np.nan, 1, 2, 3]
        self.s.upload({'a': a})
        self.assertEqual(self.s.run("eqObj(a, [,1,2,3])"), True)
        re = self.s.run("a")
        assert_array_equal(re, [np.nan, 1, 2, 3])

    def test_upload_dataframe(self):
        data = {'state': ['Ohio', 'Ohio', 'Ohio', 'Nevada', 'Nevada'],
                'year': [2000, 2001, 2002, 2001, 2002],
                'pop': [1.5, 1.7, 3.6, 2.4, 2.9]}
        df = pd.DataFrame(data)
        self.s.upload({"t1": df})
        self.assertEqual(self.s.run("all(each(eqObj,t1.values(),"
                                    "table(['Ohio', 'Ohio', 'Ohio', 'Nevada', 'Nevada'] as state, "
                                    "[2000, 2001, 2002, 2001, 2002] as year, "
                                    "[1.5, 1.7, 3.6, 2.4, 2.9] as pop).values()))"), True)
        re = self.s.run("t1")
        assert_frame_equal(re, df)

    def test_upload_dict(self):
        data = {'state': ['Ohio', 'Ohio', 'Ohio', 'Nevada', 'Nevada'],
                'year': [2000, 2001, 2002, 2001, 2002],
                'pop': [5, 7, 6, 4, 9]}
        self.s.upload({"d": data})
        self.assertEqual(self.s.run("eqObj(d[`state].sort(), `Nevada`Nevada`Ohio`Ohio`Ohio)"), True)
        self.assertEqual(self.s.run("eqObj(d[`year].sort(), [2000, 2001, 2001, 2002, 2002])"), True)
        self.assertEqual(self.s.run("eqObj(d[`pop].sort(), [4, 5, 6, 7, 9])"), True)
        re = self.s.run("d")
        self.assertEqual((data['state'] == re['state']).all(), True)
        self.assertEqual((data['year'] == re['year']).all(), True)
        self.assertEqual((data['pop'] == re['pop']).all(), True)

    def test_upload_numpy_one_dimension_array(self):
        a = np.array(range(10))
        self.s.upload({"a": a})
        self.assertEqual(self.s.run("eqObj(a, 0..9)"), True)
        re =self.s.run("a")
        assert_array_equal(re, [0,1,2,3,4,5,6,7,8,9])

    def test_upload_numpy_two_dimension_array(self):
        a = np.array([[1, 2, 3], [4, 5, 6]])
        self.s.upload({"a": a})
        self.assertEqual(self.s.run("eqObj(a, 1 4 2 5 3 6$2:3)"), True)
        re = self.s.run("a")
        # TODO:BUG
        # assert_array_equal(re, a)
        assert_array_equal(re[0], a)

    def test_upload_matrix(self):
        a = self.s.run("cross(+, 1..5, 1..5)")
        b = self.s.run("1..25$5:5")
        self.s.upload({'a': a[0], 'b': b[0]})
        self.assertEqual(self.s.run("eqObj(a, cross(+, 1..5, 1..5))"), True)
        self.assertEqual(self.s.run("eqObj(b, 1..25$5:5)"), True)

        re = self.s.run('a+b')
        self.assertEqual((re[0][0] == [3, 9, 15, 21, 27]).all(), True)
        self.assertEqual((re[0][1] == [5, 11, 17, 23, 29]).all(), True)
        self.assertEqual((re[0][2] == [7, 13, 19, 25, 31]).all(), True)
        self.assertEqual((re[0][3] == [9, 15, 21, 27, 33]).all(), True)
        self.assertEqual((re[0][4] == [11, 17, 23, 29, 35]).all(), True)

    def test_upload_numpy_eye_matrix(self):
        a = np.eye(4)
        self.s.upload({"a": a})
        self.assertEqual(self.s.run("eqObj(a, [1,0,0,0,0,1,0,0,0,0,1,0,0,0,0,1]*1.0$4:4)"), True)
        re = self.s.run("a")
        assert_array_equal(re[0], [[1., 0., 0., 0.],[0., 1., 0., 0.],[0., 0., 1., 0.],[0., 0., 0., 1.]])

    def test_upload_numpy_matrix(self):
        a = np.matrix('1 2; 3 4')
        self.s.upload({"a": a})
        self.assertEqual(self.s.run("eqObj(a, [1,3,2,4]$2:2)"), True)
        re = self.s.run("a")
        assert_array_equal(re[0], [[1, 2],[3, 4]])

if __name__ == '__main__':
    unittest.main()
