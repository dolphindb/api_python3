import unittest
import dolphindb as ddb
import pandas as pd
import numpy as np
from numpy.testing import assert_array_equal
from pandas.testing import assert_frame_equal, assert_series_equal
from setup import HOST, PORT, WORK_DIR, DATA_DIR
from datetime import datetime


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
        self.assertEqual(re == list, True)

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
        assert_array_equal(re[0], np.array([1, 2, 3]))
        assert_array_equal(re[1], np.array([4, 5, 6]))

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
        self.assertEqual(re, ['abcd', 786, 2.23, 'runoob', 70.2])

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
    
    def test_upload_float32_dataframe(self):
        pdf = pd.DataFrame({'tfloat': np.arange(1, 10, 1, dtype='float32')})
        pdf.loc[1,:]=np.nan
        self.s.upload({'t':pdf})
        re=self.s.run("t")
        assert_frame_equal(pdf, re, check_dtype=False)

    def test_upload_numpy_scalar_dtype_datetime64_day(self):
        a = np.datetime64('2012-06-08', 'D')
        self.s.upload({'a': a})
        self.assertTrue(self.s.run("eqObj(a, 2012.06.08)"))
        re = self.s.run('a')
        self.assertEqual(a, re)
        # TODO:
        # a = np.datetime64('NaT', 'D')
        # self.s.upload({'a': a})
        # self.assertTrue(self.s.run("eqObj(a, date())"))
        # re = self.s.run('a')
        # self.assertEqual(a, re)

    def test_upload_numpy_scalar_dtype_datetime64_month(self):
        a = np.datetime64('2012-06', 'M')
        self.s.upload({'a': a})
        self.assertTrue(self.s.run("eqObj(a, 2012.06M)"))
        re = self.s.run('a')
        self.assertEqual(a, re)
        # TODO:
        # a = np.datetime64('NaT', 'M')
        # self.s.upload({'a': a})
        # self.assertTrue(self.s.run("eqObj(a, month())"))
        # re = self.s.run('a')
        # self.assertEqual(a, re)

    def test_upload_numpy_scalar_dtype_year(self):
        pass
        # a = np.datetime64('2012', 'Y')
        # self.s.upload({'a': a})

    def test_upload_numpy_scalar_dtype_datetime64_minute(self):
        a = np.datetime64('2005-02-25T03:30', 'm')
        self.s.upload({'a': a})
        re = self.s.run('a')
        self.assertEqual(a, re)
        # TODO:
        # a = np.datetime64('NaT', 'm')
        # self.s.upload({'a': a})
        # re = self.s.run('a')
        # self.assertEqual(a, re)

    def test_upload_numpy_scalar_dtype_datetime64_second(self):
        a = np.datetime64('2005-02-25T03:30:25', 's')
        self.s.upload({'a': a})
        self.assertTrue(self.s.run("eqObj(a, 2005.02.25T03:30:25)"))
        re = self.s.run('a')
        self.assertEqual(a, re)
        # TODO:
        # a = np.datetime64('NaT', 's')
        # self.s.upload({'a': a})
        # re = self.s.run('a')
        # self.assertEqual(a, re)

    def test_upload_numpy_scalar_dtype_datetime64_millisecond(self):
        a = np.datetime64('2005-02-25T03:30:25.008', 'ms')
        self.s.upload({'a': a})
        # self.assertTrue(self.s.run("eqObj(a, 2005.02.05T03:30:25.008)"))
        re = self.s.run('a')
        self.assertEqual(re, a)
        # TODO:
        # a = np.datetime64('NaT', 'ms')
        # self.s.upload({'a': a})
        # self.assertTrue(self.s.run("eqObj(a, timestamp())"))
        # re = self.s.run('a')
        # self.assertEqual(a, re)
    
    def test_upload_numpy_scalar_dtype_datetime64_nanosecond(self):
        a = np.datetime64('2005-02-25T03:30:25.008007006', 'ns')
        self.s.upload({'a': a})
        self.assertTrue(self.s.run("eqObj(a, 2005.02.25T03:30:25.008007006)"))
        re = self.s.run('a')
        self.assertEqual(re, a)
        # TODO:
        # a = np.datetime64('NaT', 'ns')
        # self.s.upload({'a': a})
        # self.assertTrue(self.s.run("eqObj(a, nanotimestamp())"))
        # re = self.s.run('a')
        # self.assertEqual(a, re)


    def test_upload_numpy_array_dtype_datetime64_D(self):
        a = np.array(['2012-06-12', '1968-12-05', '2003-09-28'], dtype='datetime64[D]')
        self.s.upload({'aa': a})
        self.assertTrue(self.s.run("eqObj(aa, [2012.06.12, 1968.12.05, 2003.09.28])"))
        re = self.s.run("aa")
        assert_array_equal(a, re)
        
    def test_upload_dataframe_np_datetime64(self):
        df = pd.DataFrame({'col1': np.array(['2012-06', '2012-07', '', '2024-12'], dtype = 'datetime64[M]'),
                           'col2': np.array(['2012-06-01', '', '2012-07-05', '2013-09-08'], dtype = 'datetime64[D]'),
                           'col3': np.array(['2012-06-01T12:30:00', '2012-06-01T12:30:01', '', ''], dtype = 'datetime64'),
                           'col4': np.array(['2012-06-08T12:30:00.000','','','2012-06-08T12:30:00.001'], dtype='datetime64'),
                           'col5': np.array(['2012-06-08T12:30:00.000001', '', '2012-06-08T12:30:00.000002', ''], dtype = 'datetime64')})
        self.s.upload({'t': df})
        script = '''
        expected = table(nanotimestamp([2012.06.01, 2012.07.01, NULL, 2024.12.01]) as col1, nanotimestamp([2012.06.01, NULL, 2012.07.05, 2013.09.08]) as col2, nanotimestamp([2012.06.01T12:30:00, 2012.06.01T12:30:01, NULL, NULL]) as col3, nanotimestamp([2012.06.08T12:30:00.000, NULL, NULL, 2012.06.08T12:30:00.001]) as col4, [2012.06.08T12:30:00.000001000, NULL, 2012.06.08T12:30:00.000002000, NULL] as col5)
        loop(eqObj, expected.values(), t.values())
        '''
        re = self.s.run(script)
        assert_array_equal(re, [True, True, True, True, True])

    def test_upload_dataframe_chinese_column_name(self):
        df = pd.DataFrame({'编号':[1, 2, 3, 4, 5], '序号':['壹','贰','叁','肆','伍']})
        self.s.upload({'t': df})
        re = self.s.run("select * from t")
        assert_array_equal(re['编号'], [1, 2, 3, 4, 5])
        assert_array_equal(re['序号'], ['壹','贰','叁','肆','伍'])

    def test_upload_dataframe_chinese_column_name(self):
        df = pd.DataFrame({'编号':[1, 2, 3, 4, 5], '序号':['壹','贰','叁','肆','伍']})
        self.s.upload({'t': df})
        re = self.s.run("select * from t")
        assert_array_equal(re['编号'], [1, 2, 3, 4, 5])
        assert_array_equal(re['序号'], ['壹','贰','叁','肆','伍'])

    def test_upload_numpy_scalar_dtype_datetime64_h(self):
        a =np.datetime64("2020-01-01T01",'h')
        self.s.upload({'a': a})
        self.assertTrue(self.s.run("eqObj(a, datehour(2020.01.01T01:00:00))"))
        re = self.s.run('a')
        self.assertEqual(a, re)
        # a = np.datetime64('NaT', 'h')
        # self.s.upload({'a': a})
        # self.assertTrue(self.s.run("eqObj(a, datehour())"))
        # re = self.s.run('a')
        # self.assertEqual(a, re)

    def test_upload_numpy_array_dtype_datetime64_h(self):
        a = np.array(['2012-06-12T01', '1968-12-05T01', '2003-09-28T01'], dtype='datetime64[h]')
        self.s.upload({'a': a})
        self.assertTrue(self.s.run("eqObj(a, datehour([2012.06.12T01:00:00,1968.12.05T01:00:00,2003.09.28T01:00:00]))"))
        re = self.s.run('a')
        assert_array_equal(a, re)
        b = np.repeat(np.datetime64("2020-01-01T01",'h'),500000)
        self.s.upload({'b': b})
        self.assertTrue(self.s.run("eqObj(b, take(datehour(2020.01.01T01:00:00),500000))"))
        re = self.s.run('b')
        assert_array_equal(b, re)
        c = np.repeat(np.datetime64("Nat",'h'),500000)
        self.s.upload({'c': c})
        self.assertTrue(self.s.run("eqObj(c, take(datehour(),500000))"))
        re = self.s.run('c')
        assert_array_equal(c,re)


    
    def test_upload_dict_twice(self):
        data = {'id': [1, 2, 2, 3],
            'date': np.array(['2019-02-04', '2019-02-05', '2019-02-09', '2019-02-13'], dtype='datetime64[D]'),
            'ticker': ['AAPL', 'AMZN', 'AMZN', 'A'],
            'price': [22.2, 3.5, 21.4, 26.5]}
        self.s.upload({"t1": data})
        self.s.upload({"t1": data})
        re=self.s.run("t1")
        assert_array_equal(data['id'], re['id'])
        assert_array_equal(data['date'], re['date'])
        assert_array_equal(data['ticker'], re['ticker'])
        assert_array_equal(data['price'], re['price'])



    def test_upload_dict_repeatedly(self):
        data = {'id': [1, 2, 2, 3],
            'date': np.array(['2019-02-04', '2019-02-05', '2019-02-09', '2019-02-13'], dtype='datetime64[D]'),
            'ticker': ['AAPL', 'AMZN', 'AMZN', 'A'],
            'price': [22.2, 3.5, 21.4, 26.5]}
        for i in range(1,100): {
            self.s.upload({"t1": data})
        }
        re=self.s.run("t1")
        assert_array_equal(data['id'], re['id'])
        assert_array_equal(data['date'], re['date'])
        assert_array_equal(data['ticker'], re['ticker'])
        assert_array_equal(data['price'], re['price'])

    def test_upload_list_twice(self):
        data = [1,2,3]
        self.s.upload({"t1": data})
        self.s.upload({"t1": data})
        re=self.s.run("t1")
        assert_array_equal(data, re)

    def test_upload_list_repeatedly(self):
        data = [1,2,3]
        for i in range(1,100): {
            self.s.upload({"t1": data})
        }
        re=self.s.run("t1")
        assert_array_equal(data, re)
    
    def test_upload_array_twice(self):
        data = np.array([1,2,3.0],dtype=np.double)
        self.s.upload({'arr':data})
        self.s.upload({'arr':data})
        re = self.s.run("arr")
        assert_array_equal(data, re)

    def test_upload_array_repeatedly(self):
        data = np.array([1,2,3.0],dtype=np.double)
        for i in range(1,100):{
            self.s.upload({'arr':data})
        }
        re = self.s.run("arr")
        assert_array_equal(data, re)

    def test_upload_DataFrame_twice(self):
        df = pd.DataFrame({'id': np.int32([1, 2, 3, 6, 8]), 'x': np.int32([5, 4, 3, 2, 1])})
        self.s.upload({'t1': df})
        self.s.upload({'t1': df})
        re = self.s.run("t1.x.avg()")
        assert_array_equal(3.0, re)

    def test_upload_DataFrame_repeatedly(self):
        df = pd.DataFrame({'id': np.int32([1, 2, 3, 6, 8]), 'x': np.int32([5, 4, 3, 2, 1])})
        for i in range(1,1000):{
            self.s.upload({'t1': df})
        }      
        re = self.s.run("t1.x.avg()")
        assert_array_equal(3.0, re)

    def test_upload_paramete(self):         
        df = pd.DataFrame({'id': np.int32([1, 2, 3, 6, 8]), 'x': np.int32([5, 4, 3, 2, 1])})
        with self.assertRaises(TypeError):
            self.s.upload(nameObjectDict_ERROR={'t1': df})
        self.s.upload(nameObjectDict={'t1': df})
    
    def test_upload_table_and_update(self):
        tb=pd.DataFrame({'id': [1, 2, 2, 3],
                 'ticker': ['AAPL', 'AMZN', 'AMZN', 'A'],
                 'price': [22, 3.5, 21, 26]})
        memtab="test_upload"
        tt = self.s.table(data=tb.to_dict(), tableAliasName=memtab)
        self.s.run("update " + memtab + " set wd_time=price")
        cols = self.s.run("test_upload.colNames()")
        self.assertEqual(len(cols), 4)

    def test_upload_DataFrame_None(self):
        df = pd.DataFrame({'organization_code': [None, None,None]})
        self.s.upload({'tb_temp': df})
        dbvalue=self.s.run("tb_temp")
        assert_array_equal(['','',''], dbvalue['organization_code'])
        self.assertEqual(dbvalue['organization_code'].dtypes,"object")
        
    def test_upload_DataFrame_nan_None(self):
        df = pd.DataFrame({'organization_code': [np.nan, None,None]})
        self.s.upload({'tb_temp': df})
        dbvalue=self.s.run("tb_temp")
        assert_array_equal([np.NaN,np.NaN,np.NaN], dbvalue['organization_code'])
        self.assertEqual(dbvalue['organization_code'].dtypes,"float64")

    def test_upload_DataFrame_None_nan(self):
        df = pd.DataFrame({'organization_code': [None, None,np.nan]})
        self.s.upload({'tb_temp': df})
        dbvalue=self.s.run("tb_temp")
        assert_array_equal([np.NaN,np.NaN,np.NaN], dbvalue['organization_code'])
        self.assertEqual(dbvalue['organization_code'].dtypes,"float64")
        
    def test_upload_DataFrame_NaT_None(self):
        df = pd.DataFrame({'organization_code': [pd.NaT, None,None]})
        self.s.upload({'tb_temp': df})
        dbvalue=self.s.run("tb_temp")
        assert_array_equal(np.array([None,None,None], dtype="datetime64[ms]"), dbvalue['organization_code'])
        self.assertEqual(dbvalue['organization_code'].dtypes,"datetime64[ns]")

    def test_upload_DataFrame_None_NaT(self):
        df = pd.DataFrame({'organization_code': [None,pd.NaT,None]})
        self.s.upload({'tb_temp': df})
        dbvalue=self.s.run("tb_temp")
        assert_array_equal(np.array([None,None,None], dtype="datetime64[ms]"), dbvalue['organization_code'])
        self.assertEqual(dbvalue['organization_code'].dtypes,"datetime64[ns]")

    def test_upload_DataFrame_None_nan_NaT(self):
        df = pd.DataFrame({'organization_code': [None, np.nan,pd.NaT]})
        self.s.upload({'tb_temp': df})
        dbvalue=self.s.run("tb_temp")
        assert_array_equal(np.array([None,None,None], dtype="datetime64[ms]"), dbvalue['organization_code'])
        self.assertEqual(dbvalue['organization_code'].dtypes,"datetime64[ns]")

    def test_upload_DataFrame_NaT_None_nan(self):
        df = pd.DataFrame({'organization_code': [pd.NaT, None,np.nan]})
        self.s.upload({'tb_temp': df})
        dbvalue=self.s.run("tb_temp")
        assert_array_equal(np.array([None,None,None], dtype="datetime64[ms]"), dbvalue['organization_code'])
        self.assertEqual(dbvalue['organization_code'].dtypes,"datetime64[ns]")

    def test_upload_DataFrame_None_value(self):
        df = pd.DataFrame({'organization_code': [None, None, None,10]})
        self.s.upload({'tb_temp': df})
        dbvalue=self.s.run("tb_temp")
        assert_array_equal([np.nan,np.nan,np.nan,10], dbvalue['organization_code'])
        self.assertEqual(dbvalue['organization_code'].dtypes,"float64")

    def test_upload_DataFrame_nan_value(self):
        df = pd.DataFrame({'organization_code': [np.nan,np.nan,np.nan,10]})
        self.s.upload({'tb_temp': df})
        dbvalue=self.s.run("tb_temp")
        assert_array_equal([np.nan,np.nan,np.nan,10], dbvalue['organization_code'])
        self.assertEqual(dbvalue['organization_code'].dtypes,"float64")

    def test_upload_DataFrame_NaT_value(self):
        df = pd.DataFrame({'organization_code': [pd.NaT,pd.NaT,pd.NaT,10]})
        self.s.upload({'tb_temp': df})
        dbvalue=self.s.run("tb_temp")
        assert_array_equal([np.nan,np.nan,np.nan,10], dbvalue['organization_code'])
        self.assertEqual(dbvalue['organization_code'].dtypes,"float64")

    def test_upload_DataFrame_None_nan_value(self):
        df = pd.DataFrame({'organization_code': [None,10,np.nan,None]})
        self.s.upload({'tb_temp': df})
        dbvalue=self.s.run("tb_temp")
        assert_array_equal([np.nan,10,np.nan,np.nan], dbvalue['organization_code'])
        self.assertEqual(dbvalue['organization_code'].dtypes,"float64")

    def test_upload_DataFrame_None_NaT_value(self):
        df = pd.DataFrame({'organization_code': [None,10,pd.NaT,None]})
        self.s.upload({'tb_temp': df})
        dbvalue=self.s.run("tb_temp")
        print(dbvalue['organization_code'])
        assert_array_equal([np.nan,10,np.nan,np.nan], dbvalue['organization_code'])
        self.assertEqual(dbvalue['organization_code'].dtypes,"float64")

    def test_upload_DataFrame_nan_NaT_value(self):
        df = pd.DataFrame({'organization_code': [np.nan,10,pd.NaT,pd.NaT]})
        self.s.upload({'tb_temp': df})
        dbvalue=self.s.run("tb_temp")
        assert_array_equal([np.nan,10,np.nan,np.nan], dbvalue['organization_code'])
        self.assertEqual(dbvalue['organization_code'].dtypes,"float64")

    def test_upload_DataFrame_NaT_None_nan_value(self):
        df = pd.DataFrame({'organization_code': [pd.NaT, None,np.nan,10]})
        self.s.upload({'tb_temp': df})
        dbvalue=self.s.run("tb_temp")
        assert_array_equal([np.nan,np.nan,np.nan,10], dbvalue['organization_code'])
        self.assertEqual(dbvalue['organization_code'].dtypes,"float64")

    def test_upload_DataFrame_None_string(self):
        df = pd.DataFrame({'organization_code': [None, None, None,"string"]})
        self.s.upload({'tb_temp': df})
        dbvalue=self.s.run("tb_temp")
        assert_array_equal(["","","","string"], dbvalue['organization_code'])
        self.assertEqual(dbvalue['organization_code'].dtypes,"object")

    def test_upload_DataFrame_nan_string(self):
        df = pd.DataFrame({'organization_code': [np.nan,np.nan,np.nan,"string"]})
        self.s.upload({'tb_temp': df})
        dbvalue=self.s.run("tb_temp")
        assert_array_equal(["","","","string"], dbvalue['organization_code'])
        self.assertEqual(dbvalue['organization_code'].dtypes,"object")

    def test_upload_DataFrame_NaT_string(self):
        df = pd.DataFrame({'organization_code': [pd.NaT,pd.NaT,pd.NaT,"string"]})
        self.s.upload({'tb_temp': df})
        dbvalue=self.s.run("tb_temp")
        assert_array_equal(["","","","string"], dbvalue['organization_code'])
        self.assertEqual(dbvalue['organization_code'].dtypes,"object")
        
    def test_upload_DataFrame_None_nan_string(self):
        df = pd.DataFrame({'organization_code': [None,"string",np.nan,None]})
        self.s.upload({'tb_temp': df})
        dbvalue=self.s.run("tb_temp")
        assert_array_equal(["","string","",""], dbvalue['organization_code'])
        self.assertEqual(dbvalue['organization_code'].dtypes,"object")

    def test_upload_DataFrame_None_NaT_string(self):
        df = pd.DataFrame({'organization_code': [None,"string",pd.NaT,None]})
        self.s.upload({'tb_temp': df})
        dbvalue=self.s.run("tb_temp")
        assert_array_equal(["","string","",""], dbvalue['organization_code'])
        self.assertEqual(dbvalue['organization_code'].dtypes,"object")

    def test_upload_DataFrame_nan_NaT_string(self):
        df = pd.DataFrame({'organization_code': [np.nan,"string",pd.NaT,pd.NaT]})
        self.s.upload({'tb_temp': df})
        dbvalue=self.s.run("tb_temp")
        assert_array_equal(["","string","",""], dbvalue['organization_code'])
        self.assertEqual(dbvalue['organization_code'].dtypes,"object")

    def test_upload_DataFrame_NaT_None_nan_string(self):
        df = pd.DataFrame({'organization_code': [pd.NaT, None,np.nan,"string"]})
        self.s.upload({'tb_temp': df})
        dbvalue=self.s.run("tb_temp")
        assert_array_equal(["","","","string"], dbvalue['organization_code'])
        self.assertEqual(dbvalue['organization_code'].dtypes,"object")

    def test_upload_DataFrame_None_num_string(self):
        df = pd.DataFrame({'organization_code': [None, None, None, "123"]})
        self.s.upload({'tb_temp': df})
        dbvalue = self.s.run("tb_temp")
        assert_array_equal(["", "", "", "123"], dbvalue['organization_code'])
        self.assertEqual(dbvalue['organization_code'].dtypes, "object")

    def test_upload_DataFrame_nan_num_string(self):
        df = pd.DataFrame(
            {'organization_code': [np.nan, np.nan, np.nan, "123"]})
        self.s.upload({'tb_temp': df})
        dbvalue = self.s.run("tb_temp")
        assert_array_equal(["", "", "", "123"], dbvalue['organization_code'])
        self.assertEqual(dbvalue['organization_code'].dtypes, "object")

    def test_upload_DataFrame_NaT_num_string(self):
        df = pd.DataFrame(
            {'organization_code': [pd.NaT, pd.NaT, pd.NaT, "123"]})
        with self.assertRaises(RuntimeError):
            self.s.upload({'tb_temp': df})

    def test_upload_DataFrame_None_nan_num_string(self):
        df = pd.DataFrame({'organization_code': [None, "123", np.nan, None]})
        self.s.upload({'tb_temp': df})
        dbvalue = self.s.run("tb_temp")
        assert_array_equal(["", "123", "", ""], dbvalue['organization_code'])
        self.assertEqual(dbvalue['organization_code'].dtypes, "object")

    def test_upload_DataFrame_None_NaT_num_string(self):
        df = pd.DataFrame({'organization_code': [None, "123", pd.NaT, None]})
        with self.assertRaises(RuntimeError):
            self.s.upload({'tb_temp': df})

    def test_upload_DataFrame_nan_NaT_num_string(self):
        df = pd.DataFrame(
            {'organization_code': [np.nan, "123", pd.NaT, pd.NaT]})
        with self.assertRaises(RuntimeError):
            self.s.upload({'tb_temp': df})

    def test_upload_DataFrame_NaT_None_nan_num_string(self):
        df = pd.DataFrame({'organization_code': [pd.NaT, None, np.nan, "123"]})
        with self.assertRaises(RuntimeError):
            self.s.upload({'tb_temp': df})    
            
    def test_upload_pd_to_datetime(self):
        self.s.upload({"hh":pd.to_datetime("2022-05-23T14:51:45.421")})
        dbvalue = self.s.run("hh")
        self.assertEqual(pd.to_datetime("2022-05-23T14:51:45.421"), dbvalue)
        
    def test_upload_pd_Timestamp(self):
        self.s.upload({"hh":pd.Timestamp("2021-01-01")})
        dbvalue = self.s.run("hh")
        self.assertEqual(pd.Timestamp("2021-01-01"), dbvalue)
    
if __name__ == '__main__':
    unittest.main()
