import unittest
import dolphindb as ddb
import pandas as pd
import numpy as np
from setup import *


class TestBasicDataTypes(unittest.TestCase):
    @classmethod
    def setUp(self):
        self.s=ddb.session()
        self.s.connect(HOST,PORT,"admin","123456")
        
    @classmethod
    def tearDownClass(cls):
        pass

    def test_int_scalar(self):
        re=self.s.run("100")
        self.assertEqual(re,100)
    
    def test_bool_scalar(self):
        re=self.s.run("true")
        self.assertEqual(re,True)
    
    def test_char_scalar(self):
        re=self.s.run("'a'")
        self.assertEqual(re,97)
    
    def test_short_scalar(self):
        re=self.s.run("112h")
        self.assertEqual(re,112)
    
    def test_long_scalar(self):
        re=self.s.run("22l")
        self.assertEqual(re,22)
    
    def test_date_scalar(self):
        re=self.s.run("2012.06.12")
        self.assertEqual(re,np.datetime64('2012-06-12'))
    
    def test_month_scalar(self):
        re=self.s.run("2012.06M")
        self.assertEqual(re,np.datetime64('2012-06'))
    
    def test_time_scalar(self):
        re=self.s.run("12:30:00.008")
        self.assertEqual(re,np.datetime64('1970-01-01T12:30:00.008'))
    
    def test_minute_scalar(self):
        re=self.s.run("12:30m")
        self.assertEqual(re,np.datetime64('1970-01-01T12:30'))
    
    def test_second_scalar(self):
        re=self.s.run("12:30:10")
        self.assertEqual(re,np.datetime64('1970-01-01T12:30:10'))
    
    def test_datetime_scalar(self):
        re=self.s.run('2012.06.13 13:30:10')
        self.assertEqual(re,np.datetime64('2012-06-13T13:30:10'))
    
    def test_timestamp_scalar(self):
        re=self.s.run('2012.06.13 13:30:10.008')
        self.assertEqual(re,np.datetime64('2012-06-13T13:30:10.008'))
    
    def test_nanotime_scalar(self):
        re=self.s.run('13:30:10.008007006')
        self.assertEqual(re,np.datetime64('1970-01-01T13:30:10.008007006'))
    
    def test_nanotimestamp(self):
        re=self.s.run('2012.06.13 13:30:10.008007006')
        self.assertEqual(re,np.datetime64('2012-06-13T13:30:10.008007006'))
    
    def test_float_scalar(self):
        re=self.s.run('2.1f')
        self.assertEqual(round(re),2)
        
    def test_double_scalar(self):
        re=self.s.run('2.1')
        self.assertEqual(re,2.1)

    def test_string_scalar(self):
        re=self.s.run('"abc"')
        self.assertEqual(re,'abc')
    
    def test_uuid_scalar(self):
        re=self.s.run("uuid('5d212a78-cc48-e3b1-4235-b4d91473ee87')")
        self.assertEqual(re,'5d212a78-cc48-e3b1-4235-b4d91473ee87')

    def test_ipaddr_sclar(self):
        re=self.s.run("ipaddr('192.168.1.135')")
        self.assertEqual(re,'192.168.1.135')
    
    def test_int128_scalar(self):
        re=self.s.run("int128('e1671797c52e15f763380b45e841ec32')")
        self.assertEqual(re,'e1671797c52e15f763380b45e841ec32')

    def test_string_vector(self):
        re=self.s.run("`IBM`GOOG`YHOO")
        self.assertEqual((re==['IBM','GOOG','YHOO']).all(),True)

    def test_function_def(self):
        re=self.s.run("def(a,b){return a+b}")
        self.assertEqual((len(re)>0),True)

    def test_symbol_vector(self):
        re=self.s.run("symbol(`IBM`MSFT`GOOG`BIDU)")
        self.assertEqual((re==['IBM','MSFT','GOOG','BIDU']).all(),True)

    def test_int_vector(self):
        re=self.s.run("2938 2920 54938 1999 2333")
        self.assertEqual((re==[2938,2920,54938,1999,2333]).all(),True)

    def test_double_vector(self):
        re=self.s.run("rand(10.0,10)")
        self.assertEqual(len(re),10)

    def test_date_vector(self):
        re=self.s.run("2012.10.01 +1..3")
        expected=[np.datetime64('2012-10-02', dtype='datetime64[D]'), np.datetime64('2012-10-03', dtype='datetime64[D]'), np.datetime64('2012-10-04', dtype='datetime64[D]')]
        self.assertEqual((re==expected).all(),True)
    
    def test_datetime_vector(self):
        re=self.s.run("2012.10.01T15:00:04 + 2009..2011")
        expected=[np.datetime64('2012-10-01T15:33:33', dtype='datetime64[s]'), np.datetime64('2012-10-01T15:33:34', dtype='datetime64[s]'), np.datetime64('2012-10-01T15:33:35', dtype='datetime64[s]')]
        self.assertEqual((re==expected).all(),True)
    
    def test_int_matrix(self):
        re=self.s.run("1..6$2:3")
        expected=np.array([[1, 3, 5],[2, 4, 6]])
        self.assertEqual((re[0]==expected).all(),True)
    
    def test_int_matrix_with_label(self):
        re=self.s.run("cross(add,1..5,1..10)")
        expected=np.array([[ 2,  3,  4,  5,  6,  7,  8,  9, 10, 11],[ 3,  4,  5,  6,  7,  8,  9, 10, 11, 12],[ 4,  5,  6,  7,  8,  9, 10, 11, 12, 13], [ 5,  6,  7,  8,  9, 10, 11, 12, 13, 14],[ 6,  7,  8,  9, 10, 11, 12, 13, 14, 15]])
        self.assertEqual((re[0]==expected).all(),True)
    
    def test_table(self):
        script = '''n=20;
		syms=`IBM`C`MS`MSFT`JPM`ORCL`BIDU`SOHU`GE`EBAY`GOOG`FORD`GS`PEP`USO`GLD`GDX`EEM`FXI`SLV`SINA`BAC`AAPL`PALL`YHOO`KOH`TSLA`CS`CISO`SUN;
		mytrades=table(09:30:00+rand(18000,n) as timestamp,rand(syms,n) as sym, 10*(1+rand(100,n)) as qty,5.0+rand(100.0,n) as price);
		select qty,price from mytrades where sym==`IBM;'''
        re=self.s.run(script)
        self.assertEqual(re.shape[1],2)
    
    def test_dictionary(self):
        script = '''dict(1 2 3,`IBM`MSFT`GOOG)'''
        re=self.s.run(script)
        expected={2:'MSFT',3:'GOOG',1:'IBM'}
        self.assertDictEqual(re,expected)

    def test_any_vector(self):
        re=self.s.run("([1], [2],[1,3, 5],[0.9, 0.8])")
        self.assertEqual((re[0]==[1]).all(),True)
        self.assertEqual((re[1]==[2]).all(),True)
        self.assertEqual((re[2]==[1,3,5]).all(),True)

    def test_set(self):
        re=self.s.run("set(1+3*1..3)")
        self.assertSetEqual(re,{10,4,7})
    
    def test_pair(self):
        re=self.s.run("3:4")
        self.assertListEqual(re,list([3,4]))

    def test_any_dictionary(self):
        re=self.s.run("{a:1,b:2}")
        expected={'a':1,'b':2}
        self.assertDictEqual(re,expected)

    def test_upload_matrix(self):
        a=self.s.run("cross(+, 1..5, 1..5)")
        b=self.s.run("1..25$5:5")
        self.s.upload({'a':a,'b':b})
        re=self.s.run('a+b')
        self.assertEqual((re[0][0][0]==[3,9,15,21,27]).all(),True)
        self.assertEqual((re[0][0][1]==[5,11,17,23,29]).all(),True)
        self.assertEqual((re[0][0][2]==[7,13,19,25,31]).all(),True)
        self.assertEqual((re[0][0][3]==[9,15,21,27,33]).all(),True)
        self.assertEqual((re[0][0][4]==[11,17,23,29,35]).all(),True)

if __name__ == '__main__':
    unittest.main()