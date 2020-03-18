import unittest
import dolphindb as ddb
import pandas as pd
import numpy as np
from setup import *


class TestRunFunction(unittest.TestCase):
    @classmethod
    def setUp(self):
        self.s=ddb.session()
        self.s.connect(HOST,PORT,"admin","123456")
        
    @classmethod
    def tearDownClass(cls):
        pass
    
    def test_function_add_int(self):
        re=self.s.run('add',3,4)
        self.assertEqual(re,7)

    def test_function_add_string(self):
        re=self.s.run('add','hello','world')
        self.assertMultiLineEqual(re,'helloworld')

    def test_function_sum_list(self):
        re=self.s.run('sum',[1.0,2.0,3.0])
        self.assertAlmostEqual(re,6.0)

    def test_function_sum_numpy_array_int32(self):
        re=self.s.run('sum',np.array([100000, 200000, 300000]))
        self.assertEqual(re,600000)
    
    def test_function_sum_numpy_array_int64(self):
        pass
        #re=self.s.run('sum',np.int64([1e15, 2e15, 3e15])
    
    def test_function_sum_numpy_array_float64(self):
        re=self.s.run('sum',np.array([100000.0, 200000.0, 300000.0]))
        self.assertAlmostEqual(re,600000.0)

    def test_function_reverse_str_array(self):
        re=self.s.run('reverse',np.array(["1", "2", "3"],dtype="str"))
        self.assertMultiLineEqual(re[0],'3')
        self.assertMultiLineEqual(re[1],'2')
        self.assertMultiLineEqual(re[2],'1')
    
    def test_function_flatten_matrix(self):
        re=self.s.run('flatten',np.int32([[1, 2, 3], [4, 5, 6]]))
        self.assertEqual((re==np.array([1,4,2,5,3,6])).all(),True)

    def test_function_case_matrix(self):
        pass
        #self.s.run("cast", np.double([[1.0, 2.0, 3.0], [4.0, 5.0, 6.0]]),[2,3])

    def test_function_wavg(self):
        col1=[100,30,300]
        col2=[1,1.5,2]
        re=self.s.run("wavg",col1,col2)
        self.assertEqual(round(re),152)

    def test_user_defined_function(self):
        re=self.s.run("login('admin','123456')")
        self.s.run("def foo(a,b){return a+b-1}")
        re=self.s.run('foo',3,4)
        self.assertEqual(re,6)
    

if __name__ == '__main__':
    unittest.main()