import unittest
import dolphindb as ddb
import pandas as pd
import numpy as np
from setup import *

class TestUploadObject(unittest.TestCase):
    @classmethod
    def setUp(self):
        self.s=ddb.session()
        self.s.connect(HOST,PORT,"admin","123456")
        
    @classmethod
    def tearDownClass(cls):
        pass
    
    def test_upload_scalar(self):
        a=1
        self.s.upload({"a":a})
        re=self.s.run("a")
        self.assertEqual(re,1)

    def test_upload_array(self):
        a=[4,5,7,-3]
        self.s.upload({"a":a})
        re=self.s.run("a")
        self.assertEqual((re==a).all(),True)
    
    def test_upload_series_without_index(self):
        #a=pd.Series([4,7,-5,3])
        #self.s.upload({"a":a})
        #re=self.s.run("a")
        pass
    
    def test_upload_series_with_index(self):
        #a=pd.Series([4,7,-5,3],index=['a','b','c','d'])
        #self.s.upload({"a":a})
        #re=self.s.run("a")
        pass

    def test_upload_nan(self):
        a=np.nan
        self.s.upload({"a":a})
        re=self.s.run("a")
        self.assertEqual(pd.isnull(re),True)
    
    def test_upload_array_with_nan(self):
        a=[np.nan,1,2,3]
        self.s.upload({'a':a})
        re=self.s.run("a")
        self.assertEqual(pd.isnull(re[0]),True)
        self.assertEqual(round(re[1]),1)
        self.assertEqual(round(re[2]),2)
        self.assertEqual(round(re[3]),3)

    def test_upload_dataframe(self):
        data={'state':['Ohio','Ohio','Ohio','Nevada','Nevada'],
              'year':[2000,2001,2002,2001,2002],
              'pop':[1.5,1.7,3.6,2.4,2.9]}
        df=pd.DataFrame(data)
        self.s.upload({"t1":df})
        re=self.s.run("t1")
        self.assertEqual(df.equals(re),True)
    
    def test_upload_dict(self):
        data={'state':['Ohio','Ohio','Ohio','Nevada','Nevada'],
              'year':[2000,2001,2002,2001,2002],
              'pop':[5,7,6,4,9]}
        self.s.upload({"d":data})
        re=self.s.run("d")
        self.assertEqual((data['state']==re['state']).all(),True)
        self.assertEqual((data['year']==re['year']).all(),True)
        self.assertEqual((data['pop']==re['pop']).all(),True)

    def test_upload_numpy_array(self):
        a=np.array([[1,2,3],[4,5,6]])
        self.s.upload({"a":a})
        re=self.s.run("a")
        self.assertEqual((re[0]==a).all(),True)

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


