import unittest
import dolphindb as ddb
import pandas as pd
import numpy as np
from setup import *

class TestTable(unittest.TestCase):
    @classmethod
    def setUp(self):
        self.s=ddb.session()
        self.s.connect(HOST,PORT,"admin","123456")
        
    @classmethod
    def tearDownClass(cls):
        pass
    
    def test_create_table_by_python_dictionary(self):
        data={'state':['Ohio','Ohio','Ohio','Nevada','Nevada'],
              'year':[2000,2001,2002,2001,2002],
              'pop':[1.5,1.7,3.6,2.4,2.9]}
        tmp=self.s.table(data=data,tableAliasName="tmp")
        re=self.s.run("tmp")
        df=pd.DataFrame(data)
        self.assertEqual(df.equals(re),True)

    def test_create_table_by_pandas_dataframe(self):
        data={'state':['Ohio','Ohio','Ohio','Nevada','Nevada'],
              'year':[2000,2001,2002,2001,2002],
              'pop':[1.5,1.7,3.6,2.4,2.9]}
        df=pd.DataFrame(data)
        tmp=self.s.table(data=df,tableAliasName="tmp")
        re=self.s.run("tmp")
        self.assertEqual(df.equals(re),True)

    def test_table_toDF(self):
        tmp=self.s.loadText(DATA_DIR+"/USPrices_FIRST.csv")
        df=tmp.toDF()
        self.assertEqual(len(df),6047)
        tbName=tmp.tableName()
        self.s.run("undef",tbName)

    def test_table_showSQL(self):
        tmp=self.s.loadText(DATA_DIR+"/USPrices_FIRST.csv")
        sql=tmp.showSQL()
        tbName=tmp.tableName()
        self.assertEqual(sql,'select PERMNO,date,SHRCD,TICKER,TRDSTAT,HEXCD,CUSIP,DLSTCD,DLPRC,DLRET,BIDLO,ASKHI,PRC,VOL,RET,BID,ASK,SHROUT,CFACPR,CFACSHR,OPENPRC from {tbName}'.format(tbName=tbName))
        self.s.run("undef",tbName)

    def test_table_sql_select(self):
        tmp=self.s.loadText(DATA_DIR+"/USPrices_FIRST.csv")
        re=tmp.select(['PERMNO','date']).where(tmp.date>'2010.01.01')
        self.assertEqual(re.rows,1510)
        #self.assertEqual(re.cols,2)
        re=tmp.select(['PERMNO','date']).where(tmp.date>'2010.01.01').sort(['date desc'])
        self.assertEqual(re.rows,1510)
        #self.assertEqual(re.cols,2)
        re=tmp[tmp.date>'2010.01.01']
        self.assertEqual(re.rows,1510)
        #self.assertEqual(re.cols,2)
        tbName=tmp.tableName()
        self.s.run("undef",tbName)
    
    def test_table_sql_groupby(self):
        tmp=self.s.loadText(DATA_DIR+"/USPrices_FIRST.csv")
        origin=tmp.toDF()
        df=tmp.groupby('PERMNO').agg({'bid':['sum']}).toDF()
        self.assertEqual((df['PERMNO']==10001).all(),True)
        self.assertAlmostEqual(df['sum_bid'][0],59684.9775)
        df=tmp.groupby(['PERMNO','date']).agg({'bid':['sum']}).toDF()
        self.assertEqual(df.shape[1],3)
        self.assertEqual(len(df),6047)
        self.assertEqual((origin['BID']==df['sum_bid']).all(),True)
        df=tmp.groupby(['PERMNO','date']).agg({'bid':['sum'],'ask':['sum']}).toDF()
        self.assertEqual(df.shape[1],4)
        self.assertEqual(len(df),6047)
        self.assertEqual((origin['BID']==df['sum_bid']).all(),True)
        self.assertEqual((origin['ASK']==df['sum_ask']).all(),True)
        #df=tmp.groupby(['PERMNO']).agg2([ddb.wsum,ddb.wavg],[('bid','ask')]).toDF()

    def test_table_sql_contextby(self):
        dt=self.s.table(data={'sym': ['A', 'B', 'B', 'A', 'A'], 'vol': [1, 3, 2, 5, 4], 'price': [16, 31, 28, 19, 22]})
        re=dt.contextby('sym').agg({'price':[ddb.sum]}).toDF()
        self.assertEqual((re['sym']==['A','A','A','B','B']).all(),True)
        self.assertEqual((re['sum_price']==[57,57,57,59,59]).all(),True)
        re=dt.contextby(['sym','vol']).agg({'price':[ddb.sum]}).toDF()
        self.assertEqual((re['sym']==['A','A','A','B','B']).all(),True)
        self.assertEqual((re['vol']==[1,4,5,2,3]).all(),True)
        self.assertEqual((re['sum_price']==[16,22,19,28,31]).all(),True)
        #re=dt.contextby('sym').agg2([ddb.wsum,ddb.wavg],[('proce','vol')])
    
    def test_table_sql_pivotby(self):
        dt=self.s.table(data={'sym': ['C','MS','MS','MS','IBM','IBM','C','C','C'],
                              'price': [49.6,29.46,29.52,30.02,174.97,175.23,50.76,50.32,51.29],
                              'qty': [2200,1900,2100,3200,6800,5400,1300,2500,8800],
                              'timestamp':np.arange('2019-06-01','2019-06-10',dtype='datetime64')})
        tbName=dt.tableName()
        re=dt.pivotby(index='timestamp',column='sym',value='price').toDF()
        ddb_script='''
        sym = `C`MS`MS`MS`IBM`IBM`C`C`C				
        price= 49.6 29.46 29.52 30.02 174.97 175.23 50.76 50.32 51.29			
        qty = 2200 1900 2100 3200 6800 5400 1300 2500 8800		
        timestamp = 2019.06.01..2019.06.09
        t2 = table(timestamp, sym, qty, price)
        '''
        self.s.run(ddb_script)
        expected=self.s.run('select price from t2 pivot by timestamp,sym')
        self.assertEqual(re.equals(expected),True)
        re=dt.pivotby(index='timestamp.month()',column='sym',value='last(price)').toDF()
        expected=self.s.run('select last(price) from t2 pivot by timestamp.month(),sym')
        self.assertEqual(re.equals(expected),True)
        re=dt.pivotby(index='timestamp.month()',column='sym',value='count(price)').toDF()
        expected=self.s.run('select count(price) from t2 pivot by timestamp.month(),sym')
        self.assertEqual(re.equals(expected),True)
        self.s.run("undef",tbName)

    def test_table_sql_merge(self):
        dt1=self.s.table(data={'id':[1,2,3,3],'value':[7,4,5,0]})
        dt2=self.s.table(data={'id':[5,3,1],'qty':[300,500,800]})
        tbName=dt2.tableName()
        ddb_script='''
        t1= table(1 2 3 3 as id, 7 4 5 0 as value)
        t2 = table(5 3 1 as id, 300 500 800 as qty)
        '''
        self.s.run(ddb_script)
        re=dt1.merge(right=dt2,on='id').toDF()
        expected=self.s.run('select * from ej(t1,t2,"id")')
        self.assertEqual((re==expected).all().all(),True)
        re=dt1.merge(right=dt2,on='id',how='left').toDF()
        expected=self.s.run('select * from lj(t1,t2,"id")')
        re['qty'].fillna(0,inplace=True)
        expected['qty'].fillna(0,inplace=True)
        self.assertEqual((re==expected).all().all(),True)
        re=dt1.merge(right=dt2,on='id',how='outer').toDF()
        expected=self.s.run('select * from fj(t1,t2,"id")')
        re['id'].fillna(0,inplace=True)
        re['value'].fillna(0,inplace=True)
        re.iloc[:,2].fillna(0,inplace=True)
        re['qty'].fillna(0,inplace=True)
        expected['id'].fillna(0,inplace=True)
        expected['value'].fillna(0,inplace=True)
        expected['t2_id'].fillna(0,inplace=True)
        expected['qty'].fillna(0,inplace=True)
        self.assertEqual(np.array_equal(re['id'],expected['id']),True)
        self.assertEqual(np.array_equal(re['value'],expected['value']),True)
        self.assertEqual(np.array_equal(re.iloc[:,2],expected['t2_id']),True)
        self.assertEqual(np.array_equal(re['qty'],expected['qty']),True)

    def test_table_sql_mergr_asof(self):
        dt1 = self.s.table(data={'id': ['A','A','A','B','B'],
                                 'date': pd.to_datetime(['2017-02-06', '2017-02-08', '2017-02-10', '2017-02-07', '2017-02-09']),
                                 'price': [22, 23, 20, 100, 102]})
        dt2 = self.s.table(data={'id': ['A','A','B','B','B'],
                                 'date': pd.to_datetime(['2017-02-07', '2017-02-10', '2017-02-07', '2017-02-08', '2017-02-10'])})
        ddb_script='''
        t1=table(["A","A","A","B","B"] as id,[2017.02.06,2017.02.08,2017.02.10,2017.02.07,2017.02.09] as date,[22, 23, 20, 100, 102] as price)
        t2=table(["A","A","B","B","B"] as id,[2017.02.07,2017.02.10,2017.02.07,2017.02.08,2017.02.10] as date)
        '''
        self.s.run(ddb_script)
        re=dt2.merge_asof(right=dt1,on=['id','date']).toDF()
        expected=self.s.run('select * from aj(t2,t1,`id`date)')
        self.assertEqual((re['id']==expected['id']).all(),True)
        self.assertEqual((re['date']==expected['date']).all(),True)
        self.assertEqual((re.iloc[:,2]==expected['t1_date']).all(),True)
        self.assertEqual((re['price']==expected['price']).all(),True)
    
    def test_table_sql_merge_cross(self):
        dt1=self.s.table(data={'year':[2010,2011,2012]})
        dt2=self.s.table(data={'ticker':['IBM','C','AAPL']})
        ddb_script='''
        t1 = table(2010 2011 2012 as year);
        t2 = table(`IBM`C`AAPL as ticker);
        '''
        self.s.run(ddb_script)
        re=dt1.merge_cross(dt2).toDF()
        expected=self.s.run('select * from cj(t1,t2)')
        self.assertEqual((re['year']==expected['year']).all(),True)
        self.assertEqual((re['ticker']==expected['ticker']).all(),True)

    def test_table_sql_merge_window(self):
        #dt1=self.s.table(data={'sym':["A","A","B"],
        #                       'time':[np.datetime64('2012-09-30 09:56:06'),np.datetime64('2012-09-30 09:56:07'),np.datetime64('2012-09-30 09:56:06')],
        #                       'price':[10.6,10.7,20.6]})
        #dt2=self.s.table(data={'sym':["A","A","A","A","A","A","A","A","A","A","B","B","B","B","B","B","B","B","B","B"],
        #                       'time':pd.date_range(start='2012-09-30 09:56:01',end='2012-09-30 09:56:10',freq='s').append(pd.date_range(start='2012-09-30 09:56:01',end='2012-09-30 09:56:10',freq='s')),
        #                       'bid':[10.05,10.15,10.25,10.35,10.45,10.55,10.65,10.75,10.85,10.95,20.05,20.15,20.25,20.35,20.45,20.55,20.65,20.75,20.85,20.95],
        #                       'offer':[10.15,10.25,10.35,10.45,10.55,10.65,10.75,10.85,10.95,11.05,20.15,20.25,20.35,20.45,20.55,20.65,20.75,20.85,20.95,21.01],
        #                       'volume':[100,300,800,200,600,100,300,800,200,600,100,300,800,200,600,100,300,800,200,600]})
        #ddb_script='''
        #t1 = table(`A`A`B as sym, 2012.09.30T09:56:06 2012.09.30T09:56:07 2012.09.30T09:56:06 as time, 10.6 10.7 20.6 as price)
        #t2 = table(take(`A,10) join take(`B,10) as sym, take(2012.09.30T09:56:00+1..10,20) as time, [10.05,10.15,10.25,10.35,10.45,10.55,10.65,10.75,10.85,10.95,20.05,20.15,20.25,20.35,20.45,20.55,20.65,20.75,20.85,20.95] as bid, [10.15,10.25,10.35,10.45,10.55,10.65,10.75,10.85,10.95,11.05,20.15,20.25,20.35,20.45,20.55,20.65,20.75,20.85,20.95,21.01] as offer, take(100 300 800 200 600, 20) as volume)
        #'''
        #self.s.run(ddb_script)
        #re=dt1.merge_window(right=dt2,leftBound=-5,rightBound=0,aggFunctions="avg(bid)",on=['sym','time']).toDF()
        #expected=self.s.run('select * from wj(t1,t2,-5:0,<avg(bid)>,`sym`time)')
        #self.assertEqual(re.equals(expected),True)
        #re=dt1.merge_window(right=dt2,leftBound=-5,rightBound=-1,aggFunctions=["wavg(bid,volume)","wavg(offer,volume)"],on=["sym","time"]).toDF()
        #expected=self.s.run('select * from wj(t1,t2,-5:-1,<[wavg(bid,volume), wavg(offer,volume)]>,`sym`time)')
        #self.assertEqual(re.equals(expected),True)
        pass

if __name__ == '__main__':
    unittest.main()
