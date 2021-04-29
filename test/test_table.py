import unittest
import dolphindb as ddb
import pandas as pd
import numpy as np
from pandas.testing import assert_frame_equal
from setup import HOST, PORT, WORK_DIR, DATA_DIR
from numpy.testing import assert_array_equal, assert_array_almost_equal


class TestTable(unittest.TestCase):
    @classmethod
    def setUp(cls):
        cls.s = ddb.session()
        cls.s.connect(HOST, PORT, "admin", "123456")

    @classmethod
    def tearDownClass(cls):
        pass

    def test_create_table_by_python_dictionary(self):
        data = {'state': ['Ohio', 'Ohio', 'Ohio', 'Nevada', 'Nevada'],
                'year': [2000, 2001, 2002, 2001, 2002],
                'pop': [1.5, 1.7, 3.6, 2.4, 2.9]}
        tmp = self.s.table(data=data, tableAliasName="tmp")
        re = self.s.run("tmp")
        df = pd.DataFrame(data)
        assert_frame_equal(tmp.toDF(), df)
        assert_frame_equal(re, df)

    def test_create_table_by_pandas_dataframe(self):
        data = {'state': ['Ohio', 'Ohio', 'Ohio', 'Nevada', 'Nevada'],
                'year': [2000, 2001, 2002, 2001, 2002],
                'pop': [1.5, 1.7, 3.6, 2.4, 2.9]}
        df = pd.DataFrame(data)
        tmp = self.s.table(data=df, tableAliasName="tmp")
        re = self.s.run("tmp")
        assert_frame_equal(tmp.toDF(), df)
        assert_frame_equal(re, df)

    def test_table_toDF(self):
        tmp = self.s.loadText(DATA_DIR + "/USPrices_FIRST.csv")
        df = self.s.run("select * from loadText('{data}')".format(data=DATA_DIR + "/USPrices_FIRST.csv"))
        self.assertEqual(len(tmp.toDF()), len(df))
        assert_frame_equal(tmp.toDF(), df)
        tbName = tmp.tableName()
        self.s.run("undef", tbName)

    def test_table_showSQL(self):
        tmp = self.s.loadText(DATA_DIR + "/USPrices_FIRST.csv")
        sql = tmp.showSQL()
        tbName = tmp.tableName()
        self.assertEqual(sql, 'select PERMNO,date,SHRCD,TICKER,TRDSTAT,HEXCD,CUSIP,DLSTCD,DLPRC,'
                              'DLRET,BIDLO,ASKHI,PRC,VOL,RET,BID,ASK,SHROUT,CFACPR,CFACSHR,OPENPRC '
                              'from {tbName}'.format(tbName=tbName))
        self.s.run("undef", tbName)

    def test_table_sql_select_where(self):
        data = DATA_DIR + "/USPrices_FIRST.csv"
        tmp = self.s.loadText(data)

        re = tmp.select(['PERMNO', 'date']).where(tmp.date > '2010.01.01')
        df = self.s.run("select PERMNO,date from loadText('{data}') where date>2010.01.01".format(data=data))
        self.assertEqual(re.rows, 1510)
        assert_frame_equal(re.toDF(), df)

        re = tmp.select(['PERMNO', 'date']).where(tmp.date > '2010.01.01').sort(['date desc'])
        df = self.s.run(
            "select PERMNO,date from loadText('{data}') where date>2010.01.01 order by date desc".format(data=data))
        self.assertEqual(re.rows, 1510)
        assert_frame_equal(re.toDF(), df)

        re = tmp[tmp.date > '2010.01.01']
        df = self.s.run("select * from loadText('{data}') where date>2010.01.01".format(data=data))
        self.assertEqual(re.rows, 1510)
        assert_frame_equal(re.toDF(), df)

        tbName = tmp.tableName()
        self.s.run("undef", tbName)

    def test_table_sql_groupby(self):
        data = DATA_DIR + "/USPrices_FIRST.csv"
        tmp = self.s.loadText(data)
        origin = tmp.toDF()

        re = tmp.groupby('PERMNO').agg({'bid': ['sum']}).toDF()
        df = self.s.run("select sum(bid) from loadText('{data}') group by PERMNO".format(data=data))
        self.assertEqual((re['PERMNO'] == 10001).all(), True)
        self.assertAlmostEqual(re['sum_bid'][0], 59684.9775)
        assert_frame_equal(re, df)

        re = tmp.groupby(['PERMNO', 'date']).agg({'bid': ['sum']}).toDF()
        df = self.s.run("select sum(bid) from loadText('{data}') group by PERMNO,date".format(data=data))
        self.assertEqual(re.shape[1], 3)
        self.assertEqual(len(re), 6047)
        self.assertEqual((origin['BID'] == re['sum_bid']).all(), True)
        assert_frame_equal(re, df)

        re = tmp.groupby(['PERMNO', 'date']).agg({'bid': ['sum'], 'ask': ['sum']}).toDF()
        df = self.s.run("select sum(bid),sum(ask) from loadText('{data}') group by PERMNO,date".format(data=data))
        self.assertEqual(re.shape[1], 4)
        self.assertEqual(len(re), 6047)
        self.assertEqual((origin['BID'] == re['sum_bid']).all(), True)
        self.assertEqual((origin['ASK'] == re['sum_ask']).all(), True)
        assert_frame_equal(re, df)

        re = tmp.groupby(['PERMNO']).agg2([ddb.wsum, ddb.wavg], [('bid', 'ask')]).toDF()
        df = self.s.run("select wsum(bid,ask),wavg(bid,ask) from loadText('{data}') group by PERMNO".format(data=data))
        assert_frame_equal(re, df)

    def test_table_sql_contextby(self):
        data = {'sym': ['A', 'B', 'B', 'A', 'A'], 'vol': [1, 3, 2, 5, 4], 'price': [16, 31, 28, 19, 22]}
        dt = self.s.table(data=data, tableAliasName="tmp")

        re = dt.contextby('sym').agg({'price': [ddb.sum]}).toDF()
        df = self.s.run("select sym,sum(price) from tmp context by sym")
        self.assertEqual((re['sym'] == ['A', 'A', 'A', 'B', 'B']).all(), True)
        self.assertEqual((re['sum_price'] == [57, 57, 57, 59, 59]).all(), True)
        assert_frame_equal(re, df)

        re = dt.contextby(['sym', 'vol']).agg({'price': [ddb.sum]}).toDF()
        df = self.s.run("select sym,vol,sum(price) from tmp context by sym,vol")
        self.assertEqual((re['sym'] == ['A', 'A', 'A', 'B', 'B']).all(), True)
        self.assertEqual((re['vol'] == [1, 4, 5, 2, 3]).all(), True)
        self.assertEqual((re['sum_price'] == [16, 22, 19, 28, 31]).all(), True)
        assert_frame_equal(re, df)

        re = dt.contextby('sym').agg2([ddb.wsum, ddb.wavg], [('price', 'vol')]).toDF()
        df = self.s.run("select sym,vol,price,wsum(price,vol),wavg(price,vol) from tmp context by sym")
        assert_frame_equal(re, df)

    def test_table_sql_pivotby(self):
        dt = self.s.table(data={'sym': ['C', 'MS', 'MS', 'MS', 'IBM', 'IBM', 'C', 'C', 'C'],
                                'price': [49.6, 29.46, 29.52, 30.02, 174.97, 175.23, 50.76, 50.32, 51.29],
                                'qty': [2200, 1900, 2100, 3200, 6800, 5400, 1300, 2500, 8800],
                                'timestamp': pd.date_range('2019-06-01', '2019-06-09')}, tableAliasName="tmp")

        re = dt.pivotby(index='timestamp', column='sym', value='price').toDF()
        expected = self.s.run('select price from tmp pivot by timestamp,sym')
        self.assertEqual(re.equals(expected), True)
        assert_frame_equal(re, expected)

        re = dt.pivotby(index='timestamp.month()', column='sym', value='last(price)').toDF()
        expected = self.s.run('select last(price) from tmp pivot by timestamp.month(),sym')
        self.assertEqual(re.equals(expected), True)
        assert_frame_equal(re, expected)

        re = dt.pivotby(index='timestamp.month()', column='sym', value='count(price)').toDF()
        expected = self.s.run('select count(price) from tmp pivot by timestamp.month(),sym')
        self.assertEqual(re.equals(expected), True)
        assert_frame_equal(re, expected)

        tbName = dt.tableName()
        self.s.run("undef", tbName)

    def test_table_sql_merge(self):
        dt1 = self.s.table(data={'id': [1, 2, 3, 3], 'value': [7, 4, 5, 0]}, tableAliasName="t1")
        dt2 = self.s.table(data={'id': [5, 3, 1], 'qty': [300, 500, 800]}, tableAliasName="t2")

        re = dt1.merge(right=dt2, on='id').toDF()
        expected = self.s.run('select * from ej(t1,t2,"id")')
        assert_frame_equal(re, expected)

        re = dt1.merge(right=dt2, on='id', how='left').toDF()
        expected = self.s.run('select * from lj(t1,t2,"id")')
        re.fillna(0, inplace=True)
        expected.fillna(0, inplace=True)
        assert_frame_equal(re, expected)

        re = dt1.merge(right=dt2, on='id', how='outer').toDF()
        expected = self.s.run('select * from fj(t1,t2,"id")')
        re.fillna(0, inplace=True)
        expected.fillna(0, inplace=True)
        assert_frame_equal(re, expected)

        self.s.run("undef", dt1.tableName())
        self.s.run("undef", dt2.tableName())

    def test_table_sql_mergr_asof(self):
        dt1 = self.s.table(data={'id': ['A', 'A', 'A', 'B', 'B'],
                                 'date': pd.to_datetime(
                                     ['2017-02-06', '2017-02-08', '2017-02-10', '2017-02-07', '2017-02-09']),
                                 'price': [22, 23, 20, 100, 102]},
                           tableAliasName="t1")
        dt2 = self.s.table(data={'id': ['A', 'A', 'B', 'B', 'B'],
                                 'date': pd.to_datetime(
                                     ['2017-02-07', '2017-02-10', '2017-02-07', '2017-02-08', '2017-02-10'])},
                           tableAliasName="t2")

        re = dt2.merge_asof(right=dt1, on=['id', 'date']).toDF()
        expected = self.s.run('select * from aj(t2,t1,`id`date)')
        assert_frame_equal(re, expected)

    def test_table_sql_merge_cross(self):
        dt1 = self.s.table(data={'year': [2010, 2011, 2012]}, tableAliasName="t1")
        dt2 = self.s.table(data={'ticker': ['IBM', 'C', 'AAPL']}, tableAliasName="t2")
        re = dt1.merge_cross(dt2).toDF()
        expected = self.s.run('select * from cj(t1,t2)')
        assert_frame_equal(re, expected)

    def test_table_sql_merge_window(self):
        dt1 = self.s.table(data={'sym': ["A", "A", "B"],
                                 'time': [np.datetime64('2012-09-30 09:56:06'), np.datetime64('2012-09-30 09:56:07'),
                                          np.datetime64('2012-09-30 09:56:06')],
                                 'price': [10.6, 10.7, 20.6]},
                           tableAliasName="t1")
        dt2 = self.s.table(
            data={'sym': ["A", "A", "A", "A", "A", "A", "A", "A", "A", "A", "B", "B", "B", "B", "B", "B", "B", "B", "B", "B"],
                  'time': pd.date_range(start='2012-09-30 09:56:01', end='2012-09-30 09:56:10', freq='s').append(
                          pd.date_range(start='2012-09-30 09:56:01', end='2012-09-30 09:56:10', freq='s')),
                  'bid': [10.05, 10.15, 10.25, 10.35, 10.45, 10.55, 10.65, 10.75, 10.85, 10.95, 20.05, 20.15, 20.25,
                          20.35, 20.45, 20.55, 20.65, 20.75, 20.85, 20.95],
                  'offer': [10.15, 10.25, 10.35, 10.45, 10.55, 10.65, 10.75, 10.85, 10.95, 11.05, 20.15, 20.25, 20.35,
                            20.45, 20.55, 20.65, 20.75, 20.85, 20.95, 21.01],
                  'volume': [100, 300, 800, 200, 600, 100, 300, 800, 200, 600, 100, 300, 800, 200, 600, 100, 300, 800,
                             200, 600]},
            tableAliasName="t2")
        re = dt1.merge_window(right=dt2, leftBound=-5, rightBound=0, aggFunctions="avg(bid)", on=['sym', 'time']).toDF()
        expected = self.s.run('select * from wj(t1,t2,-5:0,<avg(bid)>,`sym`time)')
        assert_frame_equal(re, expected)

        re = dt1.merge_window(right=dt2, leftBound=-5, rightBound=-1,
                              aggFunctions=["wavg(bid,volume)", "wavg(offer,volume)"], on=["sym", "time"]).toDF()
        expected = self.s.run('select * from wj(t1,t2,-5:-1,<[wavg(bid,volume), wavg(offer,volume)]>,`sym`time)')
        assert_frame_equal(re, expected)

    def test_table_chinese_column_name(self):
        df = pd.DataFrame({'编号':[1, 2, 3, 4, 5], '序号':['壹','贰','叁','肆','伍']})
        tmp = self.s.table(data=df, tableAliasName="t")
        res=tmp.toDF()
        assert_array_equal(res['编号'], [1, 2, 3, 4, 5])
        assert_array_equal(res['序号'], ['壹','贰','叁','肆','伍'])
        
    def test_table_top_with_other_clause(self):
        df = pd.DataFrame({'id': [10, 8, 5, 6, 7, 9, 1, 4, 2, 3], 'date': pd.date_range('2012-01-01', '2012-01-10', freq="D"), 'value': np.arange(0, 10)})
        tmp = self.s.table(data=df, tableAliasName="t")
        re = tmp.top(3).sort("id").toDF()
        assert_array_equal(re['id'], [1, 2, 3])
        assert_array_equal(re['date'], np.array(['2012-01-07', '2012-01-09', '2012-01-10'], dtype="datetime64[D]"))
        assert_array_equal(re['value'], [6, 8, 9])
        re = tmp.top(3).where("id>5").toDF()
        assert_array_equal(re['id'], [10, 8, 6])
        assert_array_equal(re['date'], np.array(['2012-01-01', '2012-01-02', '2012-01-04'], dtype="datetime64[D]"))
        assert_array_equal(re['value'], [0, 1, 3])
        df = pd.DataFrame({'sym': ["C", "MS", "MS", "MS", "IBM", "IBM", "C", "C", "C"], 
                           'price': [49.6, 29.46, 29.52, 30.02, 174.97, 175.23, 50.76, 50.32, 51.29],
                           'qty':[2200, 1900, 2100, 3200, 6800, 5400, 1300, 2500, 8800]})
        tmp = self.s.table(data=df, tableAliasName="t1")
        re = tmp.top(2).contextby("sym").sort("sym").toDF()
        assert_array_equal(re['sym'], ["C", "C", "IBM", "IBM", "MS", "MS"])
        assert_array_almost_equal(re['price'], [49.6, 50.76, 174.97, 175.23, 29.46, 29.52])
        assert_array_equal(re['qty'], [2200, 1300, 6800, 5400, 1900, 2100])





if __name__ == '__main__':
    unittest.main()
