# import unittest
# import dolphindb as ddb
# import numpy as np
# import pandas as pd
# from setup import *
# from functools import partial
# import time
# from threading import Event

# def prepareLocalPublisher(tableName):
#     s = ddb.session()
#     s.connect(HOST, PORT, "admin", "123456")
#     ddb_script = '''
#     n=1000
#     share streamTable(n:0, `sym`date`price1`price2`vol1`vol2, [SYMBOL, DATE, DOUBLE, DOUBLE, INT, INT]) as {stName}
#     syms = rand(`A`B`C`D`E, n)
#     dates = rand(2012.06.01..2012.06.10, n)
#     prices1 = rand(100.0, n)
#     prices2 = rand(100.0, n)
#     vol1 = rand(100, n)
#     vol2 = rand(100, n)
#     {stName}.append!(table(syms, dates, prices1, prices2, vol1, vol2))
#     '''.format(stName=tableName)
#     s.run(ddb_script)
#     s.close()

# def prepareLocalPublisherFilter(tableName, colName):
#     s = ddb.session()
#     s.connect(HOST, PORT, "admin", "123456")
#     ddb_script = '''
#     n=1000
#     share streamTable(n:0, `sym`date`price1`price2`vol1`vol2, [SYMBOL, DATE, DOUBLE, DOUBLE, INT, INT]) as {stName}
#     setStreamTableFilterColumn({stName}, '{filterCol}')
#     syms = rand(`A`B`C`D`E, n)
#     dates = rand(2012.06.01..2012.06.10, n)
#     prices1 = rand(100.0, n)
#     prices2 = rand(100.0, n)
#     vol1 = rand(100, n)
#     vol2 = rand(100, n)
#     {stName}.append!(table(syms, dates, prices1, prices2, vol1, vol2))
#     '''.format(stName=tableName, filterCol=colName)
#     s.run(ddb_script)
#     s.close()

# def writePublisher(tableName):
#     s = ddb.session()
#     s.connect(HOST, PORT, "admin", "123456")
#     ddb_script = '''
#     n=1000
#     syms = rand(`A`B`C`D`E, n)
#     dates = rand(2012.06.01..2012.06.10, n)
#     prices1 = rand(100.0, n)
#     prices2 = rand(100.0, n)
#     vol1 = rand(100, n)
#     vol2 = rand(100, n)
#     objByName('{stName}').append!(table(syms, dates, prices1, prices2, vol1, vol2))
#     '''.format(stName=tableName)
#     s.run(ddb_script)
#     s.close()

# def saveListToDf(df, msg):
#     df.loc[len(df)] = msg 

# def saveTableToDf(df, msg):
#     df['sym'] = msg[0]
#     df['date'] = msg[1]
#     df['price1'] = msg[2]
#     df['price2'] = msg[3]
#     df['vol1'] = msg[4]
#     df['vol2'] = msg[5]

# class TestSubscribe(unittest.TestCase):
#     @classmethod
#     def setUp(cls):
#         cls.s = ddb.session()
#         cls.s.connect(HOST, PORT, "admin", "123456")
#         cls.NODE1 = cls.s.run("getNodeAlias()")
#         cls.NODE2 = cls.s.run("(exec name from rpc(getControllerAlias(), getClusterPerf) where mode = 0 and name != getNodeAlias() order by name)[0]")
#         cls.NODE3 = cls.s.run("(exec name from rpc(getControllerAlias(), getClusterPerf) where mode = 0 and name != getNodeAlias() order by name)[1]")
#         cls.PORT1 = cls.s.run("(exec port from rpc(getControllerAlias(), getClusterPerf) where mode = 0 and name = '{nodeName}')[0]".format(nodeName=cls.NODE1))
#         cls.PORT2 = cls.s.run("(exec port from rpc(getControllerAlias(), getClusterPerf) where mode = 0 and name = '{nodeName}')[0]".format(nodeName=cls.NODE2))
#         cls.PORT3 = cls.s.run("(exec port from rpc(getControllerAlias(), getClusterPerf) where mode = 0 and name = '{nodeName}')[0]".format(nodeName=cls.NODE3))
#         cls.HOST1 = cls.s.run("(exec host from rpc(getControllerAlias(), getClusterPerf) where mode = 0 and name = '{nodeName}')[0]".format(nodeName=cls.NODE1))
#         cls.HOST2 = cls.s.run("(exec host from rpc(getControllerAlias(), getClusterPerf) where mode = 0 and name = '{nodeName}')[0]".format(nodeName=cls.NODE2))
#         cls.HOST3 = cls.s.run("(exec host from rpc(getControllerAlias(), getClusterPerf) where mode = 0 and name = '{nodeName}')[0]".format(nodeName=cls.NODE3))

#     @classmethod
#     def tearDownClass(cls):
#         Event().wait(1)
#         cls.s.close()
    
#     def test_subscribe_host_missing(self):
#         self.s.enableStreaming(SUBPORT)
#         prepareLocalPublisher("st")
#         subDf = pd.DataFrame(columns=('sym', 'date', 'price1', 'price2', 'vol1', 'vol2'))
#         saveListToDf_partial = partial(saveListToDf, subDf)
#         self.assertRaises(TypeError, self.s.subscribe, self.PORT1, saveListToDf_partial, "st", "st_slave", 0)

#     def test_subscribe_host_integer(self):
#         self.s.enableStreaming(SUBPORT)
#         prepareLocalPublisher("st")
#         subDf = pd.DataFrame(columns=('sym', 'date', 'price1', 'price2', 'vol1', 'vol2'))
#         saveListToDf_partial = partial(saveListToDf, subDf)
#         self.assertRaises(TypeError, self.s.subscribe, 1, self.PORT1, saveListToDf_partial, "st", "st_slave", 0)
    
#     def test_subscribe_host_float(self):
#         self.s.enableStreaming(SUBPORT)
#         prepareLocalPublisher("st")
#         subDf = pd.DataFrame(columns=('sym', 'date', 'price1', 'price2', 'vol1', 'vol2'))
#         saveListToDf_partial = partial(saveListToDf, subDf)
#         self.assertRaises(TypeError, self.s.subscribe, np.nan, self.PORT1, saveListToDf_partial, "st", "st_slave", 0)
    
#     def test_subscribe_host_invalid(self):
#         self.s.enableStreaming(SUBPORT)
#         prepareLocalPublisher("st")
#         subDf = pd.DataFrame(columns=('sym', 'date', 'price1', 'price2', 'vol1', 'vol2'))
#         saveListToDf_partial = partial(saveListToDf, subDf)
#         self.assertRaises(RuntimeError, self.s.subscribe, "aaa", self.PORT1, saveListToDf_partial, "st", "st_slave", 0)

#     def test_subscribe_port_float(self):
#         self.s.enableStreaming(SUBPORT)
#         prepareLocalPublisher("st")
#         subDf = pd.DataFrame(columns=('sym', 'date', 'price1', 'price2', 'vol1', 'vol2'))
#         saveListToDf_partial = partial(saveListToDf, subDf)
#         self.assertRaises(TypeError, self.s.subscribe, self.HOST1, np.nan, saveListToDf_partial, "st", "st_slave", 0)


#     def test_subscribe_port_string(self):
#         self.s.enableStreaming(SUBPORT)
#         prepareLocalPublisher("st")
#         subDf = pd.DataFrame(columns=('sym', 'date', 'price1', 'price2', 'vol1', 'vol2'))
#         saveListToDf_partial = partial(saveListToDf, subDf)
#         self.assertRaises(TypeError, self.s.subscribe, self.HOST1, "aaa", saveListToDf_partial, "st", "st_slave", 0)

#     def test_subscribe_tableName_integer(self):
#         self.s.enableStreaming(SUBPORT)
#         prepareLocalPublisher("st")
#         subDf = pd.DataFrame(columns=('sym', 'date', 'price1', 'price2', 'vol1', 'vol2'))
#         saveListToDf_partial = partial(saveListToDf, subDf)
#         self.assertRaises(TypeError, self.s.subscribe, self.HOST1, self.PORT1, saveListToDf_partial, 1, "st_slave", 0)

#     def test_subscribe_tableName_float(self):
#         self.s.enableStreaming(SUBPORT)
#         prepareLocalPublisher("st")
#         subDf = pd.DataFrame(columns=('sym', 'date', 'price1', 'price2', 'vol1', 'vol2'))
#         saveListToDf_partial = partial(saveListToDf, subDf)
#         self.assertRaises(TypeError, self.s.subscribe, self.HOST1, self.PORT1, saveListToDf_partial, np.nan, "st_slave", 0)
#     def test_subscribe_table_not_exist(self):
#         self.s.enableStreaming(SUBPORT)
#         prepareLocalPublisher("st")
#         subDf = pd.DataFrame(columns=('sym', 'date', 'price1', 'price2', 'vol1', 'vol2'))
#         saveListToDf_partial = partial(saveListToDf, subDf)
#         self.assertRaises(RuntimeError, self.s.subscribe, self.HOST1, self.PORT1, saveListToDf_partial, "not_exist", "st_slave", 0)

#     def test_subscribe_actionName_integer(self):
#         self.s.enableStreaming(SUBPORT)
#         prepareLocalPublisher("st")
#         subDf = pd.DataFrame(columns=('sym', 'date', 'price1', 'price2', 'vol1', 'vol2'))
#         saveListToDf_partial = partial(saveListToDf, subDf)
#         self.assertRaises(TypeError, self.s.subscribe, self.HOST1, self.PORT1, saveListToDf_partial, "st", 1, 0)

#     def test_subscribe_actionName_float(self):
#         self.s.enableStreaming(SUBPORT)
#         prepareLocalPublisher("st")
#         subDf = pd.DataFrame(columns=('sym', 'date', 'price1', 'price2', 'vol1', 'vol2'))
#         saveListToDf_partial = partial(saveListToDf, subDf)
#         self.assertRaises(TypeError, self.s.subscribe, self.HOST1, self.PORT1, saveListToDf_partial, "st", 25.485, 0)

#     def test_subscribe_offset_string(self):
#         self.s.enableStreaming(SUBPORT)
#         prepareLocalPublisher("st")
#         subDf = pd.DataFrame(columns=('sym', 'date', 'price1', 'price2', 'vol1', 'vol2'))
#         saveListToDf_partial = partial(saveListToDf, subDf)
#         self.assertRaises(TypeError, self.s.subscribe, self.HOST1, self.PORT1, saveListToDf_partial, "st", "st_slave", "aaa")

#     def test_subscribe_resub_string(self):
#         self.s.enableStreaming(SUBPORT)
#         prepareLocalPublisher("st")
#         subDf = pd.DataFrame(columns=('sym', 'date', 'price1', 'price2', 'vol1', 'vol2'))
#         saveListToDf_partial = partial(saveListToDf, subDf)
#         self.assertRaises(TypeError, self.s.subscribe, self.HOST1, self.PORT1, saveListToDf_partial, "st", "st_slave", 0, "False")

#     def test_subscribe_filter_datatype_incompatible(self):
#         self.s.enableStreaming(SUBPORT)
#         prepareLocalPublisherFilter("st", "sym")
#         subDf = pd.DataFrame(columns=('sym', 'date', 'price1', 'price2', 'vol1', 'vol2'))
#         saveListToDf_partial = partial(saveListToDf, subDf)
#         self.assertRaises(RuntimeError, self.s.subscribe, self.HOST1, self.PORT1, saveListToDf_partial, "st", "st_slave", 0, False, np.array([1, 2, 3]))

#     def test_subscribe_offset_0(self):
#         self.s.enableStreaming(SUBPORT)
#         prepareLocalPublisher("st")
#         subDf = pd.DataFrame(columns=('sym', 'date', 'price1', 'price2', 'vol1', 'vol2'))
#         saveListToDf_partial = partial(saveListToDf, subDf)
#         self.s.subscribe(self.HOST1, self.PORT1, saveListToDf_partial, "st", "st_slave", 0)
#         time.sleep(10)
#         self.assertEqual(subDf.__len__(), 1000)
#         self.s.unsubscribe(self.HOST1, self.PORT1, "st", "st_slave")
    
#     def test_subscribe_offset_negative_1(self):
#         self.s.enableStreaming(SUBPORT)
#         prepareLocalPublisher("st")
#         subDf = pd.DataFrame(columns=('sym', 'date', 'price1', 'price2', 'vol1', 'vol2'))
#         saveListToDf_partial = partial(saveListToDf, subDf)
#         self.s.subscribe(self.HOST1, self.PORT1, saveListToDf_partial, "st", "st_slave", -1)
#         writePublisher("st")
#         time.sleep(10)
#         self.assertEqual(subDf.__len__(), 1000)
#         self.s.unsubscribe(self.HOST1, self.PORT1, "st", "st_slave")

#     def test_subscribe_offset_99(self):
#         self.s.enableStreaming(SUBPORT)
#         prepareLocalPublisher("st")
#         subDf = pd.DataFrame(columns=('sym', 'date', 'price1', 'price2', 'vol1', 'vol2'))
#         saveListToDf_partial = partial(saveListToDf, subDf)
#         self.s.subscribe(self.HOST1, self.PORT1, saveListToDf_partial, "st", "st_slave", 99)
#         time.sleep(10)
#         self.assertEqual(subDf.__len__(), 901)
#         self.s.unsubscribe(self.HOST1, self.PORT1, "st", "st_slave")

#     def test_subscribe_filter_string(self):
#         self.s.enableStreaming(SUBPORT)
#         prepareLocalPublisherFilter("st", "sym")
#         subDf = pd.DataFrame(columns=('sym', 'date', 'price1', 'price2', 'vol1', 'vol2'))
#         saveListToDf_partial = partial(saveListToDf, subDf)
#         self.s.subscribe(self.HOST1, self.PORT1, saveListToDf_partial, "st", "st_slave", 0, False, np.array(["B","C"]))
#         time.sleep(10)
#         expected = self.s.run("exec count(*) from st where sym in `B`C")
#         self.assertEqual(subDf.__len__(), expected)
#         self.s.unsubscribe(self.HOST1, self.PORT1, "st", "st_slave")

#     def test_subscribe_filter_date(self):
#         self.s.enableStreaming(SUBPORT)
#         prepareLocalPublisherFilter("st", "date")
#         subDf = pd.DataFrame(columns=('sym', 'date', 'price1', 'price2', 'vol1', 'vol2'))
#         saveListToDf_partial = partial(saveListToDf, subDf)
#         self.s.subscribe(self.HOST1, self.PORT1, saveListToDf_partial, "st", "st_slave", 0, False, np.array(['2012-06-01','2012-06-02'], dtype="datetime64[D]"))
#         time.sleep(10)
#         expected = self.s.run("exec count(*) from st where date in [2012.06.01, 2012.06.02]")
#         self.assertEqual(subDf.__len__(), expected)
#         self.s.unsubscribe(self.HOST1, self.PORT1, "st", "st_slave")

#     def test_subscribe_msgAsTable_true(self):
#         self.s.enableStreaming(SUBPORT)
#         prepareLocalPublisher("st")
#         subDf = pd.DataFrame(columns=('sym', 'date', 'price1', 'price2', 'vol1', 'vol2'))
#         saveTableToDf_partial = partial(saveTableToDf, subDf)
#         self.s.subscribe(self.HOST1, self.PORT1, saveTableToDf_partial, "st", "st_slave", offset = 0, resub = False, msgAsTable = True)
#         time.sleep(10)
#         self.assertEqual(subDf.__len__(), 1000)
#         self.s.unsubscribe(self.HOST1, self.PORT1, "st", "st_slave")
        
#     def test_multi_subscribe_one_local_table(self):
#         self.s.enableStreaming(SUBPORT)
#         prepareLocalPublisher("st")
#         subDf1 = pd.DataFrame(columns=('sym', 'date', 'price1', 'price2', 'vol1', 'vol2'))
#         subDf2 = pd.DataFrame(columns=('sym', 'date', 'price1', 'price2', 'vol1', 'vol2'))
#         saveListToDf_partial1 = partial(saveListToDf, subDf1)
#         saveListToDf_partial2 = partial(saveListToDf, subDf2)
#         self.s.subscribe(self.HOST1, self.PORT1, saveListToDf_partial1, "st", "st_slave1", 0)
#         self.s.subscribe(self.HOST1, self.PORT1, saveListToDf_partial2, "st", "st_slave2", 0)
#         time.sleep(10)
#         self.assertEqual(subDf1.__len__(), 1000)
#         self.assertEqual(subDf2.__len__(), 1000)
#         self.s.unsubscribe(self.HOST1, self.PORT1, "st", "st_slave1")
#         self.s.unsubscribe(self.HOST1, self.PORT1, "st", "st_slave2")
    
#     def test_multi_subscribe_one_local_table_unsubscribe_one(self):
#         self.s.enableStreaming(SUBPORT)
#         prepareLocalPublisher("st")
#         subDf1 = pd.DataFrame(columns=('sym', 'date', 'price1', 'price2', 'vol1', 'vol2'))
#         subDf2 = pd.DataFrame(columns=('sym', 'date', 'price1', 'price2', 'vol1', 'vol2'))
#         saveListToDf_partial1 = partial(saveListToDf, subDf1)
#         saveListToDf_partial2 = partial(saveListToDf, subDf2)
#         self.s.subscribe(self.HOST1, self.PORT1, saveListToDf_partial1, "st", "st_slave1", 0)
#         self.s.subscribe(self.HOST1, self.PORT1, saveListToDf_partial2, "st", "st_slave2", 0)
#         time.sleep(10)
#         self.assertEqual(subDf1.__len__(), 1000)
#         self.assertEqual(subDf2.__len__(), 1000)
#         # unsunscribe one of subscription
#         self.s.unsubscribe(self.HOST1, self.PORT1, "st", "st_slave1")
#         # continue writing data to publisher
#         writePublisher("st")
#         time.sleep(10)
#         self.assertEqual(subDf1.__len__(), 1000)
#         self.assertEqual(subDf2.__len__(), 2000)
#         self.s.unsubscribe(self.HOST1, self.PORT1, "st", "st_slave2")

#     def test_one_resubscribe_one_local_table(self):
#         pass
#         # self.s.enableStreaming(SUBPORT)
#         # prepareLocalPublisher("st")
#         # subDf = pd.DataFrame(columns = ('sym', 'date', 'price1', 'price2', 'vol1', 'vol2'))
#         # saveListToDf_partial = partial(saveListToDf, subDf)
#         # self.s.subscribe(self.HOST1, self.PORT1, saveListToDf_partial, "st", "st_slave", 0)
#         # time.sleep(10)
#         # self.assertEqual(subDf.__len__(), 1000)
#         # self.s.unsubscribe(self.HOST1, self.PORT1, "st", "st_slave")
#         # time.sleep(5)
#         # self.s.subscribe(self.HOST1, self.PORT1, saveListToDf_partial, "st", "st_slave", -1)
#         # writePublisher("st")
#         # time.sleep(10)
#         # self.assertEqual(subDf.__len__(), 2000)
#         # self.s.unsubscribe(self.HOST1, self.PORT1, "st", "st_slave")
#         # time.sleep(5)
#         # self.s.subscribe(self.HOST1, self.PORT1, saveListToDf_partial, "st", "st_slave", -1)
#         # writePublisher("st")
#         # time.sleep(10)
#         # self.assertEqual(subDf.__len__(), 3000)
#         # self.s.unsubscribe(self.HOST1, self.PORT1, "st", "st_slave")


# if __name__ == '__main__':
#     unittest.main()
