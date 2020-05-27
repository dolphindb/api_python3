import unittest
import dolphindb as ddb
import pandas as pd
import numpy as np
from pandas.testing import assert_frame_equal
from setup import HOST, PORT, WORK_DIR


class DBInfo:
    diskDBName = WORK_DIR + '/testSaveTable'
    table = 'tb'


class SaveTableTest(unittest.TestCase):
    @classmethod
    def setUp(cls):
        cls.s = ddb.session()
        cls.s.connect(HOST, PORT, "admin", "123456")
        dbPath = DBInfo.diskDBName
        script = """
        if(existsDatabase('{dbPath}'))
            dropDatabase('{dbPath}')
        if(exists('{dbPath}'))
            rmdir('{dbPath}', true)
        """.format(dbPath=dbPath)
        cls.s.run(script)

    @classmethod
    def tearDown(cls):
        cls.s = ddb.session()
        cls.s.connect(HOST, PORT, "admin", "123456")

        dbPath = DBInfo.diskDBName
        script = """
        if(existsDatabase('{dbPath}'))
            dropDatabase('{dbPath}')
        if(exists('{dbPath}'))
            rmdir('{dbPath}', true)
        """.format(dbPath=dbPath)
        cls.s.run(script)

    def test_saveTable_disk_unpartitioned(self):
        dbPath = DBInfo.diskDBName
        tbName = DBInfo.table
        n = 1000
        df = pd.DataFrame({"id": np.arange(1, n + 1, 1),
                           "date": pd.date_range("2020.01.01", periods=n),
                           "sym": np.random.choice(["ASA", "SADSA", "WQE"], n),
                           "val": np.random.rand(n) * 100})
        data = self.s.table(data=df, tableAliasName=tbName)
        self.s.saveTable(data, dbPath)
        rs = self.s.loadTable(tbName, dbPath)
        assert_frame_equal(rs.toDF(), df)


if __name__ == '__main__':
    unittest.main()
