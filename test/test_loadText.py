import unittest
import dolphindb as ddb
from pandas.testing import assert_frame_equal
from setup import HOST, PORT, WORK_DIR, DATA_DIR


class LoadTextTest(unittest.TestCase):
    @classmethod
    def setUp(cls):
        cls.s = ddb.session()
        cls.s.connect(HOST, PORT, "admin", "123456")

    @classmethod
    def tearDown(cls):
        cls.s = ddb.session()
        cls.s.connect(HOST, PORT, "admin", "123456")

    def test_loadText_param_fileName(self):
        data = DATA_DIR + "/trades.csv"
        tb = self.s.loadText(data)
        rs = self.s.run("select * from loadText('{data}')".format(data=data))
        assert_frame_equal(tb.toDF(), rs)

    def test_loadText_param_delimiter(self):
        data = DATA_DIR + "/trades.csv"
        tb = self.s.loadText(data, ";")
        rs = self.s.run("select * from loadText('{data}', ';')".format(data=data))
        assert_frame_equal(tb.toDF(), rs)
    
    def test_loadText_param_delimiter(self):
        data = DATA_DIR + "/trades.csv"
        with self.assertRaises(TypeError): 
            self.s.loadText(remoteFilePath_ERROR=data, delimiter=";")
        with self.assertRaises(TypeError): 
            self.s.loadText(remoteFilePath=data, delimiter_ERROR=";")
        self.s.loadText(remoteFilePath=data, delimiter=";")
if __name__ == '__main__':
    unittest.main()
