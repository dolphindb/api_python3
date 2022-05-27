import unittest
import dolphindb as ddb
import numpy as np
from setup import *


class TestEanbleStreaming(unittest.TestCase):
    @classmethod
    def setUp(cls):
        cls.s = ddb.session()
        cls.s.connect(HOST, PORT, "admin", "123456")
        cls.NODE1 = cls.s.run("getNodeAlias()")
        cls.NODE2 = cls.s.run("(exec name from rpc(getControllerAlias(), getClusterPerf) where mode = 0 and name != getNodeAlias())[0]")
        cls.NODE3 = cls.s.run("(exec name from rpc(getControllerAlias(), getClusterPerf) where mode = 0 and name != getNodeAlias())[1]")

    @classmethod
    def tearDownClass(cls):
        pass

    def test_enableStreaming_port_string(self):
        self.assertRaises(TypeError, self.s.enableStreaming, "aaa")

    def test_enableStream_port_missing(self):
        self.assertRaises(TypeError, self.s.enableStreaming)

    def test_enableStream_port_nan(self):
        self.assertRaises(TypeError, self.s.enableStreaming, np.nan)
    
    def test_enableStream_port_float(self):
        self.assertRaises(TypeError, self.s.enableStreaming, 25.48)

    def test_enableStream_port_negative_integer(self):
        self.assertRaises(RuntimeError, self.s.enableStreaming, -2)

    def test_enableStream_port_zero(self):
        self.assertRaises(RuntimeError, self.s.enableStreaming, 0)
    
    def test_enableStream_port_zero(self):
        self.assertRaises(RuntimeError, self.s.enableStreaming, 0)


if __name__ == '__main__':
    unittest.main()