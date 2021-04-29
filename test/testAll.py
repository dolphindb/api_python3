import unittest
import os

if __name__ == "__main__":
    test_dir = os.path.join(os.path.dirname(os.path.realpath(__file__)))  # 获取当前工作目录
    discover = unittest.defaultTestLoader.discover(test_dir, pattern="test_*.py")
    with open(test_dir+"/reports/unit_report.txt", "w") as f:
        runner = unittest.TextTestRunner(stream=f, verbosity=2)
        # verbosity = 0 只输出error和fail的traceback
        # verbosity = 1 在0的基础上，在第一行给出每一个用例执行的结果的标识，成功是.，失败是F，出错是E，跳过是S，
        #               类似：.EEEEEEEEEEEEEE.EE.E.EEE.....F......F..
        # verbosity = 2 输出测试结果的所有信息
        runner.run(discover)
