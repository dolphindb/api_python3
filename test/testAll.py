import unittest
import os

if __name__ == "__main__":
    test_dir = os.path.join(os.path.dirname(os.path.realpath(__file__)))  # Get the current working directory
    discover = unittest.defaultTestLoader.discover(test_dir, pattern="test_*.py")
    with open(test_dir+"/reports/unit_report.txt", "w") as f:
        runner = unittest.TextTestRunner(stream=f, verbosity=2)
        # verbosity = 0 Return traceback info about error and failure
        # verbosity = 1 Return traceback info about error and failure, and the execution result of each test case. ".": success, F: failure, E: error, S: skip.
        #               such as: .EEEEEEEEEEEEEE.EE.E.EEE.....F......F..
        # verbosity = 2 Return all test results
        runner.run(discover)
