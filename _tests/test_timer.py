from unittest import TestCase
from utility_functions.benchmark import timer
import unittest


class TestTimer(TestCase):
    def test_time(self):
        @timer
        def hello():
            i = 1
            j = 0
            while i < 1000000:
                j = j+i
                i = i+1
        hello()
        self.assertEqual(0, 0)


if __name__ == '__main__':
    unittest.main()