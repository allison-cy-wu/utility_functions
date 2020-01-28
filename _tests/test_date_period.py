from unittest import TestCase
from utility_functions.date_period import find_time_points


class TestDatePeriod(TestCase):
    def test_find_time_points(self):
        time_points = find_time_points('20200101', [-1, 30, -365])
        print(time_points)
        self.assertEqual(time_points[0], '20191231')
        self.assertEqual(time_points[1], '20200131')
        self.assertEqual(time_points[2], '20190101')
