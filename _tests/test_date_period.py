from unittest import TestCase
from utility_functions.date_period import find_time_points, bound_date_check


class TestDatePeriod(TestCase):
    def test_find_time_points(self):
        time_points = find_time_points('20200101', [-1, 30, -365])
        print(time_points)
        self.assertEqual(time_points[0], '20191231')
        self.assertEqual(time_points[1], '20200131')
        self.assertEqual(time_points[2], '20190101')

    def test_bound_date_check(self):
        """Tests to ensure the bound_date_check function is working"""
        tbl = 'ccgds.f_sls_invc_model_vw'
        dt_col_name = 'invc_dt'
        start_date = '20200113'
        end_date = '20200117'
        date_format = 'YYYYMMDD'
        check_max, check_min, max_date, min_date = bound_date_check(tbl,
                                                                    dt_col_name,
                                                                    start_date,
                                                                    end_date,
                                                                    env='PRD',
                                                                    date_format=date_format)

        self.assertLessEqual(min_date, max_date)
        self.assertTrue(min_date)
        self.assertTrue(max_date)
        self.assertTrue(check_min)
        self.assertTrue(check_max)