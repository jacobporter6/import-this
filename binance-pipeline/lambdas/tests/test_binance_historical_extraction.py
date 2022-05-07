# ./lambdas/tests/test_binance_historical_extraction.py
import unittest
#
from binance_historical_extraction import *
from tests.fixtures import *

class TestBinanceHistoricalExtraction(unittest.TestCase):
    @unittest.expectedFailure
    def test_handler(self):
        self.assertTrue(0)
        return

    def test_make_config(self):
        with self.subTest("Reverse load, optional start_date"):
            self.assertEqual(configuration_1, make_config(start_date="2020-01-01", reverse=True))

        with self.subTest("Reverse load"):
            self.assertEqual(configuration_2, make_config(reverse=True))

        with self.subTest("load from trade_id=0, optional end_date"):
            self.assertEqual(configuration_3, make_config(end_date="2020-01-01"))

        with self.subTest("load from trade_id=0"):
            self.assertEqual(configuration_4, make_config())

        with self.subTest("load from date, optional end_date"):
            self.assertEqual(configuration_5, make_config(start_date="2020-01-01", end_date="2021-02-01"))

        with self.subTest("load from date"):
            self.assertEqual(configuration_6, make_config(start_date="2020-01-01"))

        return

    def test_get_starting_ids(self):
        trade_id = 12
        reserved_concurrency = 4
        chunk_limit = 4

        unfiltered_candidates = [8, 4, 0, -4]
        filtered_candidates = [8, 4, 0]

        with self.subTest("The candidates are filtered"):
            starting_ids = get_starting_ids(trade_id, reserved_concurrency, chunk_limit, reverse=True)
            self.assertEqual(filtered_candidates, starting_ids)

        return

    def test_calculate_delay_seconds(self):
        with self.subTest("round 8.18 up"):
            reserved_concurrency = 7 # running concurrently
            rate_limit = 11 # invocations / minute 
            runtime = 30 # each lambda runs twice / minute
            delay_seconds = calculate_delay_seconds(runtime, reserved_concurrency, rate_limit)

            self.assertEqual(delay_seconds, 9)

        with self.subTest("negative to 0"):
            reserved_concurrency = 2
            rate_limit = 10
            runtime = 15
            delay_seconds = calculate_delay_seconds(runtime, reserved_concurrency, rate_limit)

            self.assertEqual(delay_seconds, 0)

        with self.subTest("integers are not rounded up"):
            reserved_concurrency = 5
            rate_limit = 10
            runtime = 20

            delay_seconds = calculate_delay_seconds(runtime, reserved_concurrency, rate_limit)
            self.assertEqual(delay_seconds, 10)

        return

    def test_get_delay_config(self):
        with self.subTest("10 seconds"):
            delay_config = get_delay_config(10)
            self.assertEqual(delay_config, {"day": 0, "hour": 0, "minute": 0, "second": 10})

        with self.subTest("100 seconds"):
            delay_config = get_delay_config(100)
            self.assertEqual(delay_config, {"day": 0, "hour": 0, "minute": 1, "second": 40})

        with self.subTest("90100 seconds"):
            delay_config = get_delay_config(90100)
            self.assertEqual(delay_config, {"day": 1, "hour": 1, "minute": 1, "second": 40})

        return
