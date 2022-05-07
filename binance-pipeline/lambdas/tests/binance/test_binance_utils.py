# ./tests/binance/test_binance_utils.py
from datetime import datetime
import unittest
#
from binance.binance_utils import get_date
from binance.binance_utils import TradeIDFinder

class TestTradeIDFinder(unittest.TestCase):
    symbol = 'BNBBTC'
    trade_id_finder = TradeIDFinder(symbol, datetime.now())
    payload_1 = {
        "id": 28457,
        "price": "4.00000100",
        "qty": "12.00000000",
        "quoteQty": "48.000012",
        "time": 1499865549590,
        "isBuyerMaker": True,
        "isBestMatch": True
      }
    payload_2 = {
            "id": 28459,
            "price": "4.00000100",
            "qty": "12.00000000",
            "quoteQty": "48.000012",
            "time": 1499865549790,
            "isBuyerMaker": True,
            "isBestMatch": True
          }

    def test_get_date(self):
        date = get_date(self.payload_1)

        self.assertEqual(date, datetime(2017, 7, 12, 14, 19, 9, 590000))
        return

    def test_estimate_id(self):
        with self.subTest():
            first_id = 15
            last_id = 35
            fraction = 0.8

            estimate = self.trade_id_finder.estimate_id(first_id, last_id, fraction)
            self.assertEqual(estimate, 31)

        with self.subTest():
            first_id = 15
            last_id = 36
            fraction = 0.8

            estimate = self.trade_id_finder.estimate_id(first_id, last_id, fraction)
            self.assertEqual(estimate, 32)
        return

    def test_make_bounds(self):
        bounds = self.trade_id_finder.make_bounds(self.payload_1, self.payload_2)
        asserted_bounds = {
                self.payload_1["id"]: get_date(self.payload_1),
                self.payload_2["id"]: get_date(self.payload_2)
                }

        self.assertEqual(bounds, asserted_bounds)
        return 
