# ./lambdas/tests/binance/test_binance_api.py
import unittest
#
from binance.binance_api import BinanceQueryAPI

class TestBinanceQueryAPI(unittest.TestCase):
    binance_query_api = BinanceQueryAPI()
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


    @unittest.expectedFailure
    def test_connectivity_test(self):
        response = self.binance_query_api.connectivity_test()

        self.assertIsInstance(response, dict)
        return

    def test_make_bounds(self):
        
        return
