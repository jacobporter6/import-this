# ./lambdas/binance/binance_utils.py
from datetime import datetime, timedelta
import itertools
import typing
#
from binance.binance_api import BinanceQueryAPI, get_latest_trade_id, get_first_trade_id

def get_date(payload: dict):
    time_ms = payload['time']
    time_s = time_ms*1e-3

    return datetime.fromtimestamp(time_s)


def get_trade_id(payload: dict):

    return payload['id']


class TradeIDFinder:
    def __init__(self, symbol, start_date: typing.Optional[datetime]=None):
        self.symbol = symbol
        if start_date:
            self.start_date = start_date - timedelta(days=1)
        else:
            self.start_date = None
        self.query_api = BinanceQueryAPI()

        return

    def make_bounds(self, first_trade_payload: dict, last_trade_payload: dict):
        bounds = dict()
        for payload in [first_trade_payload, last_trade_payload]:
            bounds[get_trade_id(payload)] = get_date(payload)

        return bounds

    def get_fraction_from_days(self, first_date, last_date):
        days_before = (self.start_date - first_date).days
        total_days = (last_date - first_date).days

        return days_before/total_days

    def estimate_id(self, bounds):
        first_id, last_id = bounds.keys()
        fraction = self.get_fraction_from_days(*bounds.values())

        return round(first_id + ((last_id - first_id)*fraction))

    def retrieve_trade_id(self, trade_id) -> dict:
        status_code, trade_id_resp = self.query_api.old_trade_lookup(self.symbol, limit=1, from_id=trade_id)

        if status_code in [418, 429]:
            raise Exception("Cannot run pipeline right now as limit on API reached")

        return trade_id_resp

    def get_trade_id_for_bounds(self, bounds: dict) -> dict:
        estimated_id = self.estimate_id(bounds)
        estimated_id_payload = self.retrieve_trade_id(estimated_id)

        lower_id, upper_id = bounds.keys()

        estimated_id_date = get_date(estimated_id_payload)
        if estimated_id_date > self.start_date:
            new_bounds = {lower_id: bounds['lower_id'], estimated_id: estimated_id_date}
            return self.get_trade_id_for_bounds(new_bounds)

        elif estimated_id_date < self.start_date:
            new_bounds = {estimated_id: estimated_id_date, upper_id: bounds['upper_id']}
            return self.get_trade_id_for_bounds(new_bounds)

        return estimated_id

    def get_trade_id_for_date(self) -> dict:
        last_trade_payload = get_latest_trade_id(self.symbol)
        first_trade_payload = get_first_trade_id(self.symbol)

        bounds = self.make_bounds(first_trade_payload, last_trade_payload)
        trade_id = self.get_trade_id_for_bounds(bounds)

        return trade_id
