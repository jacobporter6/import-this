# ./lambdas/binance_historical_extraction.py
from datetime import datetime
import json
import math
import time
#
from binance import BinanceQueryAPI
from binance import TradeIDFinder, get_latest_trade_id, get_first_trade_id

CHUNK_LIMIT = 1e3
DATEFORMAT = "%Y-%m-%d"
RATE_LIMIT: int = 12e3
RESERVED_CONCURRENCY = 4
SQS_QUEUE_NAME_EXTRACTION = ''
SQS_QUEUE_NAME_DELAY_DAY = ''
SQS_QUEUE_NAME_DELAY_HOUR = ''
SQS_QUEUE_NAME_DELAY_MINUTE = ''
SQS_QUEUE_NAME_DELAY_SECOND = ''

def parse_date(date: str) -> datetime:

    return datetime.strptime(date, DATEFORMAT)


def parse_event(payload: dict):
    if (type_ := payload['type']) == 'orchestration':
        # user sent configuration
        handle_orchestration(payload['config'])
    elif type_ == 'api_call':
        # sqs sent the payload to send as GET to API
        handle_api_call(payload['config'])
    else:
        raise Exception("lambda only accepts api_call or orchestration")

    return


def ensure_sqs_trigger(record):
    if (event_source := record["eventSource"]) != "aws:sqs":
        raise Exception(f"Event source must be aws:sqs not {event_source = }")

    return


def handle_orchestration(config: dict):
    start_date: str = config.get('start_date')
    end_date: str = config.get('end_date')
    reverse: bool = config.get('reverse')
    symbol: str = config.get('symbol')

    if reverse:
        handle_reverse_load(symbol, start_date)
    else:
        if not start_date:
            handle_from_start(symbol, end_date)
        else:
            handle_from_date(symbol, start_date, end_date)

    return


def make_config(start_date: str = '', end_date: str = '', reverse: bool = False):
    config: dict = {}
    if start_date:
        config['start_date'] = start_date
    if end_date:
        config['end_date'] = end_date

    config['reverse'] = reverse

    return config


def create_payload(symbol: str, trade_id: int) -> dict:

    return {"symbol": symbol, "limit": CHUNK_LIMIT, "fromId": trade_id}


def send_sqs_message(queue_name: str, message: dict):
    message = json.dumps(message)

    # TO DO send the message to the SQS queue
    #
    #

    return


def handle_reverse_load(symbol: str, start_date: str = ''):
    latest_trade_id = get_latest_trade_id(symbol)['id']

    config = make_config(start_date, reverse=True)
    orchestration_handler_generic(latest_trade_id, symbol, config)

    return


def handle_from_date(symbol: str, start_date: str, end_date: str = ''):
    start_date = parse_date(start_date)

    trade_id_finder = TradeIDFinder(symbol, start_date)
    trade_id = trade_id_finder.get_trade_id_for_date()

    config = make_config(start_date, end_date)
    orchestration_handler_generic(trade_id, symbol, config)

    return


def handle_from_start(symbol, end_date: str = ''):
    trade_id = 0 # assumed true

    config = make_config(end_date)
    orchestration_handler_generic(trade_id, symbol, config)

    return


def orchestration_handler_generic(trade_id, symbol, config):
    reverse = config['reverse']
    starting_ids = get_starting_ids(trade_id, RESERVED_CONCURRENCY, CHUNK_LIMIT, reverse)
    for id_ in starting_ids:
        payload = create_payload(symbol, id_)
        config['payload'] = payload

        message = {"config": config, "type": "api_call"}
        send_sqs_message(SQS_QUEUE_NAME_EXTRACTION, message)


def make_candidate_id(trade_id: int, delta: int, reverse: bool=False):
    if reverse:
        candidate_id = trade_id - delta
    else:
        candidate_id = trade_id + delta

    return candidate_id


def get_starting_ids(trade_id, concurrency, chunk_size, reverse=False):
    starting_ids = []
    for i in range(1, concurrency + 1):
        delta = i*chunk_size
        if (candidate_id := make_candidate_id(trade_id, delta, reverse)) >= 0:
            starting_ids.append(candidate_id)

    return starting_ids


def handle_api_call(config: dict):
    start = time.perf_counter()
    payload = config['payload']

    binance_query_api = BinanceQueryAPI()

    status_code, res = binance_query_api.old_trade_lookup(
                payload['symbol'], CHUNK_LIMIT, payload['fromId']
            )

    if status_code in [418, 429]:
        handle_rate_limit(status_code, res, config)

    # TO DO: store result to file
    #
    #

    else:
        delta = RESERVED_CONCURRENCY * CHUNK_LIMIT
        next_trade_id = make_candidate_id(payload['fromId'], delta, config['reverse'])

        if (len(res) == CHUNK_LIMIT) and (next_trade_id >= 0):
            config['payload']['fromId'] = next_trade_id
            message = {'config': config, 'type': 'api_call'}
            end = time.perf_counter()

            runtime = end - start
            delay_seconds = calculate_delay_seconds(runtime, RESERVED_CONCURRENCY, RATE_LIMIT)

            if delay_seconds:
                delay_config = get_delay_config(delay_seconds)
                message = {
                            "status_code": status_code,
                            "delay_config": delay_config,
                            "config": config
                        }
                send_rate_throttle_message(message)
            else:
                send_sqs_message(SQS_QUEUE_NAME_EXTRACTION, message)

    return


def calculate_delay_seconds(runtime: int, reserved_concurrency: int, rate_limit: int) -> int:
    delay_seconds = math.ceil(((60 * reserved_concurrency)/rate_limit) - runtime)
    delay_seconds = max(0, delay_seconds)

    return delay_seconds


def handle_rate_limit(status_code: int, api_response: dict, config: dict):
    """
    when asked to wait x seconds before retrying API for throttling or ban, send to the delay queue
    which will feed a separate delay lambda. The delay lambda will decrement the penalties and refeed
    the delay queue until there are none left. When penalty time has expired, that delay lambda will
    feed this lambda once again with the failed query config.
    """
    delay_config = get_delay_config(api_response['retry_after'])
    message = {"status_code": status_code, "delay_config": delay_config, "config": config}
    send_rate_throttle_message(message)

    return


def send_rate_throttle_message(message: dict):
    delay_config = message['delay_config']
    penalty_types = ['day', 'hour', 'minute', 'second']
    sqs_queue_names = map(lambda x: f'SQS_QUEUE_NAME_DELAY_{x.upper()}', penalty_types)
    penalties = map(lambda x: delay_config[x], penalty_types)

    for penalty, queue_name in zip(penalties, sqs_queue_names):
        if penalty:
            send_sqs_message(queue_name, message)
            break
    return


def get_delay_config(retry_seconds):
    unix_datetime = datetime.fromtimestamp(retry_seconds)

    delay_config = {
                "day": unix_datetime.day - 1,
                "hour": unix_datetime.hour - 1,
                "minute": unix_datetime.minute,
                "second": unix_datetime.second
            }

    return delay_config

def handle_record(record: dict):
    payload = json.loads(record['body'])
    parse_event(payload)

    return

def handler(event, _):
    for record in event['Records']:
        ensure_sqs_trigger(record)
        handle_record(record)

    return {"status_code": 200}
