import argparse
import bitfinex
import pandas as pd
import time
import lib.date_util as date_util
from lib.database import db
from models.facts_bitfinex import FactsBitcoinPrice

COLUMN_NAMES = ['time', 'open', 'close', 'high', 'low', 'volume']
INTERVAL = '1m'
SYMBOL = 'btcusd'
TICK_LIMIT = 1000
GRANULARITY_IN_MIN = 60  # minutes. (1 HOUR = 60 MIN)
UNIT_STEP = 60000  # interval of every minute. multiply it by number of minutes.
STEP = GRANULARITY_IN_MIN * UNIT_STEP
LOCAL_TIMEZONE = 'Asia/Singapore'
SAVE_PATH = './data'
PATH = '{}/bitfinex_{}.csv'.format(SAVE_PATH, SYMBOL)


def convert_to_df(data):
    df = pd.DataFrame(data, columns=COLUMN_NAMES)
    df.drop_duplicates(inplace=True)
    # UTC Time with time zone aware
    df['time'] = pd.to_datetime(df['time'], unit='ms')
    df['time'] = df['time'].dt.tz_localize('UTC')
    df.set_index('time', inplace=False)
    df['time'] = df['time'].dt.tz_convert(LOCAL_TIMEZONE)
    df['id_date'] = df.apply(date_util.convert_time_stamp_to_int_date, axis=1)

    df = df.sort_values(by='time')
    return df


def fetch_historical_candle_data(start, stop, symbol, interval, tick_limit, step):
    # Create api instance
    api_v2 = bitfinex.api_v2()

    data = []
    start_time = time.time()
    print(start)
    while start < stop:
        end = start + step
        res = api_v2.candles(symbol=symbol, interval=interval, limit=tick_limit, start=start, end=end)
        print(res)
        data.extend(res)

        print('Retrieving data from {} to {} for {}'.format(pd.to_datetime(start, unit='ms'),
                                                            pd.to_datetime(end, unit='ms'), symbol))

        start = start + step
        # time.sleep(1.5)
    stop_time = time.time()
    print(stop_time)

    print(stop_time - start_time)
    df = convert_to_df(data)
    return df


def fetch_candle_data(start_date):
    if start_date is None:
        start_date = date_util.get_yesterday_date()

    end_date = date_util.get_todays_date()

    # convert to unix time
    start_date_unix = date_util.convert_date_str_to_unix(str(start_date) + ' 00:00') * 1000
    end_date_unix = date_util.convert_date_str_to_unix(str(end_date) + ' 00:00') * 1000

    df = fetch_historical_candle_data(start_date_unix, end_date_unix, SYMBOL, INTERVAL, TICK_LIMIT, STEP)

    for index, row in df.iterrows():
        FactsBitcoinPrice.load_bitcoin_price(row)

    print('Done downloading data. Saving to .csv.')
    df.to_csv(PATH)
    print(PATH)
    print('Done saving data.')


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--date", help="specify the start date in 'YYYY-MM-DD' to fetch the data")
    args = parser.parse_args()

    start_date = str(args.date) if args.date else None
    fetch_candle_data(start_date)
