### Fetch Bitcoin(USD) from given date until yesterday.

### change your path names 
PYTHON_PATH = python3 

SCRIPT_PATH = /Users/bhumikalamba/Desktop/bead_project/bitfinex/bitfinex_instant.py

START_DATE = 'YYYY-MM-DD'

1. PYTHON_PATH SCRIPT_PATH --date START_DATE

2. HOURLY DATA IS DOWNLOADED IN ./data/bitfinex_btcusd.csv. it collects
time_stamp, open, low, high, close price and trade volume data per hour
   from btcusd candles data. 
   
3. candle data is chosen over ticker data as it more accurate.
4. Bitfinex API is used above. This exchange is given 'Very Good' rating on the https://nomics.com/exchanges
5. code is modified from https://github.com/akcarsten/bitfinex_api/blob/master/bitfinex/bitfinex_v2.py 

EXAMPLE: 
python3 /Users/bhumikalamba/Desktop/bead_project/bitfinex/bitfinex_instant.py --date '2021-02-15'