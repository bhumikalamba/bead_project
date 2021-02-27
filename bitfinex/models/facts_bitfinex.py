import psycopg2
from psycopg2 import sql
from lib.database import db

class FactsBitcoinPrice:
    def load_bitcoin_price(row):
        query = sql.SQL(""" INSERT INTO btc_price_data
                            (time_stamp, id_date, open, high, low, close, volume)
                            VALUES ({time}, {id_date}, {open}, {high}, {low},
                             {close}, {volume})""").format(time = sql.Literal(row['time']),
                                                          id_date = sql.Literal(row['id_date']),
                                                          open=sql.Literal(row['open']),
                                                          high = sql.Literal(row['high']),
                                                          low=sql.Literal(row['low']),
                                                          close=sql.Literal(row['close']),
                                                          volume = sql.Literal(row['volume']))
        return db().execute(query)
