import pandas as pd
import sqlite3
import config
import alpaca_trade_api as tradeapi
import tulipy
import numpy
import datetime
print('[+] Script started on ', datetime.datetime.now(), '\n')
connection = sqlite3.connect(config.DB_FILE)
cursor = connection.cursor()
cursor.execute("""
  SELECT max(date) as maxdate FROM stock_price join stock ON stock.id = stock_price.stock_id
  WHERE symbol = "MSFT"
""")
TZ = 'America/Toronto'
maxdate_M = pd.Timestamp(pd.to_datetime(
    cursor.fetchone()[0]), tz=TZ).isoformat()
# cursor.fetchone()[0]) + pd.DateOffset(days=1), tz=TZ).isoformat()
if maxdate_M == 'NaT':
    maxdate_M = '2000-01-01'
# quit()
connection.row_factory = sqlite3.Row
cursor = connection.cursor()
cursor.execute("""
    SELECT id, symbol, name FROM stock
    ORDER BY symbol
""")
rows = cursor.fetchall()
symbols = []
stock_dict = {}
for row in rows:
    symbol = row['symbol']
    symbols.append(symbol)
    stock_dict[symbol] = row['id']

api = tradeapi.REST(config.API_KEY, config.SECRET_KEY,
                    base_url=config.BASE_URL)
chunk_size = 200

#symbols = ['MSFT']

for i in range(0, len(symbols), chunk_size):
    symbol_chunk = symbols[i:i+chunk_size]
    barsets = api.get_barset(symbol_chunk, 'day', start=maxdate_M)
    #barsets = api.get_barset(symbol_chunk, 'minute')
    # print(barsets)

    for symbol in barsets:

        cursor = connection.cursor()
        # cursor.execute("""
        # SELECT max(date) as maxdate FROM stock_price join stock ON stock.id = stock_price.stock_id
        # WHERE symbol = ?
        # """, (symbol,))
        # TZ = 'America/Toronto'
        # maxdate = pd.Timestamp(pd.to_datetime(
        #     cursor.fetchone()[0]), tz=TZ).isoformat()
        maxdate = maxdate_M
        # print(f'[|{maxdate}|]')
        print(
            f"processing symbol: {symbol} \tChunk: {i} \tMaxdate: {maxdate} \tBarsets: {len(barsets[symbol])}")
        # print(barsets[symbol])
        recent_closes = [bar.c for bar in barsets[symbol]]

        for bar in barsets[symbol]:
            stock_id = stock_dict[symbol]
            if bar.t.date().isoformat() > maxdate:
                if len(recent_closes) >= 50 and datetime.date.today().isoformat() == bar.t.date().isoformat():
                    sma_20 = tulipy.sma(numpy.array(
                        recent_closes), period=20)[-1]
                    sma_50 = tulipy.sma(numpy.array(
                        recent_closes), period=50)[-1]
                    rsi_14 = tulipy.rsi(numpy.array(
                        recent_closes), period=14)[-1]
                else:
                    sma_20, sma_50, rsi_14 = None, None, None

                cursor.execute("""
                    INSERT INTO stock_price (stock_id, date, open, high, low, close, volume, sma_20, sma_50, rsi_14)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?) 

                """, (stock_id, bar.t.date(), bar.o, bar.h, bar.l, bar.c, bar.v, sma_20, sma_50, rsi_14))
connection.commit()
print('[+] Script ended on ', datetime.datetime.now(), '\n')


# import pandas as pd
# import sqlite3
# import config
# import alpaca_trade_api as tradeapi
# import datetime
# print('[+] Populate Prices Script started on ', datetime.datetime.now(), '\n')
# connection = sqlite3.connect(config.DB_FILE)
# connection.row_factory = sqlite3.Row
# cursor = connection.cursor()
# cursor.execute("""
#     SELECT id, symbol, name FROM stock
# """)
# rows = cursor.fetchall()
# symbols = []
# stock_dict = {}
# for row in rows:
#     symbol = row['symbol']
#     symbols.append(symbol)
#     stock_dict[symbol] = row['id']
# api = tradeapi.REST(config.API_KEY, config.SECRET_KEY,
#                     base_url=config.BASE_URL)
# chunk_size = 200
# for i in range(0, len(symbols), chunk_size):
#     symbol_chunk = symbols[i:i+chunk_size]
#     barsets = api.get_barset(symbol_chunk, 'day')
#     #barsets = api.get_barset(symbol_chunk, 'minute')
#     for symbol in barsets:
#         print(f"processing symbol {symbol}")
#         for bar in barsets[symbol]:
#             stock_id = stock_dict[symbol]
#             cursor.execute("""
#                 INSERT INTO stock_price (stock_id, date, open, high, low, close, volume)
#                 VALUES (?, ?, ?, ?, ?, ?, ?)
#             """, (stock_id, bar.t.date(), bar.o, bar.h, bar.l, bar.c, bar.v))
# connection.commit()
