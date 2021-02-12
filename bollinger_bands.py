import sqlite3
import config
import alpaca_trade_api as tradeapi
from datetime import date, datetime
from timezone import is_dst
import tulipy
from helpers import calculate_quantity

# print(datetime.now())

connection = sqlite3.connect(config.DB_FILE)
connection.row_factory = sqlite3.Row
cursor = connection.cursor()
cursor.execute("""
    SELECT id from strategy where name = 'bollinger_bands'
""")
strategy_id = cursor.fetchone()['id']


cursor.execute("""
    SELECT symbol, name
    FROM stock join stock_strategy on stock_strategy.stock_id = stock.id
    WHERE stock_strategy.strategy_id = ?
""", (strategy_id,))
stocks = cursor.fetchall()
symbols = [stock['symbol'] for stock in stocks]
#symbols = ['MSFT']

current_date = date.today().isoformat()
#current_date = '2020-11-30'

if is_dst():
    start_minute_bar = f"{current_date} 09:30:00-05:00"
    end_minute_bar = f"{current_date} 16:00:00-05:00"
else:
    start_minute_bar = f"{current_date} 09:30:00-04:00"
    end_minute_bar = f"{current_date} 16:00:00-04:00"


api = tradeapi.REST(config.API_KEY, config.SECRET_KEY,
                    base_url=config.BASE_URL)

#orders = api.list_orders()
orders = api.list_orders(status='all', after=current_date)
existing_order_symbols = [
    order.symbol for order in orders if order.status != 'canceled']


NY = 'America/New_York'

# start=pd.Timestamp('2020-11-30', tz=NY).isoformat()
# end=pd.Timestamp('2020-11-30', tz=NY).isoformat()

#start = pd.Timestamp(current_date+' 9:30:00-05:00', tz=NY).isoformat()
#end = pd.Timestamp(current_date+' 16:00:00-05:00', tz=NY).isoformat()

messages = []

for symbol in symbols:
    # print(symbol)
    minute_bars = api.get_barset(symbol, 'minute', start=current_date,
                                 limit=1000, end=current_date).df  # limit=1000, can be added
    # print(minute_bars)
    minute_bars.columns = minute_bars.columns.droplevel()
    market_open_mask = (minute_bars.index >= start_minute_bar) & (
        minute_bars.index < end_minute_bar)

    market_open_bars = minute_bars.loc[market_open_mask]

    if len(market_open_bars) >= 20:
        closes = market_open_bars.close.values
        lower, middle, upper = tulipy.bbands(closes, 20, 2)
        # print(market_open_bars.iloc[-1])
        current_candle = market_open_bars.iloc[-1]
        previous_candle = market_open_bars.iloc[-2]
        # print('[+]', current_candle.name, "\t",
        #       current_candle.close, "\t", lower[-1])
        if current_candle.close > lower[-1] and previous_candle.close < lower[-2]:
            print(f"{symbol} closed above lower bollinger band")
            print(current_candle)

            if symbol not in existing_order_symbols:
                limit_price = current_candle.close
                candle_range = current_candle.high - current_candle.low

                try:
                    api.submit_order(
                        symbol=symbol,
                        side='buy',
                        type='limit',
                        qty=calculate_quantity(limit_price),
                        time_in_force='day',
                        order_class='bracket',
                        limit_price=limit_price,
                        take_profit=dict(
                            limit_price=limit_price + (candle_range * 3),
                        ),
                        stop_loss=dict(
                            stop_price=previous_candle.low,
                        )
                    )
                    print(
                        f"Placing Buy long order for {symbol} at {limit_price}")
                except Exception as e:
                    print(f"Could not submit order {e}")
            else:
                pass
                #print("Already an order for {symbol}, skipping.")

# # print(messages)
# if len(messages) > 0:
#     with smtplib.SMTP_SSL(config.EMAIL_HOST, config.EMAIL_PORT, context=context) as server:
#         server.login(config.EMAIL_ADDRESS, config.EMAIL_PASSWORD)
#         email_message = f"Subject: Trade Notifications for {current_date}\n\n"
#         email_message += '\n\n'.join(messages)

#         server.sendmail(config.EMAIL_ADDRESS,
#                         config.EMAIL_ADDRESS, email_message)
#         server.sendmail(config.EMAIL_ADDRESS,
#                         config.EMAIL_SMS, email_message)
