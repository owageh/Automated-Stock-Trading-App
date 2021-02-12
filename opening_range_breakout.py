import sqlite3
import config
import alpaca_trade_api as tradeapi
import smtplib
import ssl
from datetime import date, datetime
from timezone import is_dst
from helpers import calculate_quantity


#print(datetime.now())
#from config import EMAIL_ADDRESS, EMAIL_HOST, EMAIL_PASSWORD, EMAIL_PORT
#from pandas.tseries.offsets import Minute

context = ssl.create_default_context()

connection = sqlite3.connect(config.DB_FILE)
connection.row_factory = sqlite3.Row
cursor = connection.cursor()
cursor.execute("""
    SELECT id from strategy where name = 'opening_range_breakout'
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
    end_minute_bar = f"{current_date} 09:45:00-05:00"
else:
    start_minute_bar = f"{current_date} 09:30:00-04:00"
    end_minute_bar = f"{current_date} 09:45:00-04:00"

api = tradeapi.REST(config.API_KEY, config.SECRET_KEY,
                    base_url=config.BASE_URL)

#orders = api.list_orders()
orders = api.list_orders(status='all', after=current_date)
existing_order_symbols = [order.symbol for order in orders if order.status !='canceled']


NY = 'America/New_York'

# start=pd.Timestamp('2020-11-30', tz=NY).isoformat()
# end=pd.Timestamp('2020-11-30', tz=NY).isoformat()

#start = pd.Timestamp(current_date+' 9:30:00-05:00', tz=NY).isoformat()
#end = pd.Timestamp(current_date+' 16:00:00-05:00', tz=NY).isoformat()

messages = []

for symbol in symbols:
    minute_bars = api.get_barset(symbol, 'minute', start=current_date, end=current_date).df
    # print(minute_bars)
    minute_bars.columns = minute_bars.columns.droplevel()
    opening_range_mask = (minute_bars.index >= start_minute_bar) & (
        minute_bars.index < end_minute_bar)
    opening_range_bars = minute_bars.loc[opening_range_mask]
    opening_range_low = opening_range_bars['low'].min()
    opening_range_high = opening_range_bars['high'].max()
    opening_range = opening_range_high - opening_range_low
#    print(opening_range_low)
#    print(opening_range_high)
#    print(opening_range)
    after_opening_range_mask = minute_bars.index >= end_minute_bar
    after_opening_range_bars = minute_bars.loc[after_opening_range_mask]
#    print(after_opening_range_bars)
    after_opening_range_breakout = after_opening_range_bars[
        after_opening_range_bars['close'] > opening_range_high]

    if not after_opening_range_breakout.empty:
        if symbol not in existing_order_symbols:
            # print(after_opening_range_breakout)
            limit_price = after_opening_range_breakout.iloc[0]['close']

            print(
                f"Placing Buy long order for {symbol} at {limit_price}, closed_above {opening_range_high} at {after_opening_range_breakout.iloc[0]}")

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
                        limit_price=limit_price + opening_range,
                    ),
                    stop_loss=dict(
                        stop_price=limit_price - opening_range,
                    )
                )
                messages.append(f"Placing Buy long order for {symbol} at {limit_price}, closed_above {opening_range_high}\n\n{after_opening_range_breakout.iloc[0]}\n\n")
            except Exception as e:
                print(f"Could not submit order {e}")
        else:
            pass
            #print("Already an order for {symbol}, skipping.")

# print(messages)
if len(messages) > 0:
    with smtplib.SMTP_SSL(config.EMAIL_HOST, config.EMAIL_PORT, context=context) as server:
        server.login(config.EMAIL_ADDRESS, config.EMAIL_PASSWORD)
        email_message = f"Subject: Trade Notifications for {current_date}\n\n"
        email_message += '\n\n'.join(messages)

        server.sendmail(config.EMAIL_ADDRESS,
                        config.EMAIL_ADDRESS, email_message)
#        server.sendmail(config.EMAIL_ADDRESS,
#                        config.EMAIL_SMS, email_message)
