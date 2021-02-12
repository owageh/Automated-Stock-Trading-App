import sqlite3
import config
import alpaca_trade_api as tradeapi
import datetime
print('[+] Script started on ', datetime.datetime.now(), '\n')

connection = sqlite3.connect(config.DB_FILE)
connection.row_factory = sqlite3.Row
cursor = connection.cursor()
cursor.execute("""
    SELECT symbol, name FROM stock
""")
rows = cursor.fetchall()
symbols = [row['symbol'] for row in rows]  # list comprehension
api = tradeapi.REST(config.API_KEY, config.SECRET_KEY,
                    base_url=config.BASE_URL)  # or use ENV Vars shown below
assets = api.list_assets()

for asset in assets:
    try:
        if asset.status == 'active' and asset.tradable and asset.symbol not in symbols:
            print(f"Added a new stock {asset.symbol} {asset.name}")
            cursor.execute("INSERT INTO stock (symbol, name, exchange, class, asset_id, easy_to_borrow, marginable, shortable, tradable, status) VALUES (?,?,?,?,?,?,?,?,?,?)", (
                asset.symbol, asset.name, asset.exchange, getattr(asset, 'class'), asset.id, asset.easy_to_borrow, asset.marginable, asset.shortable, asset.tradable, asset.status))
    except Exception as e:
        print(asset.symbol)
        print(e)

connection.commit()
