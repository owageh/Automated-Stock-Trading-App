
import config
import alpaca_trade_api as tradeapi

api = tradeapi.REST(config.API_KEY, config.SECRET_KEY,
                    base_url=config.BASE_URL)

print("[+] Closing ALL open positions by the end of the day")

# DID NOT Work for some reason so I replaced with the below script order by order.
# response = api.cancel_all_orders()
# print("[+] Closing ALL open positions by the end of the day")
# print(response)

orders = api.list_orders(status='open')
positions = api.list_positions()

if orders or positions:
    if positions:
        print(positions)

    if orders:
        print("Canceling open orders:")
        print([o.id for o in orders])
        result = [api.cancel_order(o.id) for o in orders]
        print(result)

    closed = []
    for p in positions:
        side = 'sell'
        if int(p.qty) < 0:
            p.qty = abs(int(p.qty))
            side = 'buy'
        closed.append(
            api.submit_order(p.symbol, qty=p.qty, side=side,
                             type="market", time_in_force="day")
        )

    if closed:
        print("Submitted Orders", closed)

    for o in closed:
        status = api.get_order(o.id)
        if status.status == 'rejected':
            print("ORDER FAILED: Your Order was Rejected!!!")
