import boto3
import ccxt
import config
from datetime import datetime
import json
import numpy as np
import pandas as pd
from scipy.signal import argrelextrema
import warnings


warnings.filterwarnings('ignore')
pd.set_option('display.max_rows', None) # Display all rows that we call
TESTNET_API = config.TESTNET_API
TESTNET_SECRET = config.TESTNET_SECRET
# Fetch exchange
exchange = ccxt.phemex({
    'enableRateLimit': True,
    'apiKey': TESTNET_API,
    'secret': TESTNET_SECRET,
    #'verbose': True,
})

# Load markets to they populate data faster
markets = exchange.load_markets()

# Specify paper trading environment
sandbox = exchange.set_sandbox_mode(True)

"""
These all work fine, no need to change anything.
"""

# Create RSI
def rsi(df, periods = 14, ema = True):
    """
    Returns a pd.Series with the relative strength index.
    """
    close_delta = df['close'].diff()

    # Make two series: one for lower closes and one for higher closes
    up = close_delta.clip(lower=0)
    down = -1 * close_delta.clip(upper=0)
    
    if ema == True:
        # Use exponential moving average
        ma_up = up.ewm(com = periods - 1, adjust=True, min_periods = periods).mean()
        ma_down = down.ewm(com = periods - 1, adjust=True, min_periods = periods).mean()
    else:
        # Use simple moving average
        ma_up = up.rolling(window = periods, adjust=False).mean()
        ma_down = down.rolling(window = periods, adjust=False).mean()
        
    rsi = ma_up / ma_down
    rsi = 100 - (100/(1 + rsi))
    return rsi        

# Generate bull candles
def bull(df):
    bull = df['close'] > df['open']
    return bull

# Generate bear candles
def bear(df):
    bear = df['close'] < df['open']
    return bear

# Generate peak and valleys
def peak_and_valley(df):
    # Define data.
    x = df['high']
    y = df['low']

    # Return price at which point is located
    high_indx = argrelextrema(x.to_numpy(), np.greater)
    hidx = x[high_indx[0]]

    low_indx = argrelextrema(y.to_numpy(), np.less)
    lidx = y[low_indx[0]]

    # Create df of highs and lows
    h_df = pd.DataFrame()
    h_df['peaks'] = hidx
    h_df = h_df.fillna(method='ffill')

    l_df = pd.DataFrame()
    l_df['valleys'] = lidx
    l_df = l_df.fillna(method='ffill')

    # Create df with price and highs/lows
    df = pd.concat([df,h_df,l_df], axis=1)
    df = df.fillna(method='ffill') 
    return df

def trigger(df):
    # Every time the function is run, it will print checking for signals and print the tail rows. Leave them at 30.
    print('Checking for buy and sell signals')
    print(df.tail(30))

    # Generate last and previous row index for buy/sell signal functionality
    last_row = len(df.index) - 1
    prev = last_row - 1

    # Check for open orders, generate actual buy and sell stop prices, set buy and sell stop price to 0 for later check. 
    open_orders = exchange.fetch_open_orders('LUNA/USD:USD')
    buy_stop = df['valleys'][prev] - (df['valleys'][prev] * .01)
    sell_stop = df['peaks'][prev] + (df['peaks'][prev] * .01)
    buy_stop_price = 0
    sell_stop_price = 0

    # If no open orders, then we can check for signals, we don't want to be in multiple trades at same time per ticker symbol.
    # We're only going off one ticker for now til we get this working, then add in multiples later
    if not open_orders:
        # Long signal
        
        if df['rsi'][prev] > 50 :
        # and df['bull'][prev] == True    
            print('Long signal triggered')
            
            # Create buy order
            buy_order = exchange.create_order('LUNA/USD:USD', 'market', 'buy', 100, None)
            print(buy_order)
            
            # Create buy stop loss and set stop loss price to buy_stop variable above
            buy_stop_order = exchange.create_order('LUNA/USD:USD', 'Stop', 'sell', 100, None, {'stopPx': buy_stop})
            print(buy_stop_order)

            # Get id and stop price to access order and cancel later
            buy_stop_id = buy_stop_order['info']['orderID']
            print('Buy stop ID is: ', buy_stop_id)

            buy_stop_price = buy_stop_order['stopPrice']
            print('Buy stop price is:', buy_stop_price)

            # Return values we will need for next iteration
            # return buy_stop_id, buy_stop_price, buy_or_sell

        # Elif short signal, create short order, short stop loss, and get id/stop price
        
        elif df['rsi'][prev] < 50 :
        # and df['bear'][prev] == True
            print('Short signal triggered')

            sell_order = exchange.create_order('LUNA/USD:USD', 'market', 'sell', 100, None)
            print(sell_order)

            sell_stop_order = exchange.create_order('LUNA/USD:USD', 'Stop', 'buy', 100, None, {'stopPx': sell_stop})
            print(sell_stop_order)

            sell_stop_id = sell_stop_order['info']['orderID']
            print('Sell stop ID is:', sell_stop_id)

            sell_stop_price = sell_stop_order['stopPrice']
            print('Sell stop price is:', sell_stop_price)

            # Return values we will need for next iteration
            # return sell_stop_id, sell_stop_price, buy_or_sell

    #     buy_stop = df['valleys'][prev] - (df['valleys'][prev] * .01)
    #     sell_stop = df['peaks'][prev] + (df['peaks'][prev] * .01)          
    open_orders = exchange.fetch_open_orders('LUNA/USD:USD')
    # If there are open orders, if buy stop price != 0 and buy stop price is less than buy stop value, cancel stop loss and create new one
    
    # Get data from current order
    if open_orders:
        if open_orders[0]['info']['side'] == 'Sell':
            buy_stop_id = open_orders[0]['info']['orderID']
            buy_stop_price = open_orders[0]['stopPrice']
            print(f"Current Trade Type: {open_orders[0]['info']['side']}")
            print(f"buy Stop = {buy_stop} buy Stop Price = {buy_stop_price}")
        elif open_orders[0]['info']['side'] == 'Buy':
            sell_stop_id = open_orders[0]['info']['orderID']
            sell_stop_price = open_orders[0]['stopPrice']
            print(f"Current Trade Type: {open_orders[0]['info']['side']}")
            print(f"sell_Stop = {sell_stop} sell_Stop Price = {sell_stop_price}")

        if buy_stop_price != 0 and buy_stop_price < buy_stop: 
            print('Buy stop changed.')

            cancel_buy_stop = exchange.cancel_order(buy_stop_id, 'LUNA/USD:USD')
            print(cancel_buy_stop)

            buy_stop_order = exchange.create_order('LUNA/USD:USD', 'Stop', 'sell', 100, None, {'stopPx': buy_stop})
            print(buy_stop_order)

            buy_stop_id = buy_stop_order['info']['orderID']
            print(buy_stop_id)

            buy_stop_price = buy_stop_order['stopPrice']
            print('Updated buy stop is: ', buy_stop_price)
        
        elif sell_stop_price != 0 and sell_stop_price > sell_stop:
            print('Sell stop changed')

            cancel_sell_stop = exchange.cancel_order(sell_stop_id, 'LUNA/USD:USD')
            print(cancel_sell_stop)

            sell_stop_order = exchange.create_order('LUNA/USD:USD', 'Stop', 'buy', 100, None, {'stopPx': sell_stop})
            print(sell_stop_order)

            sell_stop_id = sell_stop_order['info']['orderID']
            print(sell_stop_id)

            sell_stop_price = sell_stop_order['stopPrice']
            print('Updated sell stop is: ', sell_stop_price)

def run_bot():
    try:
        print(f"Fetching new bars for {datetime.now().isoformat()}")
        bars = exchange.fetch_ohlcvc('LUNA/USD:USD', timeframe='1h', limit=50) # Fetch ohlcv data
        df = pd.DataFrame(bars[:-1], columns=['timestamp', 'open', 'high', 'low', 'close', 'volume', 'c'])
        df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms')
        
        df = df.assign(rsi = rsi(df, periods=14, ema=True))
        df = df.assign(bull = bull(df))
        df = df.assign(bear = bear(df))
        df = peak_and_valley(df)
        trigger(df)
        print(200)
        return {
            'statusCode': 200,
            'body': json.dumps('Bot execution successful')
        }
    except:
        print(400)
        return {
            'statusCode': 400,
            'body': json.dumps('Bot execution unsuccessful')
        }
run_bot()