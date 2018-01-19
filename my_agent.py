# -*- coding: utf-8 -*-
"""
Created on Sun Jan  7 19:49:01 2018

@author: Jack Yang
"""

import urllib,requests
import pandas as pd
import numpy as np
from tqdm import tqdm
import time
import json
from datetime import datetime

class Agent():
    INTERVAL_1MIN='1m'
    INTERVAL_1HOUR='1h'
    INTERVAL_1DAY='1d'
    INTERVAL_1WEEK='1w'
    INTERVAL_1MONTH='1M'
    intervals = {('huobi',INTERVAL_1MIN):'1min',('huobi',INTERVAL_1HOUR):'60min',('huobi',INTERVAL_1DAY):'1day',
                 ('huobi',INTERVAL_1WEEK):'1week',('huobi',INTERVAL_1MONTH):'1mon'}
    traders = {'huobi':'https://api.huobi.pro/','bittrex':'https://bittrex.com/api/v1.1/',
                         'binance':'https://api.binance.com/'}
    api_keys = {'binance':'CRejMTuSYokbz2JeNBBo1cb1nt3IOL8d1pcAjeBoq9qavYKuqAQLWfxFMFsqn70o'}
    secret_keys = {'binance': 'OEKkQ2R328yMF4LAGw5K15tBwIuISYzjLpZ7nSQK8qHLWMI9AjGas6t31xvGDhUy'}
    
    def __init__(self,trader):
        assert trader in self.traders.keys(), 'Not valid trading platform!'
        self.trader = trader
    
    def _http_get_request(self,url, params, add_to_headers=None):
        headers = {
                "Content-type": "application/x-www-form-urlencoded",
                'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/39.0.2171.71 Safari/537.36',
        }
        if add_to_headers:
           headers.update(add_to_headers)
        postdata = urllib.parse.urlencode(params)

        try:
            response = requests.get(url, postdata, headers=headers, timeout=20)

            if response.status_code == 200:
                return response.json()
            else:
                return
        except BaseException as e:
                print('Exception is: {}'.format(e))
                print("httpGet failed, detail is:%s,%s" %(response.text,response.status_code))
                return
            
    def get_all_symbol_price(self):
        if self.trader == 'binance':
            path = 'api/v1/ticker/allPrices'
            self.all_symbol_price = {r['symbol']:float(r['price' ]) for r in self._http_get_request(self.traders[self.trader]\
                           +path,{})}
        elif self.trader == 'huobi':
            path = 'v1/common/symbols'
            self.all_symbol_price={}
            huobi_symbols=[r["base-currency"]+r["quote-currency"] for r in self._http_get_request(self.traders[self.trader]\
                           +path,{})['data']]
            print('getting all symbol and price for huobi...')
            for symbol in tqdm(huobi_symbols,unit='huobi_symbol'):
                result = self._http_get_request(self.traders[self.trader]+'market/trade',{'symbol':symbol})
                if result is not None and result['status'] == 'ok':
                    self.all_symbol_price[symbol] = result['tick']['data'][0]['price']
                time.sleep(0.1)
        elif self.trader == 'bittrex':
            path = 'public/getmarketsummaries'
            self.all_symbol_price = { r['MarketName']:float(r['Last']) for r in self._http_get_request(self.traders[self.trader]+path,{})\
                                     ['result'] }
        else:
            raise BaseException
        return self.all_symbol_price
    
    def get_symbol_kline(self,**params):
        """
        params: symbol, interval, limit
        """
        if self.trader == 'binance':
            path = 'api/v1/klines'
            df = pd.DataFrame(self._http_get_request(self.traders[self.trader]+path,params))
            del df[6]
            del df[9]
            del df[10]
            del df[11]
            df.columns=['open_ts','open','high','low','close','volume','asset_volume','trades']
            df['symbol'] = params['symbol']
            df['interval'] = params['interval']
            return df
        elif self.trader == 'huobi':
            path = 'market/history/kline'
            params['size'] = params['limit']
            params['period'] = self.intervals[(self.trader,params['interval'])]
            
            df = pd.DataFrame(self._http_get_request(self.traders[self.trader]+path,params)['data'])
            df.rename(columns={"vol": "asset_volume", "amount": "volume","count":"trades","id":"open_ts"},inplace=True)
            df['open_ts'] = df['open_ts']*1000
            df['symbol'] = params['symbol']
            df['interval'] = params['interval']
            return df
        else:
            raise BaseException
    
    def get_all_klines(self,interval='1d',limit=500):
        self.get_all_symbol_price()
        df2 = pd.DataFrame()
        print('getting kline for all symbols...')
        for symbol in tqdm(self.all_symbol_price.keys(),unit='symbol'):
            df1 = self.get_symbol_kline(symbol=symbol,interval=interval, limit = limit)
            df2 = pd.concat([df1,df2])
            time.sleep(0.1)
        return df2.reset_index()
    
    def get_high_growth_ratio_within(self,days=7,growth_ratio=2):
        """
        return symbols which grow more than growth_ratio within x days starting from the 1st day
        """
        df = self.get_all_klines()
        df_base = (df.assign(rn=df.sort_values(['open_ts'], ascending=True).groupby(['symbol']).cumcount() + 1).query('rn == 1'))
        return pd.merge(df,df_base, on='symbol').query('open_ts_x < open_ts_y +{} and open_ts_x > open_ts_y'.format(days*24*3600*1000))\
            .groupby('symbol').agg({'high_x':np.max,'high_y':np.max}).astype(dtype='float64').query('high_x>high_y*{}'.format(growth_ratio)) 
            
    def pull_order_book(self,symbol,limit=1000,pull_interval=5):
        if self.trader == 'binance':
            path = 'api/v1/depth'
            path2 = 'api/v1/ticker/allPrices'
            r = self._http_get_request(self.traders[self.trader]+path,{'symbol':symbol,'limit':limit})
            previous_bids = [bid[0]+'_'+bid[1] for bid in r['bids']]
            previous_asks = [ask[0]+'_'+ask[1] for ask in r['asks']]
            d = {}
            while True:
                time.sleep(pull_interval)
                try:
                    r = self._http_get_request(self.traders[self.trader]+path,{'symbol':symbol,'limit':limit})
                    price = [i['price'] for i in self._http_get_request(self.traders[self.trader]+path2,{}) if i['symbol'] == symbol][0]
                    bids = [bid[0]+'_'+bid[1] for bid in r['bids']]
                    diff_bids = np.setdiff1d(bids,previous_bids)
                    asks = [ask[0]+'_'+ask[1] for ask in r['asks']]
                except Exception as e:
                    print('error:', e)
                    continue
                diff_asks = np.setdiff1d(asks,previous_asks)
                previous_bids = bids
                previous_asks = asks
                d[int(time.time()*1000)] = {'bids':diff_bids.tolist(),'asks': diff_asks.tolist(),'price':price}
                if len(d)%100 == 0:
                    print('saving file with length {}'.format(len(d)))
                    with open('order_book_{}.json'.format(symbol), 'w') as fp:
                        json.dump(d, fp)
            with open('order_book_{}.json'.format(symbol), 'w') as fp:
                json.dump(d, fp)
                
            
if __name__== '__main__':
    Agent('binance').pull_order_book(symbol='BTCUSDT')
    #print(datetime.fromtimestamp(1516246476484/1000).strftime('%Y-%m-%d %H-%M'))

            
    