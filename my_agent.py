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
import matplotlib.pyplot as plt

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
            response = None
            response = requests.get(url, postdata, headers=headers, timeout=20)

            if response.status_code == 200:
                return response.json()
            else:
                return
        except BaseException as e:
                print('Exception is: {}'.format(e))
                if response:
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
    
    def get_all_klines(self,interval='1d',limit=500, force_refresh_data=False):
        if not self.all_symbol_price or force_refresh_data:
            self.get_all_symbol_price()
        df2 = pd.DataFrame()
        print('getting kline for all symbols...')
        for symbol in tqdm(self.all_symbol_price.keys(),unit='symbol'):
            df1 = self.get_symbol_kline(symbol=symbol,interval=interval, limit = limit)
            df2 = pd.concat([df1,df2])
            time.sleep(0.1)
        self.all_klines = df2.reset_index(drop=True)
        return self.all_klines
    
    def get_growth_ratio_within(self,days=7,growth_ratio=2,growth_above=True, force_refresh_data=False):
        """
        return symbols which grow more or less than growth_ratio within x days starting from the 1st day
        """
        comparitor = '>' if growth_above else '<'
        if not self.all_klines or force_refresh_data:
            df = self.get_all_klines()
        else:
            df = self.all_klines
        df_base = (df.assign(rn=df.sort_values(['open_ts'], ascending=True).groupby(['symbol']).cumcount() + 1).query('rn == 1'))
        return pd.merge(df,df_base, on='symbol').query('open_ts_x < open_ts_y +{} and open_ts_x > open_ts_y'.format(days*24*3600*1000))\
            .groupby('symbol').agg({'high_x':np.max,'high_y':np.max}).astype(dtype='float64').query('high_x{}high_y*{}'.format(comparitor,growth_ratio)) 
            
    def pull_depth_trade(self,symbol,limit=1000,pull_interval=5):
        if self.trader == 'binance':
            path_depth = 'api/v1/depth'
            path_ticker = 'api/v1/ticker/allPrices'
            path_trade = 'api/v1/aggTrades'
            previous_bids = []
            previous_asks = []
            previous_trade_id = []
            d = {}
            while True:
            #for i in range(3):
                try:
                    price = [i['price'] for i in self._http_get_request(self.traders[self.trader]+path_ticker,{}) if i['symbol'] == symbol][0]
                    r = self._http_get_request(self.traders[self.trader]+path_depth,{'symbol':symbol,'limit':limit})
                    trades = self._http_get_request(self.traders[self.trader]+path_trade,{'symbol':symbol,'limit':limit})
                    bids = [bid[0]+'_'+bid[1] for bid in r['bids']]
                    asks = [ask[0]+'_'+ask[1] for ask in r['asks']]
                    trade_id = [i['a'] for i in trades]
                except Exception as e:
                    print('error:', e)
                    time.sleep(600)
                    continue
                diff_bids = np.setdiff1d(bids,previous_bids)
                diff_asks = np.setdiff1d(asks,previous_asks)
                diff_trade_id = np.setdiff1d(trade_id,previous_trade_id)
                diff_trades = [{'p':i['p'],'q':i['q'],'m':i['m'],'M':i['M']} for i in trades if i['a'] in diff_trade_id]
                previous_bids = bids
                previous_asks = asks
                previous_trade_id = trade_id
                d[int(time.time()*1000)] = {'bids':diff_bids.tolist(),'asks': diff_asks.tolist(),'price':price,'trades':diff_trades}
                if len(d)%100 == 0:
                    print('saving file with length {}'.format(len(d)))
                    with open('order_book_{}.json'.format(symbol), 'w') as fp:
                        json.dump(d, fp)
                time.sleep(pull_interval)
            with open('order_book_{}.json'.format(symbol), 'w') as fp:
                json.dump(d, fp)
    
    def process_depth_trade(data_dict,valid_price_delta=0.1):
        all_records=[]
        for key, value in sorted(data_dict.items()):
            one_record = {}
            one_record['ts']=key
            one_record['price']=float(value['price'])
            valid_bids = [i.split('_') for i in value['bids'] if float(i.split('_')[0]) > one_record['price']*(1-valid_price_delta) ]
            bids_p_diff=np.array([float(i[0]) for i in valid_bids])-one_record['price']
            bids_q=np.array([float(i[1]) for i in valid_bids])
            one_record['bid_demand'] = np.dot(bids_p_diff,bids_q)
            valid_asks = [i.split('_') for i in value['asks'] if float(i.split('_')[0]) < one_record['price']*(1+valid_price_delta) ]
            asks_p_diff=np.array([float(i[0]) for i in valid_asks])-one_record['price']
            asks_q=np.array([float(i[1]) for i in valid_asks])
            one_record['ask_demand'] = np.dot(asks_p_diff,asks_q)
            trades_p_diff=np.array([float(i['p']) for i in value['trades']])-one_record['price']
            trades_q=np.array([float(i['q']) for i in value['trades']])
            one_record['trade_demand'] = np.dot(trades_p_diff,trades_q)
            one_record['trade_count'] = len(trades_q)
            one_record['buyer_maker_count'] = len([i['m'] for i in value['trades'] if i['m']] )
            one_record['best_price_count'] = len([i['m'] for i in value['trades'] if i['M']])
            all_records.append(one_record)
        df = pd.DataFrame(all_records,columns = ['ts', 'price','bid_demand','ask_demand','trade_demand','trade_count','buyer_maker_count',\
                                     'best_price_count'])
        return df
            
    def analyze_depth_trade(df):
        df['total_demand']=df['bid_demand']+df['ask_demand']+df['trade_demand']
        s = df['total_demand'].cumsum().sort_values().tail()
        plt.figure(1, figsize=(22, 6))
        plt.subplot(131)
        plt.plot(df['price'])
        plt.subplot(132)
        plt.plot(df['total_demand'])
        plt.suptitle('price vs trade demand')
        plt.show()
    
                
            
if __name__== '__main__':
    #Agent('binance').pull_depth_trade(symbol='BTCUSDT')
    #print(datetime.fromtimestamp(1516246476484/1000).strftime('%Y-%m-%d %H-%M'))
    with open('order_book_BTCUSDT.json', 'r') as f1,open('order_book_BTCUSDT_1.json', 'r') as f2,open('order_book_BTCUSDT_2.json','r') as f3:
        d1 = json.load(f1)
        d2 = json.load(f2)
        d3 = json.load(f3)
    #df = Agent.process_depth_trade({**d1,**d2,**d3})
    df = Agent.process_depth_trade(d1)
    Agent.analyze_depth_trade(df)

