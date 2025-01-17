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
import seaborn as sns

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
    with open('api_keys.json', 'r') as f1,open('secret_keys.json', 'r') as f2:
        api_keys = json.load(f1)
        secret_keys = json.load(f2)
    
    trade_strategy = {'single_sell_ratio':0.1,'single_buy_ratio':0.1}
    avg_unsold_symbols_price_ratio = 0.90
    down_percent_list=[25,30,35,40,45]
    up_percent_list=[10,20,30,40,45]  
    
    def __init__(self,trader):
        assert trader in self.traders.keys(), 'Not valid trading platform!'
        self.trader = trader
        self.all_symbol_price = None
        self.all_klines=None
    
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
        return dataframe with columns: 'open_ts','open','high','low','close','volume','asset_volume','trades','symbol','interval'
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
            return df.astype(dtype={'low':'float64','high':'float64','open_ts':'int64','close':'float64','open':'float64','volume':'float64','asset_volume':'float64','trades':'int64'})
        elif self.trader == 'huobi':
            path = 'market/history/kline'
            params['size'] = params['limit']
            params['period'] = self.intervals[(self.trader,params['interval'])]
            
            df = pd.DataFrame(self._http_get_request(self.traders[self.trader]+path,params)['data'])
            df.rename(columns={"vol": "asset_volume", "amount": "volume","count":"trades","id":"open_ts"},inplace=True)
            df['open_ts'] = df['open_ts']*1000
            df['symbol'] = params['symbol']
            df['interval'] = params['interval']
            return df.astype(dtype={'low':'float64','high':'float64','open_ts':'int64','close':'float64','open':'float64','volume':'float64','asset_volume':'float64','trades':'int64'})
        else:
            raise BaseException
    
    def get_all_klines(self,interval='1d',limit=500, refresh_all_symbols=False,klines_file=None, save_klines=True):
        """
        return dataframe with columns: 'open_ts','open','high','low','close','volume','asset_volume','trades','symbol','interval'
        """
        if klines_file is not None:
            self.all_klines=pd.read_csv(klines_file,dtype={'low':'float64','high':'float64','open_ts':'int64','close':'float64','open':'float64','volume':'float64','asset_volume':'float64','trades':'int64'})
            return self.all_klines
        if not self.all_symbol_price or refresh_all_symbols:
            self.get_all_symbol_price()
        df2 = pd.DataFrame()
        print('getting kline for all symbols...')
        for symbol in tqdm(self.all_symbol_price.keys(),unit='symbol'):
            df1 = self.get_symbol_kline(symbol=symbol,interval=interval, limit = limit)
            df2 = pd.concat([df1,df2])
            time.sleep(0.1)
        self.all_klines = df2.reset_index(drop=True)
        if save_klines:
            self.all_klines.to_csv('all_klines_{}.csv'.format(self.trader),index=False)
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
                    time.sleep(300)
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
            one_record['ts']=int(key)
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
        #s = df['total_demand'].cumsum().sort_values().tail()
        df['min'] = df['ts']/1000/5//60
        df_demand= df.groupby('min',as_index=False).agg({'price': np.mean,'total_demand':np.sum})
        df_demand['min_lead'] = df_demand['min']+1
        df_init_price = (df.assign(rn=df.sort_values(['ts'], ascending=True).groupby(['min']).cumcount()).query('rn == 0'))
        df_final_price = (df.assign(rn=df.sort_values(['ts'], ascending=False).groupby(['min']).cumcount()).query('rn == 0'))
        df_price_growth = df_init_price.merge(df_final_price,on='min')[['min','price_x','price_y']]
        df_price_growth['price_growth'] = df_price_growth['price_y']/df_price_growth['price_x']
        
        df_result = df_demand.merge(df_price_growth, left_on='min_lead', right_on='min')[['total_demand','price_growth']]
        
        plt.figure(1, figsize=(22, 6))
        plt.scatter(df_result['total_demand'],df_result['price_growth'])
        #plt.subplot(131)
        #plt.plot(df_demand['price'])
        #plt.subplot(132)
        #plt.plot(df_demand['total_demand'])
        #plt.suptitle('price vs total demand')
        plt.show()
    
    def price_down_then_up(self,cleaned_klines,down_days=4,up_days=6,down_percent=40,up_percent=10):
        df_low_price=pd.merge(cleaned_klines,cleaned_klines, on=['symbol']).query('open_ts_x > open_ts_y and open_ts_x-open_ts_y<={}*24*3.6 and low_x <= high_y*{}'
                             .format(down_days,1-down_percent/100.0))[['open_ts_x','open_ts_y','low_x','high_y','symbol']]\
                             .rename(columns={"open_ts_x": "ts_low", "open_ts_y": "ts_high","low_x":"low_price",'high_y':'high_price'}).reset_index(drop=True)
        df_low_price['unique_id']=df_low_price.index.values
        df_price_recover=pd.merge(cleaned_klines,df_low_price, on=['symbol']).query('open_ts > ts_low and open_ts-ts_low<={}*24*3.6 and high >= low_price*{}'
                                 .format(up_days,1+up_percent/100.0))[['unique_id','symbol']].drop_duplicates()
        num_price_recover=len(df_price_recover)
        num_symbol_recover=len(df_price_recover['symbol'].drop_duplicates())
        num_symbol_down=len(df_low_price['symbol'].drop_duplicates())
        ratio = 0 if len(df_low_price)==0 else num_price_recover/float(len(df_low_price))
        print(ratio,num_price_recover,num_symbol_recover,num_symbol_down)
        pd.merge(df_low_price.groupby('symbol',as_index=False).agg('size').reset_index(),df_price_recover.groupby('symbol',as_index=False).agg('size').reset_index(),
                 how='left',on='symbol').rename(columns={'0_x':'drop_count','0_y':'recover_count'}).query('drop_count>=30').to_csv('symbol_breakdown_{}_{}.csv'.format(down_percent,up_percent),index=False)
        return ratio,num_price_recover,num_symbol_recover,num_symbol_down

    def find_unsold_symbols_drop_percent(self,down_days=4,up_days=6,down_percent=30,up_percent=10,klines_file=None):
        df=self.get_all_klines(klines_file=klines_file,interval='1h')
        #remove the data for the first day of each symbol
        df['open_ts'] = df['open_ts']/1000000
        df['u_id'] = df.index.values
        df= df.assign(rn=df.sort_values(['open_ts'], ascending=True).groupby(['symbol']).cumcount()).query('rn >= 24').astype(dtype={'low':'float64','high':'float64','open_ts':'int32'})[['open_ts','low','high','symbol','u_id']]
        df_low_price=pd.merge(df,df, on=['symbol']).query('open_ts_x > open_ts_y and open_ts_x-open_ts_y<={}*24*3.6 and low_x < high_y*{}'
                             .format(down_days,1-down_percent/100.0))[['open_ts_x','open_ts_y','low_x','high_y','symbol']]\
                             .rename(columns={"open_ts_x": "ts_low", "open_ts_y": "ts_high","low_x":"low_price",'high_y':'high_price'})
        df_symbols=pd.DataFrame(pd.merge(df,df_low_price, on=['symbol']).query('open_ts > ts_low and open_ts-ts_low<={}*24*3.6 and high > low_price*{}'
                                 .format(up_days,1+up_percent/100.0))['symbol'].drop_duplicates(),columns=['symbol']).reset_index(drop=True)
        df_symbols['unique_id']=df_symbols.index.values
        df1=pd.merge(df_low_price,df_symbols, on='symbol',how='left').query('not unique_id == unique_id')
        df_final_price = df.assign(rn=df.sort_values(['open_ts','u_id'], ascending=[False,False]).groupby(['symbol']).cumcount()).query('rn == 0')
        df2=pd.merge(df1,df_final_price,on='symbol')[['symbol','low_price','low']]
        df2['price_down_ratio']= (df2['low_price']-df2['low'])/df2['low_price']
        avg_price_down_percent=np.mean(df2.groupby('symbol',as_index=False).agg({'price_down_ratio':np.max}).reset_index()['price_down_ratio'])*100
        return avg_price_down_percent
    
    def find_all_price_down_then_up(self,down_days=4,up_days=6,klines_file=None):
        df=self.get_all_klines(klines_file=klines_file,interval='1h')
        #remove the data for the first day of each symbol
        df['open_ts'] = df['open_ts']/1000000
        df= df.assign(rn=df.sort_values(['open_ts'], ascending=True).groupby(['symbol']).cumcount()).query('rn >= 24').astype(dtype={'low':'float64','high':'float64','open_ts':'int32'})[['open_ts','low','high','symbol']]
        a = []
        for down_percent in self.down_percent_list:
            for up_percent in self.up_percent_list:
                print('working on down_percent {} and up_percent {}'.format(down_percent,up_percent))
                ratio,total_occur_count,symbol_recover,symbol_down = self.price_down_then_up(df,down_percent=down_percent,up_percent=up_percent,down_days=down_days,up_days=up_days)
                a.append([down_percent,up_percent,ratio,total_occur_count,symbol_down,symbol_recover/float(symbol_down),ratio*(1+up_percent/100.0)+(1-ratio)*self.avg_unsold_symbols_price_ratio])
        df = pd.DataFrame(a,columns = ['down_percent','up_percent','occur_ratio','total_occur_count','symbol_count','symbol_ratio','expected_return_rate'])
        return df
    
    def find_low_price(self,price_down_up_file,klines_file=None,days=4,save_klines=False):
        df2=pd.read_csv(price_down_up_file,usecols=['down_percent','up_percent','expected_return_rate'],
                    dtype={'down_percent':np.int32,'up_percent':np.int32,'expected_return_rate':np.float64})
        df_best_return= df2.assign(rn=df2.sort_values(['expected_return_rate'], ascending=False).groupby(['down_percent']).cumcount()).query('rn ==0')[['down_percent','up_percent']]
        df=self.get_all_klines(interval='1h',limit=24*(days+1),klines_file=klines_file,save_klines=save_klines)
        df['open_ts'] = df['open_ts']/1000000
        df= df.assign(rn=df.sort_values(['open_ts'], ascending=True).groupby(['symbol']).cumcount()).query('rn >= 24').astype(dtype={'low':'float64','high':'float64','open_ts':'int32'})[['open_ts','low','high','symbol']]
        df_current_price=pd.DataFrame([[k,v] for k,v in self.get_all_symbol_price().items()],columns=['symbol','price'])
        df_all = df[df['open_ts']>=int(time.time()/1000-days*24*3.6)]
        df_output=pd.DataFrame()
        for index,row in df_best_return.iterrows():
            down_percent=row['down_percent']
            print('working on down_percent {}'.format(down_percent))
            df_low_price=pd.merge(df_all,df_current_price, on='symbol').query('price <= high*{}'
                             .format(1-down_percent/100.0))[['symbol','price']].drop_duplicates()
            df_low_price['down_percent']=down_percent
            up_percent=df_best_return.query('down_percent == {}'.format(down_percent)).iat[0,1]
            df_low_price['up_percent']=up_percent
            df_symbol_breakdown=pd.read_csv('symbol_breakdown_{}_{}.csv'.format(down_percent,up_percent),dtype={'drop_count':np.float64,'recover_count':np.float64})
            df_low_price=pd.merge(df_low_price,df_symbol_breakdown,how='left',on='symbol')
            df_low_price['history_ratio']=df_low_price['recover_count']/df_low_price['drop_count']
            df_low_price['sell_price']=df_low_price['price']*(1+df_low_price['up_percent']/100.0)
            df_output=pd.concat([df_low_price,df_output])
        df_output=pd.merge(df_output,df2,on=['down_percent','up_percent'])
        df_highest_ratio=df_output.groupby('symbol',as_index=False).agg({'history_ratio':np.max}).reset_index()
        df_output=df_output.query('history_ratio>0.5 or not history_ratio==history_ratio')
        df_output['history_return_rate']=df_output['history_ratio']*(1+df_output['up_percent']/100.0) + (1-df_output['history_ratio'])*self.avg_unsold_symbols_price_ratio
        df_output['merged_return_rate']=df_output.apply(lambda row: max(row['expected_return_rate'],row['history_return_rate']),axis=1)
        df_output= df_output.assign(rn=df_output.sort_values(['merged_return_rate'], ascending=False).groupby(['symbol']).cumcount()).query('rn ==0')[['symbol','price','sell_price','history_ratio','expected_return_rate','merged_return_rate','up_percent','down_percent']]
        df_output=pd.merge(df_output,df_highest_ratio,on='symbol').query('history_ratio_y>0.5 or not history_ratio_y==history_ratio_y')[['symbol','price','sell_price','history_ratio_x','expected_return_rate','merged_return_rate','up_percent','down_percent']]
        return df_output
    
    def auto_trade(self,symbols=['BTCUSDT'],allow_buy=True,allow_sell=True):
        trade_success_sleep=10
        interval_sleep=5
        while True:
            for symbol in symbols:
                if allow_sell and self._check_sell_condition(symbol):
                    sell_success=self.sell_symbol(symbol,amount)
                    if sell_success:
                        time.sleep(trade_success_sleep)
                    else:
                        time.sleep(interval_sleep)
                if allow_buy and self._check_buy_condition(symbol):
                    buy_success=self.buy_symbol(symbol,amount)
                    if buy_success:
                        time.sleep(trade_success_sleep)
                    else:
                        time.sleep(interval_sleep)
                        
    def _check_sell_condition(self,symbol):
        pass
    
    def _check_buy_condition(self,symbol):
        pass
    
                
            
if __name__== '__main__':
    #Agent('binance').pull_depth_trade(symbol='BTCUSDT')
    #print(datetime.fromtimestamp(1516246476484/1000).strftime('%Y-%m-%d %H-%M'))
    """with open('order_book_BTCUSDT.json', 'r') as f1,open('order_book_BTCUSDT_1.json', 'r') as f2,open('order_book_BTCUSDT_3.json','r') as f3:
        d1 = json.load(f1)
        d2 = json.load(f2)
        d3 = json.load(f3)
    #df = Agent.process_depth_trade({**d1,**d2,**d3})
    df = Agent.process_depth_trade(d3)
    df.to_csv('processed_output.csv')
    Agent.analyze_depth_trade(df)"""
    """df2=pd.DataFrame()
    for down_days in (4):
        for up_days in (6):
            try:
                df=pd.read_csv('price_down_up_{}_{}.csv'.format(down_days,up_days))
            except Exception:
                continue
            df2 = pd.concat([df,df2])
    df2.to_csv('price_down_up_all.csv',index=False)"""
    """print('------------working on down days {} and up days {}'.format(down_days,up_days))
            df=Agent('binance').find_all_price_down_then_up(down_days=down_days,up_days=up_days)
            df['down_days']=down_days
            df['up_days']=up_days
            df.to_csv('price_down_up_{}_{}.csv'.format(down_days,up_days),index=False)"""
    """for i in [3,2,1]:
        print('------------working on file {}'.format(i))
        df=Agent('binance').find_all_price_down_then_up(klines_file='all_klines_binance_{}.csv'.format(i))
        df.to_csv('price_down_up_{}.csv'.format(i),index=False)"""
    df=Agent('binance').find_all_price_down_then_up(klines_file='all_klines_binance.csv')
    df.to_csv('price_down_up.csv',index=False)
    #unsold_drop_percent=Agent('binance').find_unsold_symbols_drop_percent(klines_file='all_klines_binance_3.csv',down_percent=42,up_percent=40)
    #print('average price down percent is {}'.format( unsold_drop_percent))
    df=Agent('binance').find_low_price('price_down_up.csv',klines_file='all_klines_binance.csv')
    df.to_csv('low_price.csv',index=False)
    """df=pd.read_csv('price_up_down.csv')
    plt.figure()
    sns.heatmap(df.pivot('down_percent','up_percent','ratio'))
    plt.figure()
    sns.heatmap(df.pivot('down_percent','up_percent','total_symbol_count'))
    df = Agent('binance').find_low_price()
    df.to_csv('low_price.csv')"""
    """df=pd.read_csv('trades_0307.csv')
    for index, row in df.iterrows():
        print(row['symbol'],max(Agent('binance').get_symbol_kline(symbol=row['symbol'],interval='1h',limit=84).astype(dtype={'high':'float32'})['high'])/float(row['price']))"""
