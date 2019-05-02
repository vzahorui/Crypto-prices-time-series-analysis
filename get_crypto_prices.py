#!/usr/bin/env python

'''

The following script obtains crypto prices data from the free REST API of CryptoCompare (https://www.cryptocompare.com).
In order to use it one has to register and generate personal token which will be used for making web-requests.

'''


import os
import glob
import requests
from datetime import datetime, timezone, timedelta

import pandas as pd

'''

This is the core function which retrieves required data for a given cryptocurrency and time period based on the already available data.
Retrieved data is merged with existing data (if any available) and then exportend into a .csv file. Old file gets deleted.

'''

def get_currency_data(currency, start_date, end_date=None):
    
    holder = [] # holder for keeping separate DataFrames

    start_time = int(datetime.strptime(start_date, '%Y-%m-%d').replace(tzinfo=timezone.utc).timestamp() ) # given start time in UTC
    if end_date==None:
        end_time = int(datetime.timestamp(datetime.now(tz=timezone.utc)-timedelta(hours=1))) # current time in UTC
    else:
        end_time = int(datetime.strptime(end_date, '%Y-%m-%d').replace(tzinfo=timezone.utc).timestamp() ) # given end time in UT     
    
    def api_call(currency, end_time): # function for obtaining data from API
        url = 'https://min-api.cryptocompare.com/data/histohour'
        params = {
            'fsym': currency,
            'tsym': 'USD',
            'limit': 2000,
            'toTs': end_time,
            'api_key': 'XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX' # place for personal CryptoCompare token
        }
        response = requests.get(url, params = params)
        return response.json()
    
    # checking existing files for previously available data
    f_holder = glob.glob(f'{currency}*.csv') # file for a given currency
    if f_holder != []: 
        avail_startdate, avail_enddate = f_holder[0][11:24], f_holder[0][26:39] # datetime period embedded into the name of the file (strings)
        avail_starttime = int(datetime.strptime(avail_startdate, '%Y-%m-%d-%H').replace(tzinfo=timezone.utc).timestamp() ) # UNIX timestamp for available start time in UTC
        avail_endtime = int(datetime.strptime(avail_enddate, '%Y-%m-%d-%H').replace(tzinfo=timezone.utc).timestamp() ) # UNIX timestamp for available end time in UTC
        
        avail_df = pd.read_csv(f_holder[0]) # load existing data
        avail_df['time'] = pd.to_datetime(avail_df['time'])    
    
        # adding both prior and latter time 
        if start_time < avail_starttime and end_time > avail_endtime:
            # 1st part
            aux_time = avail_starttime
            while start_time < aux_time:
                data = api_call(currency, aux_time)
                holder.append(pd.DataFrame(data['Data']))
                aux_time = data['TimeFrom'] # assigning new upper bound timestamp from JSON provided by previous request 
            # 2nd part
            aux_time = end_time
            while aux_time > avail_endtime:
                data = api_call(currency, aux_time)
                holder.append(pd.DataFrame(data['Data']))
                aux_time = data['TimeFrom']
        # adding prior time only
        elif start_time < avail_starttime and end_time <= avail_endtime:
            aux_time = avail_starttime
            while start_time < aux_time:
                data = api_call(currency, aux_time)
                holder.append(pd.DataFrame(data['Data']))
                aux_time = data['TimeFrom']
        # adding latter time only
        elif start_time >= avail_starttime and end_time > avail_endtime:
            aux_time = end_time
            while aux_time > avail_endtime:
                data = api_call(currency, aux_time)
                holder.append(pd.DataFrame(data['Data']))
                aux_time = data['TimeFrom']
    
    
    else: # in case of no previously available data
        aux_time = end_time
        while aux_time > start_time:
            data = api_call(currency, aux_time)
            holder.append(pd.DataFrame(data['Data']))
            aux_time = data['TimeFrom']
    
    if holder == []:
        return
    else:
        df = pd.concat(holder, axis = 0, sort=False) # merging all loaded datasets into a single DataFrame
        df = df[['time', 'open', 'high', 'low', 'close']]
        df = df[df['time']>=start_time] # removing datapoints earlier than start_time
        df['time'] = pd.to_datetime(df['time'], unit='s')
        
        if f_holder != []: # merging with previously available data
            df = pd.concat([df, avail_df], axis = 0, sort=False)
        
        df.drop_duplicates(inplace=True)
        df.set_index('time', inplace=True) 
        df.sort_index(inplace=True, ascending=False)
        
        min_dt = min(df.index).strftime("%Y-%m-%d-%Hh")
        max_dt = max(df.index).strftime("%Y-%m-%d-%Hh")
        df.to_csv(f'{currency}_prices_{min_dt}-{max_dt}.csv')
        
        files = glob.glob(f'{currency}*.csv') # files with data (old+new)
        files.sort(key=os.path.getctime) # sort by creation date
        if len(files) == 2:
            os.remove(files[0]) # remove old file if there is one
            

'''

The following function deals with multiple choices of selecting cryptocurrencies (one or a set)

'''

def get_data(coins, start_date, end_date=None):
    if type(coins) == str: # one cryptocyrrency selected as a string
        get_currency_data(coins, start_date, end_date)
    elif type(coins) == list: # several or one cryptocurrency selected as a list
        coins_holder = []
        for c in coins:
            get_currency_data(c, start_date, end_date) 
