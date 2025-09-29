from SmartApi import SmartConnect
import os
import urllib
import json
import pandas as pd
import datetime as dt
import time
import pytz
from pyotp import TOTP
from datetime import datetime
import pytz
from in_out import *


def get_internet_time():
    """
    Returns the current UTC time as a datetime object.
    """
    utc_time = datetime.now(pytz.utc)
    return utc_time


def generate_totp(key):
    internet_time = get_internet_time()
    unix_timestamp = int(
        internet_time.timestamp()
    )
    totp = TOTP(key)
    current_otp = totp.at(unix_timestamp)
    return current_otp

def get_connection():
    key_secret = read(r"/mnt/d/personal/One/credentials.txt").split()
    obj = SmartConnect(api_key=key_secret[0])
    data = obj.generateSession(key_secret[2], key_secret[3], generate_totp(key_secret[4]))

    instrument_url = "https://margincalculator.angelbroking.com/OpenAPI_File/files/OpenAPIScripMaster.json"
    response = urllib.request.urlopen(instrument_url)
    instrument_list = json.loads(response.read())
    return obj, data, instrument_list

def token_lookup(ticker, instrument_list, exchange="NSE"):
    for instrument in instrument_list:
        if (
            instrument["name"] == ticker
            and instrument["exch_seg"] == exchange
            and instrument["symbol"].split("-")[-1] == "EQ"
        ):
            return instrument["token"]


def symbol_lookup(token, instrument_list, exchange="NSE"):
    for instrument in instrument_list:
        if (
            instrument["token"] == token
            and instrument["exch_seg"] == exchange
            and instrument["symbol"].split("-")[-1] == "EQ"
        ):
            return instrument["name"]

def all_equities(instrument_list):
    eq_list = []
    for l in instrument_list:
        if l["symbol"].split("-")[-1] == "EQ":
            eq_list.append(l)
    
    return eq_list


def hist_data_extended(obj, ticker, duration, interval, instrument_list, exchange="NSE"):
    st_date = dt.date.today() - dt.timedelta(duration)
    end_date = dt.date.today() - dt.timedelta(1)
    st_date = dt.datetime(st_date.year, st_date.month, st_date.day, 3, 30)
    end_date = dt.datetime(end_date.year, end_date.month, end_date.day)
    df_data = pd.DataFrame(columns=["date", "open", "high", "low", "close", "volume"])

    temp_st_date = st_date
    temp_end_date = st_date + dt.timedelta(30)

    while temp_end_date < end_date:
        time.sleep(0.4)  # avoiding throttling rate limit
        params = {
            "exchange": exchange,
            "symboltoken": token_lookup(ticker, instrument_list),
            "interval": interval,
            "fromdate": (temp_st_date).strftime("%Y-%m-%d %H:%M"),
            "todate": (temp_end_date).strftime("%Y-%m-%d %H:%M"),
        }
        hist_data = obj.getCandleData(params)
        temp = pd.DataFrame(
            hist_data["data"],
            columns=["date", "open", "high", "low", "close", "volume"],
        )
        df_data = pd.concat([
            df_data,
            temp
        ])
        # end_date = dt.datetime.strptime(temp["date"].iloc[0][:16], "%Y-%m-%dT%H:%M")
        # if (
        #     len(temp) <= 1
        # ):  # this takes care of the edge case where start date and end date become same
        #     break

        temp_st_date = temp_end_date + dt.timedelta(1)
        temp_end_date = temp_st_date + dt.timedelta(30)
        if temp_end_date > end_date:
            temp_end_date = end_date

    df_data.set_index("date", inplace=True)
    df_data.index = pd.to_datetime(df_data.index)
    df_data.index = df_data.index.tz_localize(None)
    df_data.drop_duplicates(keep="first", inplace=True)
    return df_data

def hist_data(obj, tickers,duration,interval,instrument_list,exchange="NSE"):
    """
    intervals: ONE_MINUTE, THREE_MINUTE, FIVE_MINUTE, TEN_MINUTE, FIFTEEN_MINUTE, THIRTY_MINUTE, ONE_HOUR, and ONE_DAY.
    """

    hist_data_tickers = {} 
    for ticker in tickers:
        params = {
                 "exchange": exchange,
                 "symboltoken": token_lookup(ticker,instrument_list),
                 "interval": interval,
                 "fromdate": (dt.date.today() - dt.timedelta(duration)).strftime('%Y-%m-%d %H:%M'),
                 "todate": dt.datetime.now().strftime('%Y-%m-%d %H:%M')  
                 }
        hist_data = obj.getCandleData(params)
        df_data = pd.DataFrame(hist_data["data"],
                               columns = ["date","open","high","low","close","volume"])
        df_data.set_index("date",inplace=True)
        df_data.index = pd.to_datetime(df_data.index)
        df_data.index = df_data.index.tz_localize(None)
        hist_data_tickers[ticker] = df_data
    return hist_data_tickers