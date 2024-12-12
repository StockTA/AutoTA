#!/usr/bin/env python
# coding: utf-8

# In[ ]:


# AutoTA using FMP Service(JSON)
# Code by Emmett Lange.  All rights reserved.

##### IMPORT LIBRARIES #####
#import datetime, time
from datetime import datetime
from datetime import date
from datetime import timedelta
from datetime import time
#from datetime import timezone
from dateutil.relativedelta import relativedelta
#from zoneinfo import ZoneInfo
import os
#import time

import pandas as pd
import urllib, json
import requests
from urllib.request import urlopen
import pandas_datareader as pdr
import pandas_ta as ta
import math
import numpy as np
from scipy.stats import zscore
import sqlite3 as sql
import sys # required for command line parameters
import logging
logger = logging.getLogger()
import warnings
warnings.filterwarnings("ignore", category=FutureWarning)  # Used to suppress annoying CONCAT empty dataframe warning

##### NOTES #####
# %Y: Four-digit year, %y: Two-digit year, %m: Two-digit month, %d: Two-digit day, %H: Hour, %M: Minute, %S: Second, %B: Month name
# S&P 500:  https://financialmodelingprep.com/api/v3/historical-chart/1day/%5EGSPC?apikey=qJ9JoGI3SnyzfYK4UwHiXQjormGreJWC
# VIX:  https://financialmodelingprep.com/api/v3/historical-chart/1day/%5EVIX?apikey=qJ9JoGI3SnyzfYK4UwHiXQjormGreJWC
# TNX:  https://financialmodelingprep.com/api/v3/historical-chart/1day/%5ETNX?apikey=qJ9JoGI3SnyzfYK4UwHiXQjormGreJWC
# All Treasury rates:  https://financialmodelingprep.com/api/v4/treasury?from={start_dt_1m:%Y-%m-%d}&apikey={api_key}
# FX:
#    EURUSDHistory_ar = get_jsonparsed_data(f"https://financialmodelingprep.com/api/v3/historical-price-full/EURUSD?from={start_dt_1m:%Y-%m-%d}&apikey={api_key}")
#    EURUSDHistory_df = pd.DataFrame(EURUSDHistory_ar['historical'])
#    EURUSDHistory_df['date'] = pd.to_datetime(EURUSDHistory_df['date'])
# ETFList_df.head()
# Use [0] when assigning discrete value from dataframe row; otherwise, Pandas assumes it's a series/dataframe
# https://financialmodelingprep.com/api/v3/historical-chart/1day/FBTC?from=2023-12-02&apikey=qJ9JoGI3SnyzfYK4UwHiXQjormGreJWC


##### FUNCTIONS #####

# Function to get the Data
def get_jsonparsed_data(url):
    res = urlopen(url)
    data = res.read().decode("utf-8")
    return json.loads(data)

def is_in_time_range(start_time, end_time, current_time=None):
    if current_time is None:
      current_time = datetime.now().time()

    if start_time <= end_time:
      return start_time <= current_time <= end_time
    else:
      # Time range crosses midnight (e.g., 22:00 - 02:00)
      return current_time >= start_time or current_time <= end_time

def std_dev_from_residuals(x, y, slope, intercept):
    """Calculates the standard deviation of residuals given data and linear regression parameters."""
    y_pred = slope * x + intercept
    residuals = y - y_pred
    return np.std(residuals)

def anchored_vwap(df, anchor_date):

    df = df[df['date'] >= anchor_date].copy()
    df['Typical Price'] = (df['high'] + df['low'] + df['close']) / 3
    df['Cum_Volume'] = df['volume'].cumsum()
    df['Cum_TPV'] = (df['Typical Price'] * df['volume']).cumsum()
    df['VWAP'] = df['Cum_TPV'] / df['Cum_Volume']
    return df['VWAP']

def macro_regime(dxy_df, tnx_df, spread_df):

    SpreadTrend = 0
    USDTrend = 0
    US10YTrend = 0
    Regime = 0

    dxyclose = round(dxy_df['close'].iloc[-1], 3)
    tnxclose = round(tnx_df['close'].iloc[-1], 3)
    spreadclose = round(spread_df['spread'].iloc[-1], 3)
    dxyavg = dxy_df['DXYEMA5D'].iloc[-1]
    tnxavg = tnx_df['10YEMA5D'].iloc[-1]
    spreadavg = spread_df['spreadEMA5D'].iloc[-1]

    # Determine trends
    if dxyclose >= dxyavg:
      USDTrend = 1
    else:
      USDTrend = -1

    if tnxclose >= tnxavg:
      US10YTrend = 1
    else:
      US10YTrend = -1

    if spreadclose >= spreadavg:
      SpreadTrend = 1
    else:
      SpreadTrend = -1

    # Determine regime bucket (1 = very bearish, 8 = very bullish)
    if (SpreadTrend == 1 and USDTrend == 1 and US10YTrend == 1):
      Regime = 1

    if (SpreadTrend == 1 and USDTrend == 1 and US10YTrend == -1):
      Regime = 2

    if (SpreadTrend == 1 and USDTrend == -1 and US10YTrend == 1):
      Regime = 3

    if (SpreadTrend == 1 and USDTrend == -1 and US10YTrend == -1):
      Regime = 4

    if (SpreadTrend == -1 and USDTrend == 1 and US10YTrend == 1):
      Regime = 5

    if (SpreadTrend == -1 and USDTrend == 1 and US10YTrend == -1):
      Regime = 6

    if (SpreadTrend == -1 and USDTrend == -1 and US10YTrend == 1):
      Regime = 7

    if (SpreadTrend == -1 and USDTrend == -1 and US10YTrend == -1):
      Regime = 8

    print(f"\nSpread (close, avg, trend): {spreadclose} {spreadavg} {SpreadTrend}")
    print(f"USD (close, avg, trend): {dxyclose} {dxyavg} {USDTrend}")
    print(f"US10Y (close, avg, trend): {tnxclose} {tnxavg} {US10YTrend}")
    print(f"Regime: {Regime}")
    
    return Regime


##### GLOBAL DECLARATIONS #####
api_key = 'qJ9JoGI3SnyzfYK4UwHiXQjormGreJWC' # API key for FMP data service
#os.environ['TZ'] = 'America/New_York' # Set the new timezone
#time.tzset()


# Command line parameters

#print("Script name:", sys.argv[0])
#print("Arguments:", sys.argv[1:])

#Args:  1: AssetClass_Master 2: ETF_Master 3: Stock_Master 4: Portfolio_Master 5: Test_Master 6: Problem_Master

SymbList_arg = "2" # Default if no command line argument provided

query = '''SELECT * FROM AssetClass_Master'''

if len(sys.argv) > 1:
  SymbList_arg = sys.argv[1]

# Database declarations
database = "AutoTA.db"
connection = sql.connect(database)

if SymbList_arg == "1":
  query = '''SELECT * FROM Portfolio_Master'''

if SymbList_arg == "2":
  query = '''SELECT * FROM AssetClass_Master'''

if SymbList_arg == "3":
  query = '''SELECT * FROM ETF_Master'''

if SymbList_arg == "4":
  query = '''SELECT * FROM Stock_Master'''

if SymbList_arg == "5":
  query = '''SELECT * FROM Test_Master'''

if SymbList_arg == "6":
  query = '''SELECT * FROM Problem_Master'''

print("\nCommand line argument: ", SymbList_arg)

#utc_now = datetime.now(timezone.utc)
#est_now = utc_now.astimezone(ZoneInfo('America/New_York'))

#timezone = ZoneInfo('America/New_York')
end_dt = date.today()
start_dt_1y = end_dt - relativedelta(years=1) # 1 year of ETF data
start_dt_1m = end_dt - relativedelta(months=1) # 1 month of macro data
current_dt_5h_start = datetime.now() - relativedelta(hours=5) # changes to EST

#ETFCount = 10


# Scoring Parameters
ChangePeriod = 5 # Change Period
RSPeriod = 5 # RS Period
RSThreshold = 2 # RS Threshold
RSImpThreshold = 1.5 # RS Improvement Threshold
RelVolThreshold = 1.25 # Relative Volume Threshold
RelVolPeriod = 21 # Relative Volume SMA Period
OBThreshold = 75 # Overbought Threshold
OSThreshold = 25 # Oversold Threshold
RSIPeriod = 14 # RSI Period
RSISmoothD = 8 # RSI Smooth D Period
StochPeriod = 14 # Stoch Period
smoothK = 3 # Stoch Smooth K Period
StochSmoothD = 3 # Stoch Smooth D Period
ADXPeriod = 14 # ADX Period
ADXThreshold = 20 # ADX Trend Threshold
BBPeriod = 50 # BB SMA ZScore Period
ZScoreThreshold = 1.5 # ZScore Threshold
LinRegPeriod1 = "1M" # Linear Regression LTF
LinRegPeriod2 = "3M" # Linear Regression HTF
ATRThreshold = 6 # ATR Threshold
ATRPeriod = 50 # ATR SMA Period
PivotPeriod = 63 # Pivot Period
FibThreshold = 62 # Fibonacci Threshold
PeakThreshold = 5 # Approaching New High Threshold
VWAPBuffer = 0.1 # VWAP Buffer ZScore
RelVolatilityThreshold = 1 # Relative Volatility Threshold
ScoreThreshold = 8 # Scoring Threshold

# Scanner values
TickerID = ""
TickerDesc = ""
TickerClass = ""
tf = ""
Open = 0.0
High = 0.0
Low = 0.0
Close = 0.0
Volume = 0.0
Change1D = 0.0
Change5D = 0.0
Slope3M = 0.0
Slope1M = 0.0
Reg3MZScore = 0.0
Reg1MZScore = 0.0
MACD = 0.0
MACDHist = 0.0
MACDSig = 0.0
MACDAdv = False
RS5 = 0.0
RS10 = 0.0
RelVol = 0.0
RSI = 0.0
RSILowest = 0.0
RSI = 0.0
RSI_D = 0.0
RSIAdv = False
StochK = 0.0
StochLowest = 0.0
StochHighest = 0.0
StochD = 0.0
StochAdv = False
BBZScore = 0.0
BBUpper = 0.0
BBLower = 0.0
RetracementPerc63D = 0.0
PivotHigh63D = 0.0
PivotLow63D = 0.0
RetracementPerc21D = 0.0
SDPivot3MDt = 0.0
VWAP3MPx = 0.0
VWAP1MPx = 0.0
ADX = 0.0
ATR = 0.0
ATRx = 0.0
EMA5D = 0.0
EMA10D = 0.0
EMA20D = 0.0
SMA50D = 0.0
SMA150D = 0.0
SMA200D = 0.0
RVol1M3M = 0.0
VolSqueeze = False
PxLowestLow = 0.0
BullEngulfing = False
Hammer = False
ThreeSoldiers = False
PxHighestHigh = 0.0
BearEngulfing = False
HangingMan = False
ThreeCrows = False


# Playbook core portfolio ETFs
Reg1ETFs = "XLE,XLV,BTAL,RWM,HDGE,HYKE,PFIX,TBF,SJB,UUP,DGZ,BITI,DBA,KRBN,VXX"
Reg2ETFs = "XLP,XLU,XLV,BTAL,SPHD,RWM,HDGE,SHY,TLT,AGG,EDV,SJB,UUP,DGZ,BITI,BNDX,USRT"
Reg3ETFs = "SPY,XLE,XLV,BTAL,HYKE,PFIX,TBF,SJB,UDN,FXF,GLD,FBTC,IBIT,SPDW,EEM,DBA,USO,INFL"
Reg4ETFs = "SPY,XLP,XLU,XLV,SPHD,SHY,TLT,AGG,EDV,SJB,UDN,FXF,GLD,FBTC,IBIT,SPDW,EEM,BNDX,USRT"
Reg5ETFs = "SPY,IAK,XLB,XLE,XLF,XLI,XLK,IVE,HYKE,PFIX,TBF,HYG,UUP,DGZ,BITI,DBB,GSG,URA,USO,INFL"
Reg6ETFs = "SPY,XLI,XLK,IVE,SPHD,SHY,TLT,AGG,EDV,HYG,UUP,DGZ,BITI,BNDX,URA,WOOD,USRT"
Reg7ETFs = "SPY,XLB,XLC,XLE,XLF,XLI,XLK,XLY,IVE,IWF,MGK,SMG,QUAL,MTUM,SPMO,SPHB,HYKE,PFIX,TBF,HYG,UDN,FXE,GLD,FBTC,IBIT,SPDW,EEM,DBB,GSG,URA,USO,INFL"
Reg8ETFs = "SPY,XLB,XLC,XLF,XLI,XLK,XLY,IWM,IWR,IWR,IYT,MGK,SMH,MTUM,SPMO,SPHB,SHY,TLT,AGG,EDV,HYG,UDN,FXE,GLD,FBTC,IBIT,SPDW,EEM,BNDX,DBB,GSG,URA,WOOD,USRT"


##### START PROCESSING #####
#print(f"\n\nStart! {datetime.today():%B %d, %Y %H:%M:%S}\n")
print(f"\n\nStart! {current_dt_5h_start:%B %d, %Y %H:%M:%S}\n")


##### CHECK IF IN SESSION #####
if is_in_time_range(time(14, 30, 0, 0), time(21, 00, 00, 0)): # UTC time
  VolumePeriod = 1 # Use yesterday's close if in session for relative volume
  print("Stock exchange in-session.\n")
else:
  VolumePeriod = 0 # Use today's close if session closed for relative volume
  print("Stock exchange closed.\n")


##### LOAD MASTER ETF LIST #####
#ETFList_df = pd.read_csv('/content/drive/MyDrive/AutoTA/Master - ETF.csv') # , index_col=0)
ETFList_df = pd.read_sql_query(query, connection)


##### MACRO REGIME #####
print(f"Determining Macro Regime:\n")

# US Dollary (DXY Index)
print(f" DXY: https://financialmodelingprep.com/api/v3/historical-chart/1day/DX-Y.NYB?from={start_dt_1m:%Y-%m-%d}")
DXYHistory_ar = get_jsonparsed_data(f"https://financialmodelingprep.com/api/v3/historical-chart/1day/DX-Y.NYB?from={start_dt_1m:%Y-%m-%d}&apikey={api_key}")
DXYHistory_df = pd.DataFrame(DXYHistory_ar)
DXYHistory_df['date'] = pd.to_datetime(DXYHistory_df['date'])
DXYHistory_df = DXYHistory_df.sort_values(by = ['date'], ascending = True)
DXYHistory_df = DXYHistory_df.set_index('date').reset_index()
DXYHistory_df['DXYEMA5D'] = round(ta.ema(DXYHistory_df['close'], length = 5, fillna = 0), 3)

# 10 Year Treasury Interest (^TNX Index) Rates
print(f" TNX: https://financialmodelingprep.com/api/v3/historical-chart/1day/%5ETNX?from={start_dt_1m:%Y-%m-%d}")
IntRatesHistory_ar = get_jsonparsed_data(url=f"https://financialmodelingprep.com/api/v3/historical-chart/1day/%5ETNX?from={start_dt_1m:%Y-%m-%d}&apikey={api_key}")
#IntRatesHistory_ar = get_jsonparsed_data(url=f"https://financialmodelingprep.com/api/v4/treasury?from={start_dt_1m:%Y-%m-%d}&apikey={api_key}")
IntRatesHistory_df = pd.DataFrame(IntRatesHistory_ar)
IntRatesHistory_df['date'] = pd.to_datetime(IntRatesHistory_df['date'])
IntRatesHistory_df = IntRatesHistory_df.sort_values(by = ['date'], ascending = True)
IntRatesHistory_df = IntRatesHistory_df.set_index('date').reset_index()
IntRatesHistory_df['10YEMA5D'] = round(ta.ema(IntRatesHistory_df['close'], length = 5, fillna = 0), 3)

# High Yield Corporate Credit Spread (IEI/HYG ETFs)
print(f" IEI: https://financialmodelingprep.com/api/v3/historical-chart/1day/IEI?from={start_dt_1m:%Y-%m-%d}")
IEIHistory_ar = get_jsonparsed_data(f"https://financialmodelingprep.com/api/v3/historical-chart/1day/IEI?from={start_dt_1m:%Y-%m-%d}&apikey={api_key}")
IEIHistory_df = pd.DataFrame(IEIHistory_ar)
IEIHistory_df['date'] = pd.to_datetime(IEIHistory_df['date'])
IEIHistory_df = IEIHistory_df.sort_values(by = ['date'], ascending = True)
IEIHistory_df = IEIHistory_df.set_index('date').reset_index()

print(f" HYG: https://financialmodelingprep.com/api/v3/historical-chart/1day/HYG?from={start_dt_1m:%Y-%m-%d}")
HYGHistory_ar = get_jsonparsed_data(f"https://financialmodelingprep.com/api/v3/historical-chart/1day/HYG?from={start_dt_1m:%Y-%m-%d}&apikey={api_key}")
HYGHistory_df = pd.DataFrame(HYGHistory_ar)
HYGHistory_df['date'] = pd.to_datetime(HYGHistory_df['date'])
HYGHistory_df = HYGHistory_df.sort_values(by = ['date'], ascending = True)
HYGHistory_df = HYGHistory_df.set_index('date').reset_index()

SpreadHistory_df = pd.merge(IEIHistory_df, HYGHistory_df, how = 'outer', on = 'date')
SpreadHistory_df = SpreadHistory_df.sort_values(by = ['date'], ascending = True)
SpreadHistory_df = SpreadHistory_df.set_index('date').reset_index()
SpreadHistory_df['spread'] = round((SpreadHistory_df['close_x'] / SpreadHistory_df['close_y']), 3)  # x = IEI  y = HYG
SpreadHistory_df['spreadEMA5D'] = round(ta.ema(SpreadHistory_df['spread'], length = 5, fillna = 0), 3)

# Determine Macro Regime
MacroRegime = 0
MacroRegimeStat = ""
MacroRegime = macro_regime(DXYHistory_df, IntRatesHistory_df, SpreadHistory_df)  # Returns current macro regime

if MacroRegime >= 1 and MacroRegime <= 3:
    MacroRegimeStat = "Bearish"

if MacroRegime >= 4 and MacroRegime <= 5:
    MacroRegimeStat = "Neutral"

if MacroRegime >= 6 and MacroRegime <= 8:
    MacroRegimeStat = "Bullish"

print(f"\nMacro Regime: {MacroRegime} - {MacroRegimeStat}\n")

# Set ETF list based on macro regime
match MacroRegime:
    case 1:
      RegETFs = Reg1ETFs
    case 2:
      RegETFs = Reg2ETFs
    case 3:
      RegETFs = Reg3ETFs
    case 4:
      RegETFs = Reg4ETFs
    case 5:
      RegETFs = Reg5ETFs
    case 6:
      RegETFs = Reg6ETFs
    case 7:
      RegETFs = Reg7ETFs
    case 8:
      RegETFs = Reg8ETFs


##### BUILD SCREENER #####
ETFScreener_df = ETFScreenerTemp_df = pd.DataFrame(columns=['Ticker', 'Description', 'Class', 'TF', 'open', 'high', 'low', 'close'])

# GET SPY PRICES (FOR RS BASE)
print(f"Retrieving historical SPY data\n")
SPYPriceHistory_ar = get_jsonparsed_data(f"https://financialmodelingprep.com/api/v3/historical-chart/1day/SPY?from={start_dt_1y:%Y-%m-%d}&apikey={api_key}")
SPYPriceHistory_df = pd.DataFrame(SPYPriceHistory_ar)
SPYPriceHistory_df['date'] = pd.to_datetime(SPYPriceHistory_df['date'])
SPYPriceHistory_df = SPYPriceHistory_df.sort_values(by = ['date'], ascending = True)
SPYPriceHistory_df = SPYPriceHistory_df.set_index('date').reset_index()


# LOOP THROUGH TICKERS
TFList = ["1day","1week"]
print(f"Retrieving & processing historical ETF data:\n")
for i in ETFList_df.index:   #for i in ETFList_df.index:       for i in range(0, 4):

  # LOOP THROUGH TIMEFRAMES
  for tf in TFList:

    TickerID = str.strip(str(ETFList_df['Ticker'][i]))
    TickerDesc = str(ETFList_df['Description'][i])
    TickerClass = str(ETFList_df['Class'][i])
    ##print(Ticker,TickerDesc,TickerClass)

    try:
      # GET HISTORICAL PRICE DATA
      print(f"Retrieving: {TickerID} {tf}: https://financialmodelingprep.com/api/v3/historical-chart/{tf}/{TickerID}?from={start_dt_1y:%Y-%m-%d}")
      ETFPriceHistory_ar = get_jsonparsed_data(url=f"https://financialmodelingprep.com/api/v3/historical-chart/{tf}/{TickerID}?from={start_dt_1y:%Y-%m-%d}&apikey={api_key}") # &to={end_dt:%Y-%m-%d}
      ETFPriceHistory_df = pd.DataFrame(ETFPriceHistory_ar)
      ETFPriceHistory_df = ETFPriceHistory_df.sort_values(by=['date'], ascending = [True])
      ETFPriceHistory_df['date'] = pd.to_datetime(ETFPriceHistory_df['date'])
      ETFPriceHistory_df = ETFPriceHistory_df.sort_values(by = ['date'], ascending = True)
      ETFPriceHistory_df = ETFPriceHistory_df.set_index('date').reset_index()

      # INDICATOR CALCULATIONS

      # Price & Volume
      if ETFPriceHistory_df['open'].iloc[-1] is not None:
        Open = round(ETFPriceHistory_df['open'].iloc[-1], 2)
      else:
        Open = 0

      if ETFPriceHistory_df['high'].iloc[-1] is not None:
        High = round(ETFPriceHistory_df['high'].iloc[-1], 2)
      else:
        High = 0

      if ETFPriceHistory_df['low'].iloc[-1] is not None:
        Low = round(ETFPriceHistory_df['low'].iloc[-1], 2)
      else:
        Low = 0

      if ETFPriceHistory_df['close'].iloc[-1] is not None:
        Close = round(ETFPriceHistory_df['close'].iloc[-1], 2)
      else:
        Close = 0

      if ETFPriceHistory_df['volume'].iloc[-1] is not None:
        Volume = round(ETFPriceHistory_df['volume'].iloc[-1], 2)
      else:
        Volume = 0

        
      ETFPriceHistory_df['hlc3'] = round(ta.hlc3(ETFPriceHistory_df['high'], ETFPriceHistory_df['low'], ETFPriceHistory_df['close'], fillna = 0), 2)

      # ROC
      if tf == "1day":
        ETFPriceHistory_df['Change1D'] = ta.roc(ETFPriceHistory_df['close'], length = 1, fillna = 0)
        Change1D = round(ETFPriceHistory_df['Change1D'].iloc[-1], 2)
        ETFPriceHistory_df['Change5D'] = ta.roc(ETFPriceHistory_df['close'], length = 5, fillna = 0)
        Change5D = round(ETFPriceHistory_df['Change5D'].iloc[-1], 2)
      else:
        Change1D = 0
        Change5D = 0

      # SMA/EMAs
      if tf == "1day":
        ETFPriceHistory_df['EMA5D'] = ta.ema(ETFPriceHistory_df['close'], length = 5, fillna = 0)
        EMA5D = round(ETFPriceHistory_df['EMA5D'].iloc[-1], 2)

        ETFPriceHistory_df['EMA10D'] = ta.ema(ETFPriceHistory_df['close'], length = 10, fillna = 0)
        EMA10D = round(ETFPriceHistory_df['EMA10D'].iloc[-1], 2)

        ETFPriceHistory_df['EMA20D'] = ta.ema(ETFPriceHistory_df['close'], length = 20, fillna = 0)
        EMA20D = round(ETFPriceHistory_df['EMA20D'].iloc[-1], 2)

        ETFPriceHistory_df['SMA50D'] = ta.sma(ETFPriceHistory_df['close'], length = 50, fillna = 0)
        SMA50D = round(ETFPriceHistory_df['SMA50D'].iloc[-1], 2)

        ETFPriceHistory_df['SMA150D'] = ta.sma(ETFPriceHistory_df['close'], length = 150, fillna = 0) #  if tf == "1day" else 30
        SMA150D = round(ETFPriceHistory_df['SMA150D'].iloc[-1], 2)

        ETFPriceHistory_df['SMA200D'] = ta.sma(ETFPriceHistory_df['close'], length = 200, fillna = 0)
        SMA200D = round(ETFPriceHistory_df['SMA200D'].iloc[-1], 2)

      # RS
      if tf == "1day":
        ETFPriceHistory_df['RS5'] = ((ETFPriceHistory_df['close'] / ETFPriceHistory_df['close'].iloc[-5]) / (SPYPriceHistory_df['close'] / SPYPriceHistory_df['close'].iloc[-5]) - 1) * 100
        RS5 = round(ETFPriceHistory_df['RS5'].iloc[-1], 2)

        ETFPriceHistory_df['RS10'] = ((ETFPriceHistory_df['close'].iloc[-5] / ETFPriceHistory_df['close'].iloc[-10]) / (SPYPriceHistory_df['close'].iloc[-5] / SPYPriceHistory_df['close'].iloc[-10]) - 1) * 100
        RS10 = round(ETFPriceHistory_df['RS10'].iloc[-1], 2)
      else:
        RS5 = 0
        RS10 = 0

      # RSI
      ETFPriceHistory_df['RSI'] = ta.rsi(ETFPriceHistory_df['close'], length = 14, fillna = 0)
      RSI = ETFPriceHistory_df['RSI'].iloc[-1]
      ETFPriceHistory_df['RSILowest'] = ETFPriceHistory_df['RSI'].rolling(window = 3).min()
      ETFPriceHistory_df['RSIHighest'] = ETFPriceHistory_df['RSI'].rolling(window = 3).max()
      RSILowest = round(ETFPriceHistory_df['RSILowest'].iloc[-1], 1)
      RSIHighest = round(ETFPriceHistory_df['RSIHighest'].iloc[-1], 1)

      ETFPriceHistory_df['RSI_D'] = ta.sma(ETFPriceHistory_df['RSI'], length = 8, fillna = 0)
      RSIAdv = False
      if ETFPriceHistory_df['RSI'].iloc[-1] >= ETFPriceHistory_df['RSI_D'].iloc[-1]:
        RSIAdv = True
      RSI = round(ETFPriceHistory_df['RSI'].iloc[-1], 1)
      RSI_D = round(ETFPriceHistory_df['RSI_D'].iloc[-1], 1)

      # Stochastic
      Stoch_df = ta.stoch(ETFPriceHistory_df['high'], ETFPriceHistory_df['low'], ETFPriceHistory_df['close'], 14, 3, 3) # Returns a tuple so has a separate df
      Stoch_df['StochLowest'] = Stoch_df['STOCHk_14_3_3'].rolling(window = 3).min()
      Stoch_df['StochHighest'] = Stoch_df['STOCHk_14_3_3'].rolling(window = 3).max()
      StochLowest = round(Stoch_df['StochLowest'].iloc[-1], 1)
      StochHighest = round(Stoch_df['StochHighest'].iloc[-1], 1)

      StochAdv = False
      if Stoch_df['STOCHk_14_3_3'].iloc[-1] >= Stoch_df['STOCHd_14_3_3'].iloc[-1]:
        StochAdv = True
      StochK = round(Stoch_df['STOCHk_14_3_3'].iloc[-1], 1)
      StochD = round(Stoch_df['STOCHd_14_3_3'].iloc[-1], 1)

      # ADX
      ADX_df = ta.adx(ETFPriceHistory_df['high'], ETFPriceHistory_df['low'], ETFPriceHistory_df['close'], length = 14, fillna = 0)
      ADX = round(ADX_df['ADX_14'].iloc[-1], 0)

      # Bollinger Bands
      if tf == "1day":  # ta.bbands generates error if length adjusted for weekly timeframe
        BB_df = ta.bbands(ETFPriceHistory_df['close'], length = 50, ddof = 0, mamode = None, talib = None, offset = None) # Returns a tuple so has a separate df
        BBUpper = round(BB_df['BBU_50_2.0'].iloc[-1], 2)
        BBMiddle = round(BB_df['BBM_50_2.0'].iloc[-1], 2)
        BBLower = round(BB_df['BBL_50_2.0'].iloc[-1], 2)
        BBPerc = round(BB_df['BBP_50_2.0'].iloc[-1], 2) # Percentage
        ETFPriceHistory_df['BBZScore'] = round(ta.zscore(ETFPriceHistory_df['close'], length = 50, std = 1, ddof = 0, mamode = None, talib = None, offset = None).iloc[-1], 2) # ZScore std dev = 1 with 50D SMA
        BBZScore = round(ETFPriceHistory_df['BBZScore'].iloc[-1], 1)
      else:
        BBUpper = 0
        BBMiddle = 0
        BBLower = 0
        BBPerc = 0
        BBZScore = 0

      # 3M & 3W Demand and Supply Zones
      if tf == "1day":
        ETFPriceHistory_df['PivotHigh21D'] = ETFPriceHistory_df['high'].rolling(window = 21).max()
        PivotHigh21D = ETFPriceHistory_df['PivotHigh21D'].iloc[-2]
        ETFPriceHistory_df['PivotLow21D'] = ETFPriceHistory_df['low'].rolling(window = 21).min()
        PivotLow21D = ETFPriceHistory_df['PivotLow21D'].iloc[-2]
        Range21D = PivotHigh21D - PivotLow21D
        RetracementPerc21D = round((((ETFPriceHistory_df['close'].iloc[-1] - PivotHigh21D) / Range21D) * 100), 0)

        ETFPriceHistory_df['PivotHigh63D'] = ETFPriceHistory_df['high'].rolling(window = 63).max()
        PivotHigh63D = ETFPriceHistory_df['PivotHigh63D'].iloc[-2]
        ETFPriceHistory_df['PivotLow63D'] = ETFPriceHistory_df['low'].rolling(window = 63).min()
        PivotLow63D = ETFPriceHistory_df['PivotLow63D'].iloc[-2]
        Range63D = PivotHigh63D - PivotLow63D
        RetracementPerc63D = round((((ETFPriceHistory_df['close'].iloc[-1] - PivotHigh63D) / Range63D) * 100), 0)
      else:
        PivotHigh21D = 0
        PivotLow21D = 0
        Range21D = 0
        RetracementPerc21D = 0
        PivotHigh63D = 0
        PivotLow63D = 0
        Range63D = 0
        RetracementPerc63D = 0

      # ATR & ATRx
      ETFPriceHistory_df['ATR'] = ta.atr(ETFPriceHistory_df['high'], ETFPriceHistory_df['low'], ETFPriceHistory_df['close'], length = 50 if tf == "1day" else 10, fillna = 0)
      ATR = round(ETFPriceHistory_df['ATR'].iloc[-1], 2)
      ETFPriceHistory_df['ATRx'] = (ETFPriceHistory_df['close'] - ta.sma(ETFPriceHistory_df['close'], length = 50 if tf == "1day" else 10)) / ETFPriceHistory_df['ATR']
      ATRx = round(ETFPriceHistory_df['ATRx'].iloc[-1], 1)

      # Relative Volume
      if not np.isnan(ETFPriceHistory_df['volume'].iloc[-1]) or ETFPriceHistory_df['volume'].iloc[-1] != 0:
        ETFPriceHistory_df['MaxVlm_Prev3'] = ETFPriceHistory_df['volume'].rolling(window = 2, min_periods = 1).max() # Get highest volume from previous two days
        ETFPriceHistory_df['RelVol'] = round(ETFPriceHistory_df['MaxVlm_Prev3'][VolumePeriod] / ta.sma(ETFPriceHistory_df['volume'], 21), 1)
      else:
        ETFPriceHistory_df['RelVol'] = 0

      RelVol = ETFPriceHistory_df['RelVol'].iloc[-1]

      # 1M/3M Realized Volatility
      if tf == "1day":
        ETFPriceHistory_df['Var'] = ETFPriceHistory_df['close'].pct_change()
        ETFPriceHistory_df['VarLog'] = np.log(1 + ETFPriceHistory_df['Var'])
        ETFPriceHistory_df['RVol1M'] = ETFPriceHistory_df['VarLog'].rolling(window=21).std() * np.sqrt(252)
        ETFPriceHistory_df['RVol3M'] = ETFPriceHistory_df['VarLog'].rolling(window=63).std() * np.sqrt(252)
        ETFPriceHistory_df['RVol1M3M'] = round(ETFPriceHistory_df['RVol1M'] / ETFPriceHistory_df['RVol3M'], 2)
        RVol1M3M = ETFPriceHistory_df['RVol1M3M'].iloc[-1]

      else:
        RVol1M3M = 0

      # MACD
      macd_df = ta.macd(ETFPriceHistory_df['close'], slow = 12, fast = 26, fillna = 0)
      macd_df.fillna(0, inplace = True)
      macd_df.columns = ['macd', 'histogram', 'signal']
      ETFPriceHistory_df['MACD'] = macd_df['macd']
      ETFPriceHistory_df['MACD_Hist'] = macd_df['histogram']
      ETFPriceHistory_df['MACD_Signal'] = macd_df['signal']
      ETFPriceHistory_df['MACDAdv'] = ta.increasing(ETFPriceHistory_df['MACD_Hist'], 3)
      MACD = round(ETFPriceHistory_df['MACD'].iloc[-1], 2)
      MACDHist = round(ETFPriceHistory_df['MACD_Hist'].iloc[-1], 2)
      MACDSig = round(ETFPriceHistory_df['MACD_Signal'].iloc[-1], 2)
      if ETFPriceHistory_df['MACDAdv'].iloc[-1] == 1:
        MACDAdv = True
      else:
        MACDAdv = False

      # Trend Channel
      if tf == "1day":
        # 3M Regression Line
        rw = len(ETFPriceHistory_df) # number of rows in dataframe
        PriceHistory3Mx_df = pd.DataFrame(range(0,62), columns=['sequential'])  # 63 # Create X values.  Can't pass dates to polyfit so have to use integers instead
        PriceHistory3My_df = ETFPriceHistory_df['close'].truncate(before = rw - 62)  # Truncate dataframe to match lookback period
        PriceHistory3My_df = PriceHistory3My_df.to_frame() # Series type created for some reason so converted to dataframe
        PriceHistory3Mx = np.array(PriceHistory3Mx_df['sequential']) # convert dataframe to a 1 dimensional array
        PriceHistory3My = np.array(PriceHistory3My_df['close']) # convert dataframe to 1 dimensional array
        Slope3M, Intercept3M = np.polyfit(PriceHistory3Mx, PriceHistory3My, 1)

        std_dev3M = std_dev_from_residuals(PriceHistory3Mx, PriceHistory3My, Slope3M, Intercept3M)
        y = Slope3M * PriceHistory3Mx_df['sequential'].iloc[-1] + Intercept3M #y = mx+b [62]
        Reg3MZScore = (ETFPriceHistory_df['close'].iloc[-1] - y) / std_dev3M
        Reg3MZScore = round(Reg3MZScore, 1)

        # 1M Regression Line
        PriceHistory1Mx_df = pd.DataFrame(range(0,20), columns=['sequential'])  # 21 # Create X values.  Can't pass dates to polyfit so have to use integers instead
        PriceHistory1My_df = ETFPriceHistory_df['close'].truncate(before = rw - 20)  # Truncate dataframe to match lookback period.   df = df.iloc[232:]
        PriceHistory1My_df = PriceHistory1My_df.to_frame() # Series type created for some reason so converted to dataframe
        PriceHistory1Mx = np.array(PriceHistory1Mx_df['sequential']) # convert dataframe to a 1 dimensional array
        PriceHistory1My = np.array(PriceHistory1My_df['close']) # convert dataframe to 1 dimensional array
        Slope1M, Intercept1M = np.polyfit(PriceHistory1Mx, PriceHistory1My, 1)

        std_dev1M = std_dev_from_residuals(PriceHistory1Mx, PriceHistory1My, Slope1M, Intercept1M)
        y = Slope1M * PriceHistory1Mx_df['sequential'].iloc[-1] + Intercept1M #y = mx+b # [20]
        Reg1MZScore = (ETFPriceHistory_df['close'].iloc[-1] - y) / std_dev1M
        Reg1MZScore = round(Reg1MZScore, 1)

      #else:
      #  Slope3M = 0
      #  Slope1M = 0
      #  Reg3MZScore = 0
      #  Reg1MZScore = 0

      # VWAP
      SDPivot = 0
      if tf == "1day":
        ETFVWAPHist_df = ETFPriceHistory_df.truncate(before = 190)
        ETFVWAPHist_df = ETFVWAPHist_df.sort_values(by = 'date', ascending = True)
        if Slope3M >= 0:
          SDPivot3MID = ETFVWAPHist_df['hlc3'].idxmin()
          SDPivot3MDt = ETFVWAPHist_df['date'][SDPivot3MID]
          SDPivot3MPx = ETFVWAPHist_df['close'][SDPivot3MID]
        else:
          SDPivot3MID = ETFVWAPHist_df['hlc3'].idxmax()
          SDPivot3MDt = ETFVWAPHist_df.at[SDPivot3MID, 'date']
          SDPivot3MPx = ETFVWAPHist_df.at[SDPivot3MID, 'close']
        VWAP3M = anchored_vwap(ETFVWAPHist_df, SDPivot3MDt)
        VWAP3MPx = round(VWAP3M.iloc[-1], 2)

        ETFVWAPHist_df = ETFPriceHistory_df.truncate(before = 232)
        ETFVWAPHist_df = ETFVWAPHist_df.sort_values(by = 'date', ascending = True)
        if Slope1M >= 0:
          SDPivot1MID = ETFVWAPHist_df['hlc3'].idxmin()
          SDPivot1MDt = ETFVWAPHist_df['date'][SDPivot1MID]
          SDPivot1MPx = ETFVWAPHist_df['close'][SDPivot1MID]
        else:
          SDPivot1MID = ETFVWAPHist_df['hlc3'].idxmax()
          SDPivot1MDt = ETFVWAPHist_df.at[SDPivot1MID, 'date']
          SDPivot1MPx = ETFVWAPHist_df.at[SDPivot1MID, 'close']
        VWAP1M = anchored_vwap(ETFVWAPHist_df, SDPivot1MDt)
        VWAP1MPx = round(VWAP1M.iloc[-1], 2)
      else:
        VWAP3MPx = 0
        VWAP1MPx = 0
        SDPivot3MPx = 0
        SDPivot1MPx = 0

      # Bottomed Out
      ETFPriceHistory_df['PxLowestLow'] = round(ETFPriceHistory_df['low'].rolling(window = 4).min(), 2)
      PxLowestLow = ETFPriceHistory_df['PxLowestLow'].iloc[-1]

      # Bullish Engulfing Candle
      if ETFPriceHistory_df['close'].iloc[-1] > ETFPriceHistory_df['high'].iloc[-2]:
        BullEngulfing = True
      else:
        BullEngulfing = False

      # Hammer
      if (ETFPriceHistory_df['close'].iloc[-1] > ETFPriceHistory_df['open'].iloc[-1]) and ((ETFPriceHistory_df['close'].iloc[-1] - ETFPriceHistory_df['open'].iloc[-1]) < (ETFPriceHistory_df['open'].iloc[-1] - ETFPriceHistory_df['low'].iloc[-1])):
        Hammer = True
      else:
        Hammer = False

      # Three White Soldiers
      if (ETFPriceHistory_df['close'].iloc[-1] > ETFPriceHistory_df['close'].iloc[-2]) and (ETFPriceHistory_df['close'].iloc[-2] > ETFPriceHistory_df['close'].iloc[-3]) and (ETFPriceHistory_df['close'].iloc[-3] > ETFPriceHistory_df['close'].iloc[-4] and (ETFPriceHistory_df['close'].iloc[-1] > ETFPriceHistory_df['open'].iloc[-1]) and (ETFPriceHistory_df['close'].iloc[-2] > ETFPriceHistory_df['open'].iloc[-2]) and (ETFPriceHistory_df['close'].iloc[-3] > ETFPriceHistory_df['open'].iloc[-3])):
        ThreeSoldiers = True
      else:
        ThreeSoldiers = False

      # Topped Out
      ETFPriceHistory_df['PxHighestHigh'] = round(ETFPriceHistory_df['high'].rolling(window = 4).max(), 2)
      PxHighestHigh = ETFPriceHistory_df['PxHighestHigh'].iloc[-1]

      # Bearish Engulfing Candle
      if ETFPriceHistory_df['close'].iloc[-1] < ETFPriceHistory_df['low'].iloc[-2]:
        BearEngulfing = True
      else:
        BearEngulfing = False

      # Hanging Man (Bearish Hammer)
      if (ETFPriceHistory_df['close'].iloc[-1] < ETFPriceHistory_df['open'].iloc[-1]) and ((ETFPriceHistory_df['close'].iloc[-1] - ETFPriceHistory_df['open'].iloc[-1]) > (ETFPriceHistory_df['open'].iloc[-1] - ETFPriceHistory_df['low'].iloc[-1])):
        HangingMan = True
      else:
        HangingMan = False

      # Three Black Crows
      if (ETFPriceHistory_df['close'].iloc[-1] < ETFPriceHistory_df['close'].iloc[-2]) and (ETFPriceHistory_df['close'].iloc[-2] < ETFPriceHistory_df['close'].iloc[-3]) and (ETFPriceHistory_df['close'].iloc[-3] < ETFPriceHistory_df['close'].iloc[-4] and (ETFPriceHistory_df['close'].iloc[-1] < ETFPriceHistory_df['open'].iloc[-1]) and (ETFPriceHistory_df['close'].iloc[-2] < ETFPriceHistory_df['open'].iloc[-2]) and (ETFPriceHistory_df['close'].iloc[-3] < ETFPriceHistory_df['open'].iloc[-3])):
        ThreeCrows = True
      else:
        ThreeCrows = False

      # Candlestick Patterns
      #if tf == "1day":
      #  df1 = ETFPriceHistory_df.ta.cdl_pattern(name=["engulfing", "hammer", "3whitesoldiers"], )
      #  if ETFPriceHistory_df['CDLENGULFING'][0] == 1:
      #    Engulfing = True
      #  else:
      #    Engulfing = False
      #  if ETFPriceHistory_df['CDLHAMMER'][0] == 1:
      #    Hammer = True
      #  else:
      #    Hammer = False
      #  if ETFPriceHistory_df['CDL3WHITESOLDIERS'][0] == 1:
      #    Soldiers = True
      #  else:
      #    Soldiers = False

      # Volatility Squeeze  -- CONSIDER LAZYBEAR IMPLEMENTATION -- SOMETHING WRONG WITH THIS CODE. GENERATES ERROR IF PLACED HIGHER IN CODE
      VolSqueeze = False
      if tf == "1day":
        Squeeze_df = ETFPriceHistory_df.ta.squeeze(high = ETFPriceHistory_df['high'], low = ETFPriceHistory_df['low'], close = ETFPriceHistory_df['close'], bb_length = 20, bb_std = 2, kc_length = 20, kc_scalar = 1.5, mom_length = 12, mom_smooth = 12, tr = True, lazybear = False)
        if Squeeze_df['SQZ_ON'][0] == 1:
          VolSqueeze = True
        else:
          VolSqueeze = False
      else:
        VolSqueeze = False


      # APPEND TO SCREENER DATAFRAME
      ETFScreenerRow_df = pd.DataFrame({'Ticker': [TickerID], 'Description': [TickerDesc], 'Class': [TickerClass], 'TF': [tf], 'Open': [Open], 'High': [High], 'Low': [Low], 'Close': [Close], 'Volume': [Volume],
                                      'Change1D': [Change1D], 'Change5D': [Change5D], 'Slope3M': [Slope3M], 'Slope1M': [Slope1M], 'Reg3MZScore': [Reg3MZScore], 'Reg1MZScore': [Reg1MZScore], 'MACD': [MACD], 'MACDHist': [MACDHist], 'MACDSig': [MACDSig], 'MACDAdv': [MACDAdv], 'RS5': [RS5], 'RS10': [RS10], 'RelVol': [RelVol],
                                      'RSI': [RSI], 'RSILowest': [RSILowest], 'RSILowest': [RSI], 'RSID': [RSI_D], 'RSIAdv':[RSIAdv], 'Stoch': [StochK], 'StochLowest': [StochLowest], 'StochHighest': [StochHighest], 'StochD': [StochD], 'StochAdv': [StochAdv], 'BBZScore': [BBZScore], 'BBUpper': [BBUpper], 'BBLower': [BBLower],
                                      'PivotHigh63D': [PivotHigh63D], 'PivotLow63D': [PivotLow63D], 'RetPerc63D': [RetracementPerc63D], 'PivotHigh21D': [PivotHigh21D], 'PivotLow21D': [PivotLow21D], 'RetPerc21D': [RetracementPerc21D], 'SDPivot3MDt': [SDPivot3MDt], 'VWAP3MPx': [VWAP3MPx], 'VWAP1MPx': [VWAP1MPx], 'ADX': [ADX], 'ATR': [ATR], 'ATRx': [ATRx],
                                      'EMA5D': [EMA5D], 'EMA10D': [EMA10D], 'EMA20D': [EMA20D], 'SMA50D': [SMA50D], 'SMA150D': [SMA150D], 'SMA200D': [SMA200D],
                                      'RVol1M3M': [RVol1M3M],'VolSqueeze': [VolSqueeze], 'PxLowestLow': [PxLowestLow], 'BullEngulfing': [BullEngulfing], 'Hammer': [Hammer], 'ThreeSoldiers': [ThreeSoldiers], 'PxHighestHigh': [PxHighestHigh], 'BearEngulfing': [BearEngulfing], 'HangingMan': [HangingMan], 'ThreeCrows': [ThreeCrows]}) #.set_index(['Ticker', 'TF'])

      if tf == "1day" and i == 0:  # Prevents concat with initial null dataframe which Python dislikes
        ETFScreener_df = ETFScreenerRow_df
      else:
        ETFScreener_df = pd.concat([ETFScreener_df, ETFScreenerRow_df], sort = False)  # a.extend(b)

    except Exception as e:
      print(f"Exception {TickerID} {tf}")
      logger.error('%s', repr(e))


##### SCORING #####
print(f"\nScoring ETFs:\n")
ETFScore_df = ETFScoreTemp_df = pd.DataFrame(columns=['Ticker', 'Description', 'Class', 'Trade', 'Score', 'Explanation'])
ETFScore_df = ETFScore_df.astype({'Ticker': str, 'Description': str, 'Class': str, 'Trade': str, 'Score': int})
ETFScoreTemp_df = ETFScoreTemp_df.astype({'Ticker': str, 'Description': str, 'Class': str, 'Trade': str, 'Score': int})


for i in ETFList_df.index:  # Loop through tickers. #for i in range(0, ETFCount):     for i in ETFList_df.index:

  TickerID = str.strip(str(ETFList_df['Ticker'][i]))
  TickerDesc = str(ETFList_df['Description'][i])
  TickerClass = str(ETFList_df['Class'][i])

  Score1 = 0
  Score2 = 0
  Score3 = 0
  Score4 = 0
  ScoreXp1 = ""
  ScoreXp2 = ""
  ScoreXp3 = ""
  ScoreXp4 = ""
  Score = 0
  ScoreXp = ""
  Trade = ""


  TFList = ["1day", "1week"]
  for tf in TFList: # Loop through timeframes

    try:
      #print(f"Scoring: {TickerID} {tf}")
      ETFScreenerEvalRow_df = ETFScreener_df[((ETFScreener_df['Ticker'] == TickerID) & (ETFScreener_df['TF'] == tf))] # Filter all ETFScreener_df to just current row being evaluated
      if ETFScreenerEvalRow_df.empty:
        print(f"Scoring error: {TickerID} {tf} - No data")
      if len(ETFScreenerEvalRow_df) > 1:
        print(f"Scoring error: {TickerID} {tf} - More than one data row")

      if tf == "1day":
        Change1D = ETFScreenerEvalRow_df['Change1D'][0]
        Reg3MZScore = ETFScreenerEvalRow_df['Reg3MZScore'][0]
        ATRx = ETFScreenerEvalRow_df['ATRx'][0]

      # (1) Bullish market structure. Mean reversion.
      if (ETFScreenerEvalRow_df['Slope3M'][0] >= 0) and (ETFScreenerEvalRow_df['Reg3MZScore'][0] < 0.2) and (ETFScreenerEvalRow_df['StochAdv'][0]):

        if TickerID in RegETFs and tf == "1day":  # Ticker in ETF playbook. Only score once (i.e., 1day TF)
          InRegime = True
          Score1 += 2
          ScoreXp1 += "2.0 In regime " + tf + "\n"

        if ETFScreenerEvalRow_df['MACDAdv'][0]: # MACD Rising.  i.e., fast EMA rising faster than slow EMA
          Score1 += 1
          ScoreXp1 += "1.0 MACD Rising " + tf + "\n"

        if ETFScreenerEvalRow_df['Close'][0] <= ETFScreenerEvalRow_df['EMA5D'][0] and tf == "1day": # Price below EMA5. Short term trend down.
          Score1 += 1
          ScoreXp1 += "1.0 Price below EMA5 " + tf + "\n"

        if ETFScreenerEvalRow_df['Close'][0] <= ETFScreenerEvalRow_df['EMA10D'][0] and tf == "1day": # Price below EMA10. Short term trend down.
          Score1 += 1
          ScoreXp1 += "1.0 Price below EMA10 " + tf + "\n"

        if ETFScreenerEvalRow_df['Close'][0] <= ETFScreenerEvalRow_df['EMA20D'][0] and tf == "1day": # Price below EMA20. Short term trend down.
          Score1 += 1
          ScoreXp1 += "1.0 Price below EMA20 " + tf + "\n"

        if ETFScreenerEvalRow_df['RS5'][0] - ETFScreenerEvalRow_df['RS10'][0] > 0 and tf == "1day": # Relative strength improving
          Score1 += 2
          ScoreXp1 += "2.0 Relative strength improving " + tf + "\n"

        if ETFScreenerEvalRow_df['RSILowest'][0] <= OSThreshold: # RSI oversold
          Score1 += 1
          ScoreXp1 += "1.0 RSI oversold " + tf + "\n"

        if ETFScreenerEvalRow_df['StochLowest'][0] <= OSThreshold: # Stochastic oversold
          Score1 += 1
          ScoreXp1 += "1.0 Stochastic oversold " + tf + "\n"

        if ETFScreenerEvalRow_df['StochAdv'][0]: # Stochastic improving
          Score1 += 2
          ScoreXp1 += "2.0 Stochastic improving " + tf + "\n"

        if ETFScreenerEvalRow_df['BBZScore'][0] <= -1: # Bollinger band oversold
          Score1 += 1
          ScoreXp1 += "1.0 BB oversold by 1 std " + tf + "\n"

        if ETFScreenerEvalRow_df['BBZScore'][0] <= (ZScoreThreshold * -1): # Bollinger band oversold
          Score1 += 1
          ScoreXp1 += "1.0 BB oversold by threshold " + tf + "\n"

        if ETFScreenerEvalRow_df['Reg1MZScore'][0] <= -1 and tf == "1day": # 1M trend channel oversold
          Score1 += 1
          ScoreXp1 += "1.0 1M channel oversold by 1 std " + tf + "\n"

        if ETFScreenerEvalRow_df['Reg3MZScore'][0] <= -1 and tf == "1day": # 3M trend channel oversold
          Score1 += 1
          ScoreXp1 += "1.0 3M channel oversold by 1 std " + tf + "\n"

        if ETFScreenerEvalRow_df['Reg1MZScore'][0] <= (ZScoreThreshold * -1) and tf == "1day": # 1M trend channel oversold
          Score1 += 1
          ScoreXp1 += "1.0 1M channel oversold by threshold " + tf + "\n"

        if ETFScreenerEvalRow_df['Reg3MZScore'][0] <= (ZScoreThreshold * -1) and tf == "1day": # 3M trend channel oversold
          Score1 += 1
          ScoreXp1 += "1.0 3M channel oversold by threshold " + tf + "\n"

        if ETFScreenerEvalRow_df['RetPerc63D'][0] <= -1 * FibThreshold and tf == "1day": # 3M Fibonacci retraced.
          Score1 += 2
          ScoreXp1 += "2.0 3M Fib retraced to threshold " + tf + "\n"

        if ETFScreenerEvalRow_df['RetPerc63D'][0] <= -78 and tf == "1day": # 3M Fibonacci retraced 78%
          Score1 += 1
          ScoreXp1 += "1.0 3M Fib retraced 78% " + tf + "\n"

        if ETFScreenerEvalRow_df['RetPerc21D'][0] <= -1 * FibThreshold and tf == "1day": # 1M Fibonacci retraced
          Score1 += 2
          ScoreXp1 += "2.0 1M Fib retraced to threshold " + tf + "\n"

        if ETFScreenerEvalRow_df['RetPerc21D'][0] <= -78 and tf == "1day": # 1M Fibonacci retraced 78%
          Score1 += 1
          ScoreXp1 += "1.0 1M Fib retraced 78% " + tf + "\n"

        if ETFScreenerEvalRow_df['Close'][0] <= ETFScreenerEvalRow_df['VWAP3MPx'][0] and tf == "1day": # 3M VWAP oversold
          Score1 += 1
          ScoreXp1 += "1.0 3M VWAP oversold " + tf + "\n"

        if ETFScreenerEvalRow_df['Close'][0] <= ETFScreenerEvalRow_df['VWAP1MPx'][0] and tf == "1day": # 1M VWAP oversold
          Score1 += 1
          ScoreXp1 += "1.0 1M VWAP oversold " + tf + "\n"

        if ETFScreenerEvalRow_df['RelVol'][0] >= RelVolThreshold and tf == "1day": # High relative volume
          Score1 += 1
          ScoreXp1 += "1.0 High volume " + tf + "\n"

        if ETFScreenerEvalRow_df['RVol1M3M'][0] and tf == "1day": # Relative volatility
          Score1 += 1
          ScoreXp1 += "1.0 Low historical volatility " + tf + "\n"

        if ETFScreenerEvalRow_df['VolSqueeze'][0] and tf == "1day": # Volatility squeeze
          Score1 += 1
          ScoreXp1 += "1.0 Volatility Squeeze " + tf + "\n"

        if ETFScreenerEvalRow_df['Close'][0] >= ETFScreenerEvalRow_df['PxLowestLow'][0] and tf == "1day": # Bottomed out
          Score1 += 0.5
          ScoreXp1 += "0.5 Bottomed out " + tf + "\n"

        if ETFScreenerEvalRow_df['BullEngulfing'][0]: # Bullish engulfing candle
          Score1 += 0.5
          ScoreXp1 += "0.5 Bullish engulfing candle " + tf + "\n"

        if ETFScreenerEvalRow_df['ThreeSoldiers'][0]: # Three White Soldiers
          Score1 += 0.5
          ScoreXp1 += "0.5 Three white soldiers " + tf + "\n"

        if ETFScreenerEvalRow_df['Hammer'][0]: # Bullish Hammer
          Score1 += 0.5
          ScoreXp1 += "0.5 Bullish hammer " + tf + "\n"

      # (2) Bullish market structure. Trending.
      if (ETFScreenerEvalRow_df['Slope3M'][0] >= 0) and (ETFScreenerEvalRow_df['RSIAdv'][0]) and (ETFScreenerEvalRow_df['Close'][0] >= ETFScreenerEvalRow_df['SMA150D'][0]) and (ETFScreenerEvalRow_df['Close'][0] >= ETFScreenerEvalRow_df['SMA200D'][0]):

        if TickerID in RegETFs and tf == "1day":  # Ticker in ETF playbook. Only score once (i.e., 1day TF)
          InRegime = True
          Score2 += 2
          ScoreXp2 += "2.0 In regime " + tf + "\n"

        if ETFScreenerEvalRow_df['MACDAdv'][0]: # MACD Rising.  i.e., fast EMA rising faster than slow EMA
          Score2 += 1
          ScoreXp2 += "1.0 MACD Rising " + tf + "\n"

        if ETFScreenerEvalRow_df['MACDHist'][0] > 0: # Positive MACD.  i.e., fast EMA above slow EMA
          Score2 += 1
          ScoreXp2 += "1.0 Positive MACD " + tf + "\n"

        if ETFScreenerEvalRow_df['Close'][0] >= ETFScreenerEvalRow_df['EMA5D'][0] and tf == "1day": # Price above EMA5. Short term trend up.
          Score2 += 1
          ScoreXp2 += "1.0 Price above EMA5 " + tf + "\n"

        if ETFScreenerEvalRow_df['Close'][0] >= ETFScreenerEvalRow_df['EMA10D'][0] and tf == "1day": # Price above EMA10. Short term trend up.
          Score2 += 1
          ScoreXp2 += "1.0 Price above EMA10 " + tf + "\n"

        if ETFScreenerEvalRow_df['Close'][0] >= ETFScreenerEvalRow_df['EMA20D'][0] and tf == "1day": # Price above EMA20. Short term trend up.
          Score2 += 1
          ScoreXp2 += "1.0 Price above EMA20 " + tf + "\n"

        if ETFScreenerEvalRow_df['Close'][0] >= ETFScreenerEvalRow_df['SMA50D'][0] and tf == "1day": # Price above SMA50. Medium term trend up.
          Score2 += 1
          ScoreXp2 += "1.0 Price above SMA50 " + tf + "\n"

        if ETFScreenerEvalRow_df['EMA5D'][0] >= ETFScreenerEvalRow_df['EMA10D'][0] and tf == "1day": # EMA5D above EMA10D.
          Score2 += 1
          ScoreXp2 += "1.0 EMA5D above EMA10D " + tf + "\n"

        if ETFScreenerEvalRow_df['EMA10D'][0] >= ETFScreenerEvalRow_df['EMA20D'][0] and tf == "1day": # EMA10D above EMA20D.
          Score2 += 1
          ScoreXp2 += "1.0 EMA10D above EMA20D " + tf + "\n"

        if ETFScreenerEvalRow_df['EMA20D'][0] >= ETFScreenerEvalRow_df['SMA50D'][0] and tf == "1day": # EMA20D above SMA50D.
          Score2 += 1
          ScoreXp2 += "1.0 EMA20D above SMA50D " + tf + "\n"

        if ETFScreenerEvalRow_df['SMA50D'][0] >= ETFScreenerEvalRow_df['SMA200D'][0] and tf == "1day": # SMA50D above SMA200D.
          Score2 += 1
          ScoreXp2 += "1.0 SMA50D above SMA200D " + tf + "\n"

        if ETFScreenerEvalRow_df['Close'][0] >= ETFScreenerEvalRow_df['PivotHigh21D'][0] and tf == "1day": # 1M high.  Turtle System 1 rule.
          Score2 += 1
          ScoreXp2 += "1.0 21 day high (1M) " + tf + "\n"

        if ETFScreenerEvalRow_df['Close'][0] >= ETFScreenerEvalRow_df['PivotHigh63D'][0] and tf == "1day": # 3M high.  Turtle System 2 rule.
          Score2 += 1
          ScoreXp2 += "1.0 63 day high (3M) " + tf + "\n"

        if ETFScreenerEvalRow_df['RS5'][0] >= 0 and tf == "1day": # Positive relative strength
          Score2 += 2
          ScoreXp2 += "2.0 Positive relative strength " + tf + "\n"

        if ETFScreenerEvalRow_df['RS5'][0] >= RSThreshold and tf == "1day": # Strong relative strength
          Score2 += 2
          ScoreXp2 += "2.0 Strong relative strength " + tf + "\n"

        if ETFScreenerEvalRow_df['RS5'][0] - ETFScreenerEvalRow_df['RS10'][0] > 0 and tf == "1day": # Relative strength improving
          Score2 += 2
          ScoreXp2 += "2.0 Relative strength improving " + tf + "\n"

        if ETFScreenerEvalRow_df['RSIAdv'][0]: # RSI improving
          Score2 += 2
          ScoreXp2 += "1.0 RSI improving " + tf + "\n"

        if ETFScreenerEvalRow_df['ADX'][0] >= ADXThreshold and tf == "1day": # Strong trend. ADX above threshold.
          Score2 += 2
          ScoreXp2 += "2.0 ADX above threshold " + tf + "\n"

        if ETFScreenerEvalRow_df['RelVol'][0] >= RelVolThreshold and tf == "1day": # High relative volume
          Score2 += 1
          ScoreXp2 += "1.0 High volume " + tf + "\n"

        if ETFScreenerEvalRow_df['RVol1M3M'][0] and tf == "1day": # Relative volatility
          Score2 += 1
          ScoreXp2 += "1.0 Low historical volatility " + tf + "\n"

        if ETFScreenerEvalRow_df['VolSqueeze'][0] and tf == "1day": # Volatility squeeze
          Score2 += 1
          ScoreXp2 += "1.0 Volatility Squeeze " + tf + "\n"

        if ETFScreenerEvalRow_df['BullEngulfing'][0]: # Bullish engulfing candle
          Score2 += 0.5
          ScoreXp2 += "0.5 Bullish engulfing candle " + tf + "\n"

        if ETFScreenerEvalRow_df['ThreeSoldiers'][0]: # Three White Soldiers
          Score2 += 0.5
          ScoreXp2 += "0.5 Three white soldiers " + tf + "\n"

        if ETFScreenerEvalRow_df['Hammer'][0]: # Bullish Hammer
          Score2 += 0.5
          ScoreXp2 += "0.5 Bullish hammer " + tf + "\n"

        if ETFScreenerEvalRow_df['ADX'][0] >= ADXThreshold and ETFScreenerEvalRow_df['Reg1MZScore'][0] <= (ZScoreThreshold * -1) and tf == "1day": # Strong trend and 1M trend channel oversold by threshold.
          Score2 += 1
          ScoreXp2 += "1.0 Strong ADX and 1M channel oversold by threshold " + tf + "\n"

        if ETFScreenerEvalRow_df['ADX'][0] >= ADXThreshold and ETFScreenerEvalRow_df['Reg3MZScore'][0] <= (ZScoreThreshold * -1) and tf == "1day": # Strong trend and 3M trend channel oversold by threshold.
          Score2 += 1
          ScoreXp2 += "1.0 Strong ADX and 3M channel oversold by threshold " + tf + "\n"

      # (3) Bearish market structure. Mean reversion.
      if (ETFScreenerEvalRow_df['Slope3M'][0] < 0) and (ETFScreenerEvalRow_df['Reg3MZScore'][0] > -0.2) and (not ETFScreenerEvalRow_df['StochAdv'][0]):

        if TickerID not in RegETFs and tf == "1day":  # Ticker not in ETF playbook. Only score once (i.e., 1day TF)
          InRegime = False
          Score3 += 2
          ScoreXp3 += "2.0 Not in regime " + tf + "\n"

        if not ETFScreenerEvalRow_df['MACDAdv'][0]: # MACD falling.  i.e., fast EMA falling faster than slow EMA
          Score3 += 1
          ScoreXp3 += "1.0 MACD falling " + tf + "\n"

        if ETFScreenerEvalRow_df['Close'][0] > ETFScreenerEvalRow_df['EMA5D'][0] and tf == "1day": # Price above EMA5. Short term trend up.
          Score3 += 1
          ScoreXp3 += "1.0 Price above EMA5 " + tf + "\n"

        if ETFScreenerEvalRow_df['Close'][0] > ETFScreenerEvalRow_df['EMA10D'][0] and tf == "1day": # Price above EMA10. Short term trend up.
          Score3 += 1
          ScoreXp3 += "1.0 Price above EMA10 " + tf + "\n"

        if ETFScreenerEvalRow_df['Close'][0] > ETFScreenerEvalRow_df['EMA20D'][0] and tf == "1day": # Price above EMA20. Short term trend up.
          Score3 += 1
          ScoreXp3 += "1.0 Price above EMA20 " + tf + "\n"

        if ETFScreenerEvalRow_df['RS5'][0] - ETFScreenerEvalRow_df['RS10'][0] < 0 and tf == "1day": # Relative strength worsening.
          Score3 += 2
          ScoreXp3 += "2.0 Relative strength worsening " + tf + "\n"

        if ETFScreenerEvalRow_df['RSILowest'][0] >= OBThreshold: # RSI overbought
          Score3 += 1
          ScoreXp3 += "1.0 RSI overbought " + tf + "\n"

        if ETFScreenerEvalRow_df['StochLowest'][0] <= OBThreshold: # Stochastic overbought
          Score3 += 1
          ScoreXp3 += "1.0 Stochastic overbought " + tf + "\n"

        if not ETFScreenerEvalRow_df['StochAdv'][0]: # Stochastic worsening
          Score3 += 2
          ScoreXp3 += "2.0 Stochastic worsening " + tf + "\n"

        if ETFScreenerEvalRow_df['BBZScore'][0] >= 1: # Bollinger band overbought
          Score3 += 1
          ScoreXp3 += "1.0 BB overbought by 1 std " + tf + "\n"

        if ETFScreenerEvalRow_df['BBZScore'][0] >= ZScoreThreshold: # Bollinger band overbought
          Score3 += 1
          ScoreXp3 += "1.0 BB overbought by threshold " + tf + "\n"

        if ETFScreenerEvalRow_df['Reg1MZScore'][0] >= 1 and tf == "1day": # 1M trend channel overbought
          Score3 += 1
          ScoreXp3 += "1.0 1M channel overbought by 1 std " + tf + "\n"

        if ETFScreenerEvalRow_df['Reg3MZScore'][0] >= 1 and tf == "1day": # 3M trend channel overbought
          Score3 += 1
          ScoreXp3 += "1.0 3M channel overbought by 1 std " + tf + "\n"

        if ETFScreenerEvalRow_df['Reg1MZScore'][0] >= ZScoreThreshold and tf == "1day": # 1M trend channel overbought
          Score3 += 1
          ScoreXp3 += "1.0 1M channel overbought by threshold " + tf + "\n"

        if ETFScreenerEvalRow_df['Reg3MZScore'][0] >= ZScoreThreshold and tf == "1day": # 3M trend channel overbought
          Score3 += 1
          ScoreXp3 += "1.0 3M channel overbought by threshold " + tf + "\n"

        if ETFScreenerEvalRow_df['RetPerc63D'][0] >= FibThreshold and tf == "1day": # 3M Fibonacci retraced to threshold.
          Score3 += 2
          ScoreXp3 += "2.0 3M Fib retraced to threshold " + tf + "\n"

        if ETFScreenerEvalRow_df['RetPerc63D'][0] >= 78 and tf == "1day": # 3M Fibonacci retraced 78%
          Score3 += 1
          ScoreXp3 += "1.0 3M Fib retraced 78% " + tf + "\n"

        if ETFScreenerEvalRow_df['RetPerc21D'][0] >= FibThreshold and tf == "1day": # 1M Fibonacci retraced
          Score3 += 2
          ScoreXp3 += "2.0 1M Fib retraced to threshold " + tf + "\n"

        if ETFScreenerEvalRow_df['RetPerc21D'][0] >= 78 and tf == "1day": # 1M Fibonacci retraced 78%
          Score3 += 1
          ScoreXp3 += "1.0 1M Fib retraced 78% " + tf + "\n"

        if ETFScreenerEvalRow_df['Close'][0] >= ETFScreenerEvalRow_df['VWAP3MPx'][0] and tf == "1day": # 3M VWAP overbought
          Score3 += 1
          ScoreXp3 += "1.0 3M VWAP overbought " + tf + "\n"

        if ETFScreenerEvalRow_df['Close'][0] >= ETFScreenerEvalRow_df['VWAP1MPx'][0] and tf == "1day": # 1M VWAP overbought
          Score3 += 1
          ScoreXp3 += "1.0 1M VWAP overbought " + tf + "\n"

        if ETFScreenerEvalRow_df['RelVol'][0] >= RelVolThreshold and tf == "1day": # High relative volume
          Score3 += 1
          ScoreXp3 += "1.0 High volume " + tf + "\n"

        if ETFScreenerEvalRow_df['RVol1M3M'][0] and tf == "1day": # Relative volatility
          Score3 += 1
          ScoreXp3 += "1.0 Low historical volatility " + tf + "\n"

        if ETFScreenerEvalRow_df['VolSqueeze'][0] and tf == "1day": # Volatility squeeze
          Score3 += 1
          ScoreXp3 += "1.0 Volatility Squeeze " + tf + "\n"

        if ETFScreenerEvalRow_df['Close'][0] <= ETFScreenerEvalRow_df['PxHighestHigh'][0] and tf == "1day": # Topped out
          Score3 += 0.5
          ScoreXp3 += "0.5 Topped out " + tf + "\n"

        if ETFScreenerEvalRow_df['BearEngulfing'][0]: # Bearish engulfing candle
          Score3 += 0.5
          ScoreXp3 += "0.5 Bearish engulfing candle " + tf + "\n"

        if ETFScreenerEvalRow_df['ThreeCrows'][0]: # Three black crows
          Score3 += 0.5
          ScoreXp3 += "0.5 Three black crows " + tf + "\n"

        if ETFScreenerEvalRow_df['HangingMan'][0]: # Hanging man (Bearish Hammer)
          Score3 += 0.5
          ScoreXp3 += "0.5 Hanging man " + tf + "\n"

      # (4) Bearish market structure. Trending.
      if (ETFScreenerEvalRow_df['Slope3M'][0] < 0) and (not ETFScreenerEvalRow_df['RSIAdv'][0]) and (ETFScreenerEvalRow_df['Close'][0] < ETFScreenerEvalRow_df['SMA150D'][0]) and (ETFScreenerEvalRow_df['Close'][0] < ETFScreenerEvalRow_df['SMA200D'][0]):

        if TickerID not in RegETFs and tf == "1day":  # Ticker not in ETF playbook. Only score once (i.e., 1day TF)
          InRegime = True
          Score4 += 2
          ScoreXp4 += "2.0 In regime " + tf + "\n"

        if not ETFScreenerEvalRow_df['MACDAdv'][0]: # MACD falling.  i.e., fast EMA falling faster than slow EMA
          Score4 += 1
          ScoreXp4 += "1.0 MACD Rising " + tf + "\n"

        if ETFScreenerEvalRow_df['MACDHist'][0] < 0: # Negative MACD.  i.e., fast EMA below slow EMA
          Score4 += 1
          ScoreXp4 += "1.0 Positive MACD " + tf + "\n"

        if ETFScreenerEvalRow_df['Close'][0] < ETFScreenerEvalRow_df['EMA5D'][0] and tf == "1day": # Price below EMA5. Short term trend down.
          Score4 += 1
          ScoreXp4 += "1.0 Price below EMA5 " + tf + "\n"

        if ETFScreenerEvalRow_df['Close'][0] < ETFScreenerEvalRow_df['EMA10D'][0] and tf == "1day": # Price below EMA10. Short term trend down.
          Score4 += 1
          ScoreXp4 += "1.0 Price below EMA10 " + tf + "\n"

        if ETFScreenerEvalRow_df['Close'][0] < ETFScreenerEvalRow_df['EMA20D'][0] and tf == "1day": # Price below EMA20. Short term trend down.
          Score4 += 1
          ScoreXp4 += "1.0 Price below EMA20 " + tf + "\n"

        if ETFScreenerEvalRow_df['Close'][0] < ETFScreenerEvalRow_df['SMA50D'][0] and tf == "1day": # Price below SMA50. Medium term trend down.
          Score4 += 1
          ScoreXp4 += "1.0 Price below SMA50 " + tf + "\n"

        if ETFScreenerEvalRow_df['EMA5D'][0] < ETFScreenerEvalRow_df['EMA10D'][0] and tf == "1day": # EMA5D below EMA10D.
          Score4 += 1
          ScoreXp4 += "1.0 EMA5D below EMA10D " + tf + "\n"

        if ETFScreenerEvalRow_df['EMA10D'][0] < ETFScreenerEvalRow_df['EMA20D'][0] and tf == "1day": # EMA10D below EMA20D.
          Score4 += 1
          ScoreXp4 += "1.0 EMA10D below EMA20D " + tf + "\n"

        if ETFScreenerEvalRow_df['EMA20D'][0] < ETFScreenerEvalRow_df['SMA50D'][0] and tf == "1day": # EMA20D below SMA50D.
          Score4 += 1
          ScoreXp4 += "1.0 EMA20D below SMA50D " + tf + "\n"

        if ETFScreenerEvalRow_df['SMA50D'][0] < ETFScreenerEvalRow_df['SMA200D'][0] and tf == "1day": # SMA50D below SMA200D.
          Score4 += 1
          ScoreXp4 += "1.0 SMA50D below SMA200D " + tf + "\n"

        if ETFScreenerEvalRow_df['Close'][0] <= ETFScreenerEvalRow_df['PivotLow21D'][0] and tf == "1day": # 1M low.  Turtle System 1 rule.
          Score4 += 1
          ScoreXp4 += "1.0 21 day low (1M) " + tf + "\n"

        if ETFScreenerEvalRow_df['Close'][0] <= ETFScreenerEvalRow_df['PivotLow63D'][0] and tf == "1day": # 3M low.  Turtle System 2 rule.
          Score4 += 1
          ScoreXp4 += "1.0 63 day low (3M) " + tf + "\n"

        if ETFScreenerEvalRow_df['RS5'][0] < 0 and tf == "1day": # Negative relative strength
          Score4 += 2
          ScoreXp4 += "2.0 Negative relative strength " + tf + "\n"

        if ETFScreenerEvalRow_df['RS5'][0] <= (RSThreshold * -1) and tf == "1day": # Weak relative strength
          Score4 += 2
          ScoreXp4 += "2.0 Weak relative strength " + tf + "\n"

        if ETFScreenerEvalRow_df['RS5'][0] - ETFScreenerEvalRow_df['RS10'][0] < 0 and tf == "1day": # Relative strength weakening
          Score4 += 2
          ScoreXp4 += "2.0 Relative strength weakening " + tf + "\n"

        if not ETFScreenerEvalRow_df['RSIAdv'][0]: # RSI worsening
          Score4 += 2
          ScoreXp4 += "1.0 RSI worsening " + tf + "\n"

        if ETFScreenerEvalRow_df['ADX'][0] >= ADXThreshold and tf == "1day": # Strong trend. ADX above threshold.
          Score4 += 2
          ScoreXp4 += "2.0 ADX above threshold " + tf + "\n"

        if ETFScreenerEvalRow_df['RelVol'][0] >= RelVolThreshold and tf == "1day": # High relative volume
          Score4 += 1
          ScoreXp4 += "1.0 High volume " + tf + "\n"

        if ETFScreenerEvalRow_df['RVol1M3M'][0] and tf == "1day": # Relative volatility
          Score4 += 1
          ScoreXp4 += "1.0 Low historical volatility " + tf + "\n"

        if ETFScreenerEvalRow_df['VolSqueeze'][0] and tf == "1day": # Volatility squeeze
          Score4 += 1
          ScoreXp4 += "1.0 Volatility Squeeze " + tf + "\n"

        if ETFScreenerEvalRow_df['BearEngulfing'][0]: # Bearish engulfing candle
          Score4 += 0.5
          ScoreXp4 += "0.5 Bearish engulfing candle " + tf + "\n"

        if ETFScreenerEvalRow_df['ThreeCrows'][0]: # Three black crows
          Score4 += 0.5
          ScoreXp4 += "0.5 Three black crows " + tf + "\n"

        if ETFScreenerEvalRow_df['HangingMan'][0]: # Hanging man (Bearish Hammer)
          Score4 += 0.5
          ScoreXp4 += "0.5 Hanging man " + tf + "\n"

        if ETFScreenerEvalRow_df['ADX'][0] >= ADXThreshold and ETFScreenerEvalRow_df['Reg1MZScore'][0] >= ZScoreThreshold and tf == "1day": # Strong trend and 1M trend channel overbought by threshold.
          Score4 += 1
          ScoreXp4 += "1.0 Strong ADX and 1M channel oversold by threshold " + tf + "\n"

        if ETFScreenerEvalRow_df['ADX'][0] >= ADXThreshold and ETFScreenerEvalRow_df['Reg3MZScore'][0] >= ZScoreThreshold and tf == "1day": # Strong trend and 3M trend channel overbought by threshold.
          Score4 += 1
          ScoreXp4 += "1.0 Strong ADX and 3M channel oversold by threshold " + tf + "\n"

      print(f"{TickerID} {tf} Score 1: {Score1} Score 2: {Score2} Score 3: {Score3} Score 4: {Score4} Score: {Score} Trade: {Trade}")

    except Exception as e:
      print(f"Exception {TickerID} {tf}")
      logger.error('%s', repr(e))

  else:  # End of timeframe loop

    # Determine most probable setup
    if Score1 >= Score2 and Score1 >= Score3 and Score1 >= Score4 and Score1 != 0:  # Bullish trend. Mean reversion.
      Score = math.ceil(Score1) # Rounds up
      Trade = "MR"
      ScoreXp = ScoreXp1
    if Score2 >= Score1 and Score2 >= Score3 and Score2 >= Score4 and Score2 != 0:  # Bullish trend. Trending.
      Score = math.ceil(Score2)
      Trade = "TF"
      ScoreXp = ScoreXp2
    if Score3 >= Score1 and Score3 >= Score2 and Score3 >= Score4 and Score3 != 0:  # Bearish trend. Mean reversion.
      Score = math.ceil(Score3 * -1)
      Trade = "MR"
      ScoreXp = ScoreXp3
    if Score4 >= Score1 and Score4 >= Score2 and Score4 >= Score3 and Score4 != 0:  # Bearish trend. Trending.
      Score = math.ceil(Score4 * -1)
      Trade = "TF"
      ScoreXp = ScoreXp4

    print(f"{TickerID} Score 1: {Score1} Score 2: {Score2} Score 3: {Score3} Score 4: {Score4} Score: {Score} Trade: {Trade}")

    # Append to scoring dataframe
    ETFScoreRow_df = pd.DataFrame({'Ticker': [TickerID], 'Description': [TickerDesc], 'Class': [TickerClass], 'Trade': [Trade], 'Score': [Score], 'Change1D': [Change1D], 'Reg3MZScore': [Reg3MZScore], 'ATRx': [ATRx], 'Explanation': [ScoreXp]})

    if i == 0:  # Prevents concat with initial null dataframe which Python dislikes
      ETFScore_df = ETFScoreRow_df
    else:
      ETFScore_df = pd.concat([ETFScore_df, ETFScoreRow_df], sort = False)


##### OUTPUT #####
# Set the display width to 100 characters
pd.set_option('display.width', 400)
#print(f"Screener:\n\n{ETFScreener_df}")
#print(f"ETF Scores:\n\n{ETFScore_df}")


#ETFScreener_df.to_csv('../AutoTA/ETFScreener.csv', index = False)
#ETFScreener_df.to_html('../AutoTA/ETFScreener.html', index = False, justify = 'center')
#ETFScore_df.to_html('../AutoTA/ETFScore.html', index = False, justify = 'center')

if SymbList_arg == "1":
  ETFScore_df.to_sql(name = 'Portfolio_Score', con = connection, if_exists = 'replace', index = False) 

if SymbList_arg == "2":
  ETFScore_df.to_sql(name = 'AssetClass_Score', con = connection, if_exists = 'replace', index = False) 

if SymbList_arg == "3":
  ETFScore_df.to_sql(name = 'ETF_Score', con = connection, if_exists = 'replace', index = False) 

if SymbList_arg == "4":
  ETFScore_df.to_sql(name = 'Stock_Score', con = connection, if_exists = 'replace', index = False) 

if SymbList_arg == "5":
  ETFScore_df.to_sql(name = 'Test_Score', con = connection, if_exists = 'replace', index = False) 

if SymbList_arg == "6":
  ETFScore_df.to_sql(name = 'Problem_Score', con = connection, if_exists = 'replace', index = False) 


#ETFScore_df.to_sql(name = sqltbl, con = connection, if_exists = 'replace', index = False) 
connection.close()

#ETFScreenerEvalRow_df.to_html('../AutoTA/ETFScreenerEvalRow.html', index = False, justify = 'center')

current_dt_5h_end = datetime.now() - relativedelta(hours=5) # changes to EST
processingtime = current_dt_5h_end - current_dt_5h_start

print(f"\nDone! {current_dt_5h_end:%B %d, %Y %H:%M:%S}")
print(f"\nDuration (seconds): {processingtime}\n")

##### END #####


# In[ ]:





# In[ ]:




