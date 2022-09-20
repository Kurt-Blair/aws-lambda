try:
    # For Python 3.0 and later
    from urllib.request import urlopen
except ImportError:
    # Fall back to Python 2's urllib2
    from urllib2 import urlopen

import certifi
import json
from datetime import datetime
import pandas as pd
import pandasql as ps
from pandas import json_normalize
from urllib.parse import quote_plus

def lambda_handler(event, context):
    # TODO implement
   
    gainers_url = ("https://financialmodelingprep.com/api/v3/stock_market/gainers?apikey=X")
    response = urlopen(gainers_url, cafile=certifi.where())
    data0 = response.read().decode("utf-8")
    data = json.loads(data0)
   
    dics = []
   
    for count,record in enumerate(data):
        dics.append(data[count]['symbol'])
       
    string = ""
   
    for l in dics:
     if l != dics[-1]:
          string = string + str(l) + ","
     else:
         string = string + str(l)
     
    quotesurl = ("https://financialmodelingprep.com/api/v3/quote/" + string + "?apikey=X")

    response = urlopen(quotesurl, cafile=certifi.where())
    data0 = response.read().decode("utf-8")
    data = json.loads(data0)
   
    df = pd.DataFrame(data)    
    top_gainers = ps.sqldf("""
    SELECT
    symbol
    ,round(price,2) as price
    ,round(dayHigh,2) as day_high
    ,round(open,2) as open
    ,round((price-open)/open*100,2) || '%' as gain_since_open
    ,round((dayHigh-price)/dayHigh*100,2) || '%' as percent_off_high
    ,round(changesPercentage,2) || '%' as gains_since_prev_close
    ,timestamp
    FROM df
    WHERE gain_since_open > percent_off_high
    AND symbol NOT IN ('NaN','NA')
    AND symbol IS NOT NULL
    ORDER BY (price-open)/open DESC
    """)
   
    top_gainers['timestamp'] = [datetime.fromtimestamp(x) for x in top_gainers['timestamp']]
   
    top_gainers.to_csv('s3://gainerbucket/gainers.csv')
   
    return top_gainers
