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
    ,cast(price as float) as price
    ,cast(dayHigh as float) as dayHigh
    ,cast(open as float) as open
    ,cast(((price-open)/open)*100 as float) as gains_since_open
    ,cast(((dayHigh-price)/dayHigh)*100 as float) as percent_off_high
    ,cast(changesPercentage as float) as gains_since_prev_close
    FROM df 
    WHERE gains_since_open > percent_off_high 
    ORDER BY (price-open)/open DESC
    """)
    
    top_gainers.to_json(r's3://gainerbucket/gainers/gainers.json',orient='index')
    
    return top_gainers
