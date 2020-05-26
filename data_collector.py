import json
import boto3
import os
import subprocess
import sys

subprocess.check_call([sys.executable, "-m", "pip", "install", "--target", "/tmp", 'yfinance'])
sys.path.append('/tmp')
import yfinance as yf


def lambda_handler(event, context):
    symbols = ['FB', 'SHOP', 'BYND', 'NFLX', 'PINS', 'SQ', 'TTD', 'OKTA', 'SNAP', 'DDOG']
    tickers = yf.Tickers(' '.join(symbols))
    res = tickers.history(start="2020-05-14", end="2020-05-15", period='1d', interval='1m', group_by='row')
    records = []
    for st in symbols:
        for i in range(len(res)):
            ss = res.iloc[i].loc[st,['High','Low']]
            ss.reset_index(level=0, drop=True, inplace=True)
            ss.rename({'High':'high', "Low":'low'},inplace=True)
            ss['ts'] = str(res.index[i])
            ss['name'] = st
            records.append(ss.to_json())
    
    fh = boto3.client("firehose", "us-east-2")

    for r in records:
        fh.put_record(
                DeliveryStreamName="DataTransformer",
                Record={"Data":r.encode('utf-8')}
        )
    
    return {'statusCode': 200, 'body': json.dumps(f'Done! Records')}