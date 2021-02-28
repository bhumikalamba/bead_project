import os
from datetime import datetime, timedelta
import json

_main_script = "twitterHistorical.py"

_start_date = '2021-02-26' ## Amend start date for a different timeperiod
json_data = None

with open('twitterTokens.json') as json_file:
    json_data = json.load(json_file)

if json_data is not None:
    for token_detail in json_data:

        start_date = datetime.strptime(_start_date,'%Y-%m-%d')
        start_date_str = start_date.strftime('%Y-%m-%d')
        call_script = _main_script + ' '+ str(token_detail.get('CONSUMER_KEY')) + ' ' +str(token_detail.get('CONSUMER_SECRET')) +\
                          ' ' +str(token_detail.get('ACCESS_TOKEN')) + ' '+ str(token_detail.get('ACCESS_SECRET')) + ' ' + start_date_str + ' ' + str(token_detail.get('NAME'))
        os.system(call_script)
        _start_date = datetime.strptime(start_date_str,'%Y-%m-%d') + timedelta(1)
        _start_date = _start_date.strftime('%Y-%m-%d')

