import requests
import time
import os



url = 'https://api.coincap.io/v2/assets'

params = {
    'search' : 'BTC',
    'limit': 1
}

while True: 

    r = requests.get(url, params=params)
    data = r.json()
    print(data['data'][0]['name'])
    print(data['data'][0]['priceUsd'])
    time.sleep(31)
    