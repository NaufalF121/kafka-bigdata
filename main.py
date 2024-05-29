import requests
import time
import os
import finnhub


API_KEY = os.getenv("API_KEY")
finnhub_client = finnhub.Client(api_key=API_KEY)
url = 'https://api.coincap.io/v2/assets'

params = {
    'search' : 'BTC',
    'limit': 1
}

while True: 

    r = requests.get(url, params=params)
    data = r.json()
    print("Cryptocurrency")
    print(data['data'][0]['name'])
    print(data['data'][0]['priceUsd'])
    print("Stock")
    print(finnhub_client.quote('PBCRF'))
    time.sleep(31)
    