import requests
from bs4 import BeautifulSoup, SoupStrainer
import pandas as pd

def get_sectors():
    urls = ['https://www.stockmonitor.com/sector/basic-materials/',
            'https://www.stockmonitor.com/sector/communication-services/',
            'https://www.stockmonitor.com/sector/consumer-cyclical/',
            'https://www.stockmonitor.com/sector/consumer-defensive/',
            'https://www.stockmonitor.com/sector/energy/',
            'https://www.stockmonitor.com/sector/financial-services/',
            'https://www.stockmonitor.com/sector/healthcare/',
            'https://www.stockmonitor.com/sector/industrials/',
            'https://www.stockmonitor.com/sector/technology/',
            'https://www.stockmonitor.com/sector/utilities/'
            ]
    sectors = ['Basic Materials',
               'Communication Services',
               'Consumer Cyclical',
               'Consumer Defensive',
               'Energy',
               'Financial Services',
               'Healthcare',
               'Industrials',
               'Technology',
               'Utilities'
               ]
    df = pd.DataFrame(columns=['Symbol','Sector'])
    for url, sector in zip(urls, sectors):
        resp = requests.get(url)
        table = SoupStrainer('tbody')
        soup = BeautifulSoup(resp.content, 'lxml', parse_only=table)
        links = soup.find_all('a')
        tickers = list()
        for link in links:
            tickers.append(link.get_text())
        tmp = pd.DataFrame(tickers, columns=['Symbol'])
        tmp['Sector'] = sector
        df = pd.concat([df, tmp], ignore_index=True)
    df.to_csv('stock_monitor_tickers.csv', index=False)