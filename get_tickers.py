from bs4 import BeautifulSoup, SoupStrainer
import requests
import pandas as pd

a = list(range(65,65+26))
pages = [chr(c)+'.htm' for c in a]

base = 'https://eoddata.com/stocklist/NYSE/'

strainer = SoupStrainer("tr",class_=['ro','re'])
tickers = list()
    for page in pages:
        print(page)
        resp = requests.get(base + page)
        soup = BeautifulSoup(resp.content, 'lxml', parse_only=strainer)
        soup.find_all(class_=['ro','re'])
        links = soup.find_all("a")
        for link in links:
            tickers.append(link.get_text().strip())

tickers = [t for t in tickers if t!='']
df = pd.DataFrame(tickers)

df.to_csv('tickers.csv', index=False, header=False)