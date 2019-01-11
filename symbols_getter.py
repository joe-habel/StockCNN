import requests
from bs4 import BeautifulSoup

r = requests.get('http://www.wsj.com/mdc/public/page/2_3024-NYSE.html')

soup = BeautifulSoup(r.text, 'html.parser')

symbols = []

for table in soup.find_all('table'):
    for link in table.find_all('a'):
        symbols.append(link.text.strip())
with open('nyse.txt', 'w') as sym:    
    for i,symbol in enumerate(symbols[1:]):
        if i == len(symbols) - 1:
            sym.write(symbol)
            continue
        sym.write(symbol + '\n')