from bs4 import BeautifulSoup
import requests
import pandas as pd


url = "https://web.archive.org/web/20200318083015/https://en.wikipedia.org/wiki/List_of_largest_banks"
html = requests.get(url)

soup = BeautifulSoup(html.content,"html.parser")
table = soup.find("table", class_="mw-content-text > div > table:nth-child(9)r")
print(table)
