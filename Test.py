import requests
import lxml.html as lh
from itertools import cycle, islice
from matplotlib import colors
import pandas as pd
import matplotlib.pyplot as plt
from bs4 import BeautifulSoup
%matplotlib inline



url = 'https://www.invertironline.com/titulo/cotizacion/BCBA/GGAL/GRUPO-FINANCIERO-GALICIA-S.A/opciones'

# Make a GET request to fetch the raw HTML content
html_content = requests.get(url).text

# Parse the html content
soup = BeautifulSoup(html_content, "html.parser")
#print(soup.prettify()) # print the parsed data of html
print(len(soup))
#print(soup.title)
#print(soup.title.text)

# for link in soup.find_all("a"):
#     print("Inner Text: {}".format(link.text))
#     print("Title: {}".format(link.get("title")))
#     print("href: {}".format(link.get("href")))

gdp_table = soup.find('table', attrs={'class':'table table-striped fontsize12'})
print(gdp_table)
gdp_table_data = gdp_table.find_all('tr')  # contains 2 rows
print(gdp_table_data)


# Get all the headings of Lists
headings = []
for td in gdp_table_data[0].find_all("td"):
    # remove any newlines and extra spaces from left and right
    headings.append(td.b.text.replace('\n', ' ').strip())

# print(headings)

