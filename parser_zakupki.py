import pandas as pd
import matplotlib.pyplot as plt
import feedparser
import re
from multiprocessing import Pool
from multiprocessing.dummy import Pool as ThreadPool

def get_url():
"""create links. Specify the period in the presented format. Enter ID"""
    ID_reg = "5277386" #Republic of tuva
    date_start = "2018.01.01"
    date_end = "2018.11.14"
    daterange = pd.date_range(date_start, date_end)
    url = []
    for i in daterange:
        start_time = i.strftime("%d.%m.%Y")
        end_time = (i+1).strftime("%d.%m.%Y")
        urlgrab = "http://www.zakupki.gov.ru/epz/order/extendedsearch/rss?morphology=on&pageNumber=1&sortDirection=false&recordsPerPage=_500&showLotsInfoHidden=false&fz44=on&fz223=on&ppRf615=on&fz94=on&af=true&ca=true&pc=true&pa=true&currencyIdGeneral=-1&publishDateFrom="+start_time+"&publishDateTo="+end_time+"&regions="+ID_reg+"&regionDeleted=false&sortBy=UPDATE_DATE&openMode=USE_DEFAULT_PARAMS"
        url.append(urlgrab)
    return url
    
 def fetch_url():
 """parser function"""
    pool = ThreadPool(2) #more than two threads cause an error bonzo
    d = pool.map(feedparser.parse, get_url())
    dataframe = []
    for p in range(0,len(d)):
        for i in d[p]["entries"]:
            link =  [i["link"]]
            autor = [i["author"]]
            description = (i["description"])
            info = (re.findall(r'Наименование объекта закупки: </strong>([^<]+)<br/>', description))
            date = (re.findall(r'Размещено: </strong>([^<]+)<br/>', description))
            update = (re.findall(r'Обновлено: </strong>([^<]+)<br/>', description))
            price = (re.findall(r'Начальная цена контракта: </strong>([^<]+)<strong>', description))
            etap = (re.findall(r'Этап размещения: </strong>([^<]+)<br/>', description))
            etap = (re.findall(r'Этап размещения: </strong>([^<]+)<br/>', description))
            post = autor + info +  price + date + update + etap + link
            dataframe.append(post) 
    return dataframe
    
df = pd.DataFrame.from_dict(fetch_url())
df = df.rename(columns={0: "company", 1: "info", 2: "suma", 3: "date", 4: "update", 5: "status", 6: "link"})
df["suma"] = pd.to_numeric(df["suma"], errors='coerce')
df["date"] = pd.to_datetime(df["date"])
df["update"] = pd.to_datetime(df["update"])
df
