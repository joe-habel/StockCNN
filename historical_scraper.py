from multiprocessing import Pool, cpu_count, freeze_support
import requests
from requests import Timeout
from json import JSONDecodeError
from time import sleep
import datetime
import os


def get_symbols():
    with open('nyse.txt', 'r') as sym:
        symbols = sym.readlines()
    return symbols 


def next_date(date):
    month_dict = {1:31,2:[28,29],3:31,4:30,5:31,6:30,7:31,8:31,9:30,10:31,11:30,12:31}
    date = str(date)
    year = int(date[:4])
    month = int(date[4:6])
    day = int(date[6:])
    if month == 2: 
        if (year % 4 == 0) and (year % 100 != 0 or year % 400 == 0):
            if month_dict[month][1] == day:
                day = 1
                month += 1
        else:
            if month_dict[month][0] == day:
                day = 1
                month += 1
            else:
                day += 1
    elif month_dict[month] == day:
        if month == 12:
            month = 1
            day = 1
            year += 1
        else:
            day = 1 
            month += 1
    else:
        day += 1
    if month < 10:
        str_month = '0' + str(month)
    else:
        str_month = str(month)
    if day < 10:
        str_day = '0' + str(day)
    else:
        str_day = str(day)
    return str(year) + str_month + str_day

def get_last_date(symbol):
    if not os.path.exists('Data/%s.txt'%symbol.strip()):
        return None
    with open('Data/%s.txt'%symbol.strip(), 'r') as json_day:
        txt = json_day.read()
    last_date_loc = txt.rfind('date')
    if last_date_loc < 0:
        return None
    last_date = txt[last_date_loc + 8: last_date_loc + 16]
    return last_date

def date_diff(last_date):
    today = datetime.date.today()
    last_date = datetime.datetime.strptime(last_date, '%Y%m%d').date()
    delta = today - last_date
    return min(30,delta.days)


def no_last_date():
    start = datetime.date.today() + datetime.timedelta(-30)
    return start.strftime('%Y%m%d')
    

def proc(symbol,sleep_time):
    url = 'https://api.iextrading.com/1.0/stock/%s/chart/date/'%symbol.strip()
    last_date = get_last_date(symbol)
    if last_date is not None:
        prev = date_diff(last_date)
        date = next_date(last_date)
    else:
        prev = 30
        date = no_last_date()
    print ('....' + symbol.strip() + ' || %s days to update'%prev)
    for i in range(prev):
        try:
            r = requests.get(url+date,timeout=10)
        except Timeout:
            print ("Timeout")
            with open('TimedOutData.txt', 'a') as nd:
                nd.write('%s,%s\n'%(symbol,date))
        with open('Data/%s.txt'%symbol.strip(), 'a') as hist:
            try:
                hist.write(str(r.json()))
            except JSONDecodeError:
                pass
        sleep(sleep_time)
        date = next_date(date)

def scrape(sleep_time,gofast=True): 
    symbols = get_symbols()
    if gofast == False:
        cores = int(cpu_count()*.8)
    else:
        cores = cpu_count()
    print ('Scraping Using %s cores:'%cores)
    pool = Pool(cores)
    for symbol in symbols:
        pool.apply_async(proc, args=(symbol,sleep_time))
    pool.close()
    pool.join()
   
if __name__ == "__main__":
    freeze_support()
    scrape(0.25)
