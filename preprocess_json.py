from multiprocessing import Pool, cpu_count, freeze_support
import pandas as pd
from glob import glob
import ast
import os


def proc(file):
    print ('....', file)
    json_days = []
    with open(file, 'r') as stock:
        days = stock.read()
        oloc = days.find('[')
        while oloc >= 0:
            cloc = days.find(']',oloc)
            json_day = days[oloc+1:cloc]
            if cloc - oloc > 2:
                json_day = '[' + json_day + ']'
                json_day = ast.literal_eval(json_day)
                json_days.append(json_day)
            oloc = days.find('[', cloc)
    ticker = os.path.basename(file)
    ticker = ticker[:ticker.find('.')]
    if not os.path.exists('CSV/%s.csv'%ticker):
        mode = 'w'
    else:
        mode = 'a'
    toCSV(json_days,ticker,mode)

    

def preprocess(data_dir,gofast=True):
    if gofast == False:
        cores = int(cpu_count()*.8)
    else:
        cores = cpu_count()         
    print ("Preprocessing using %s cores:"%cores)
    pool = Pool()
    for file in glob(data_dir + '/*.txt'):
        pool.apply_async(proc, args = (file,))
    pool.close()
    pool.join()
            


def toCSV(json_days,ticker,mode):
    stock_df = pd.DataFrame()
    for json_day in json_days:
        day_df = pd.DataFrame.from_records(json_day)
        sLen = len(day_df.index)
        ticker_col = pd.Series([ticker]*sLen)
        day_df['Ticker'] = ticker_col
        stock_df = stock_df.append(day_df,sort=True)
    if mode == 'w':
        stock_df.to_csv('CSV/%s.csv'%ticker,na_rep='None')
    else:
        with open('CSV/%s.csv'%ticker, 'a') as csv:
            stock_df.to_csv(csv,na_rep='None',header=False, index=False)
        stock_df = pd.read_csv('CSV/%s.csv'%ticker)
        stock_df = stock_df.drop_duplicates()
        stock_df.to_csv('CSV/%s.csv'%ticker,na_rep='None')
        

if __name__ == "__main__":
    freeze_support()
    preprocess('Data')    
    

