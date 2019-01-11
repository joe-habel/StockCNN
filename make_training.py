from multiprocessing import Pool, cpu_count, freeze_support
import pandas as pd
import numpy as np
from glob import glob
import pickle


class TrainingData(object):
    def __init__(self):
        self.data = {
                'X_train' : None,
                'Y_train' : None,
                'X_test' : None,
                'Y_test' : None,
                'preds' : []}
    def update_data(self,result):
        x_train,x_test,y_train,y_test,predictions = result
        x_train = x_train.reshape((int(x_train.shape[0]/390),390,6))
        x_test = x_test.reshape((int(x_test.shape[0]/390),390,6))
        if self.data['X_train'] is None:
            self.data['X_train'] = x_train
            self.data['Y_train'] = y_train
        else:
            self.data['X_train'] = np.vstack((self.data['X_train'],x_train))
            self.data['Y_train'] = np.vstack((self.data['Y_train'], y_train))
        if self.data['X_test'] is None:
            self.data['X_test'] = x_test
            self.data['Y_test'] = y_test
        else:
            self.data['X_test'] = np.vstack((self.data['X_test'], x_test))
            self.data['Y_test'] = np.vstack((self.data['Y_test'], y_test))
        self.data['preds'].extend(predictions)
        

def load_processed(csv_dir):
    print ('Loading CSVs:')
    type_dict = {'marketOpen' : float,
                 'marketClose': float,
                 'marketAverage': float,
                 'marketHigh': float,
                 'marketLow' : float,
                 'marketVolume' : float,
                 'date' : str,
                 'minute': str,
                 'notional': str,
                 'label': str}
    lst = []
    for file in glob(csv_dir + '/*.csv'):
        print ('....' + file)
        data = pd.read_csv(file, dtype=type_dict, na_values=['None'],header=0)
        #except ValueError:
        #    print ('...... Problem CSV:' + file)
        #    continue
        lst.append(data)
    return lst

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
    
    
    
def get_pred_params(day_df,percent=True):
    close = day_df['marketClose'].interpolate(limit=100, limit_direction='both').values[-1]
    high = max(day_df['marketHigh'].interpolate(limit=100, limit_direction='both').values)
    low = min(day_df['marketLow'].interpolate(limit=100, limit_direction='both').values)
    if percent is True:
        p25 = np.percentile(day_df['marketAverage'].interpolate(limit=100, limit_direction='both').values, 25)
        p50 = np.percentile(day_df['marketAverage'].interpolate(limit=100, limit_direction='both').values, 50)
        p75 = np.percentile(day_df['marketAverage'].interpolate(limit=100, limit_direction='both').values, 75)
        return np.array([[close,low,p25,p50,p75,high]])
    else:
        return np.array([[close,high,low]])
    
def nans(x):
    if x.shape[1] == 7:
        x = x[:,:6]
        x = x.tolist()
    return np.any(np.isnan(x))

def proc(stock_df,percent):
    stock = stock_df['Ticker'][0]
    print ('....', stock)
    x_train = None
    x_test = None
    predictions = []
    dates = stock_df.date.unique()
    for i,date in enumerate(dates):
        try:
            if np.isnan(int(date)):
                print ('.......NaN Date')
                continue
        except ValueError:
            print ('.......NaN Date')
            continue
        stock_day = stock_df.loc[stock_df['date'] == date]
        stock_day = stock_day.replace(-1,np.nan)
        stock_day = stock_day.replace('None',np.nan)
        if len(stock_day['marketClose'].values) == 0:
            continue
        if i != len(dates) - 1: 
            a = stock_day['marketOpen'].interpolate(limit=100, limit_direction='both').values
            a = a.reshape((a.shape[0],1))
            b = stock_day['marketAverage'].interpolate(limit=100, limit_direction='both').values
            b = b.reshape((b.shape[0],1))
            c = stock_day['marketClose'].interpolate(limit=100, limit_direction='both').values
            c = c.reshape((c.shape[0],1))
            d = stock_day['marketLow'].interpolate(limit=100, limit_direction='both').values
            d = d.reshape((d.shape[0],1))
            e = stock_day['marketHigh'].interpolate(limit=100, limit_direction='both').values
            e = e.reshape((e.shape[0],1))
            f = stock_day['marketVolume'].interpolate(limit=100, limit_direction='both').values
            f = f.reshape((f.shape[0],1))
            day_ts = np.hstack((a,b,c,d,e,f))
            if day_ts.shape[0] != 390:
                print (".......I bet it's missing minutes. Let's just skip this day.",date)
                continue
            if nans(day_ts):
                print (".......There's still NaNs coming through here.",date)
                continue
            tommo_df = stock_df.loc[stock_df['date'] == next_date(date)]
            tommo_df = tommo_df.replace(-1,np.nan)
            if len(tommo_df['marketClose'].values) == 0:
                continue
            preds = get_pred_params(tommo_df,percent)
            if nans(preds):
                print (".......There's NaNs in the prediction.")
                continue
            if (i+1)/len(dates) < 0.9:
                if x_train is None:
                    x_train = day_ts
                    y_train = preds
                x_train = np.vstack((x_train,day_ts))
                y_train = np.vstack((y_train,preds))
            else:
                if x_test is None:
                    x_test = day_ts
                    y_test = preds
                x_test = np.vstack((x_test,day_ts))
                y_test = np.vstack((y_test,preds))
        else:
            a = stock_day['marketOpen'].interpolate(limit=100, limit_direction='both').values
            a = a.reshape((a.shape[0],1))
            b = stock_day['marketAverage'].interpolate(limit=100, limit_direction='both').values
            b = b.reshape((b.shape[0],1))
            c = stock_day['marketClose'].interpolate(limit=100, limit_direction='both').values
            c = c.reshape((c.shape[0],1))
            d = stock_day['marketLow'].interpolate(limit=100, limit_direction='both').values
            d = d.reshape((d.shape[0],1))
            e = stock_day['marketHigh'].interpolate(limit=100, limit_direction='both').values
            e = e.reshape((e.shape[0],1))
            f = stock_day['marketVolume'].interpolate(limit=100, limit_direction='both').values
            f = f.reshape((f.shape[0],1))
            g = stock_day['date'].values
            g = g.reshape((g.shape[0],1))
            pred_day = np.hstack((a,b,c,d,e,f,g))
            if pred_day.shape[0] != 390:
                print ('.......Skipping prediction, it looks like minutes are missing',date)
                continue
            if nans(pred_day):
                print (".......Skipping prediction, there's still NaNs.",date)
                continue
            predictions.append((stock,pred_day))
    return (x_train,x_test,y_train,y_test,predictions)

    
def split_data(df_list,percent=True,gofast=True):
    training = TrainingData()
    if gofast == False:
        cores = int(cpu_count()*.8)
    else:
        cores = cpu_count()
    print ("Generating training data using %s cores:"%cores)
    pool = Pool(cores)
    for df in df_list:  
        pool.apply_async(proc, args=(df,percent),callback=training.update_data)
    pool.close()
    pool.join()
    return training.data
                     
def save_train(data,name):
    output = open('Training and Test/%s.pkl'%name, 'wb')
    pickle.dump(data,output)
    output.close()
    
if __name__ == "__main__":
    freeze_support()
    df_list = load_processed('CSV')
    data = split_data(df_list,percent=True,gofast=True)
    print (data['Y_train'].shape)
    save_train(data,'full_train3_5NC')
    