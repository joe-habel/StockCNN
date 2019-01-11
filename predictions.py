from keras.models import load_model
from CNNreg import r_square, rmse, maape
from multiprocessing import freeze_support
import pickle


def get_model(model_file):
    return load_model('Models/%s.h5'%model_file, custom_objects={'r_square': r_square,
                                                                 'rmse' : rmse,
                                                                 'maape': maape})

def load_train(name):
    pkl = open('Training and Test/%s.pkl'%name, 'rb')
    data = pickle.load(pkl)
    pkl.close()
    return data['preds'], data['Y_train'].shape[1]
    

def full_preds(preds,model,out_size):
    tommo = []
    print (len(preds))
    for stock, data in preds:
        p = model.predict(data.reshape((1,data.shape[0],data.shape[1])))
        if out_size == 6:
            if (p[0][1] < p[0][2] < p[0][3] < p[0][4] < p[0][5]) and (p[0][0] <= p[0][5]): 
                tommo.append((stock,p))
        elif out_size == 3:
            if (max(p[0]) == p[0][1]) and (min(p[0]) == p[0][2]) and (p[0][0] <= p[0][1]):
                tommo.append((stock,p))
    print (len(tommo))
    return tommo

def fast_preds(preds,stocks,model):
    fast = []
    for stock, data in preds:
        if stock in stocks:
            p = model.predict(data.reshape((1,data.shape[0],data.shape[1])))
            fast.append((stock,p))
    return fast

def save_preds(preds,date):
    with open('Predictions/%s.csv'%date, 'w') as pred_file:
        pred_file.write('Ticker,Close,Low,Q1,Med,Q3,High\n')
        for stock,pred in preds:
            pred_file.write('%s,%s/n'%(stock,pred[0]))

def make_predictions(model_file,verification_date,training_file,fast=None):
    model = get_model(model_file)
    preds, out_size = load_train(training_file)
    verified_preds = [i for i in preds if i[1][0][6] == verification_date]
    preds = [(stock,i[:,:6]) for (stock,i) in verified_preds]
    if fast is None:
        return full_preds(preds,model,out_size)
    else:
        return fast_preds(preds,fast,model)
    

if __name__ == "__main__":
    freeze_support()
    preds = make_predictions('take_two_5NC','20180924','full_train3_5NC')
    save_preds(preds,'20180924')