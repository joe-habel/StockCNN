import matplotlib.pyplot as plt
import pickle

from keras.layers import Convolution1D, Dense, MaxPooling1D, Flatten
from keras.models import Sequential
from keras.optimizers import Adam
from keras import backend as K
from theano.tensor import arctan

def rmse(y_true, y_pred):
    return K.sqrt(K.mean(K.square(y_pred - y_true), axis=-1))

def mpe(y_true, y_pred):
    return K.mean(K.abs(y_pred-y_true)/(y_true+0.001))

def maape(y_true, y_pred):
    return K.mean(arctan(K.abs((y_pred-y_true)/y_true)))

def r_square(y_true, y_pred):
    SS_res =  K.sum(K.square(y_true - y_pred)) 
    SS_tot = K.sum(K.square(y_true - K.mean(y_true))) 
    return ( 1 - SS_res/(SS_tot + K.epsilon()) )



def tsRegCNN(kernel_size,filters,input_dim,output_dim):
    model = Sequential()
    #We might want to consider a leaky relu if we're losing gradients
    model.add(Convolution1D(filters=filters,kernel_size=kernel_size,activation='relu',input_shape=(390,input_dim),padding='same'))
    model.add(MaxPooling1D())
    model.add(Convolution1D(filters=64,kernel_size=kernel_size,activation='relu',padding='same'))
    model.add(MaxPooling1D())
    model.add(Convolution1D(filters=32,kernel_size=5,activation='relu',padding='same'))
    model.add(Convolution1D(filters=32,kernel_size=5,activation='relu',dilation_rate=2,padding='same'))
    model.add(MaxPooling1D())
    model.add(Convolution1D(filters=16,kernel_size=5,activation='relu',padding='same'))
    model.add(Convolution1D(filters=16,kernel_size=3,activation='relu',dilation_rate=1,padding='same'))
    model.add(MaxPooling1D())
    model.add(Convolution1D(filters=16,kernel_size=7,activation='relu',dilation_rate=3,padding='same'))
    model.add(Convolution1D(filters=16,kernel_size=3,activation='relu',padding='same'))
    model.add(Flatten())
    #Linear should be the choice we use here for regression
    model.add(Dense(14,activation='linear'))
    model.add(Dense(output_dim,activation='linear'))
    
    adam = Adam(clipnorm=1.0)
    model.compile(optimizer=adam, loss='mse', metrics=[r_square, rmse, maape])
    return model


def load_train(name):
    pkl = open('Training and Test/%s.pkl'%name, 'rb')
    data = pickle.load(pkl)
    pkl.close()
    return data
                   
def evaluate_model(data,batch_size,epochs,save=None):
    data = load_train(data) 
    model = tsRegCNN(128,3,data['X_train'].shape[2],data['Y_train'].shape[1])
    model.summary()
    history = model.fit(data['X_train'],data['Y_train'],batch_size,epochs,validation_data=(data['X_test'],data['Y_test']))
    plt.plot(history.history['maape'], label='train')
    plt.plot(history.history['val_maape'], label='test')
    plt.xlabel('Epcoh')
    plt.ylabel('MAAPE')
    plt.legend()
    plt.show()
    if save is not None:
        save_model(model,str(save))

def save_model(model,name):
    model.save('Models/' + name + '.h5')

def make_prediction(data,model):
    return model.predict(data)

if __name__ == "__main__":
    evaluate_model('full_train3_5NC',100,750,'take_two_5NC')
        