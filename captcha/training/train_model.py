#                                   `.,,,,,.`             
#                              ,::::::::::::::::::::,.`   
#                           `::::::::::::::::,.`   `.,::::
#                          :::::::::::,`   .'''''''''''`  
#                        .:::::::::.   ,+++''''''''''     
#                       :::::::::   :++++++''''''''`      
#                     `::::::::`  '++++++++'''''''        
#                    ::::::::,  ;++++++++++''''',         
#                  .::::::::` .++++++++++++''''`          
#                `:::::::::  +++++++++++++++''            
#              .,::::::::` '++++++++++++++++:             
#           `,,,:::::::. :++++++++++++++++,               
#        .,,,,,,::::.  ;#++++++++';;;:.                   
#   `,,...```                                             
#   _____                      _            __      __        _            
#  / ____|                    | |           \ \    / /       | |           
# | |     __ _ ___  __ _    __| | ___  ___   \ \  / /__ _ __ | |_ ___  ___ 
# | |    / _` / __|/ _` |  / _` |/ _ \/ __|   \ \/ / _ \ '_ \| __/ _ \/ __|
# | |___| (_| \__ \ (_| | | (_| | (_) \__ \    \  /  __/ | | | || (_) \__ \
#  \_____\__,_|___/\__,_|  \__,_|\___/|___/     \/ \___|_| |_|\__\___/|___/
#

import cv2, string, os, re

import numpy as np

import pandas as pd

from sklearn.preprocessing import LabelBinarizer

from sklearn.model_selection import train_test_split

from itertools import compress

#keras
from keras.models import Sequential

from keras.layers.convolutional import Conv2D, MaxPooling2D

from keras.layers.core import Flatten, Dense

from joblib import dump

from car.captcha import settings

def get_letters(img, letter_image_regions):
    regions_num = len(letter_image_regions)
    
    letters = []
    
    for i in range(regions_num):
        letter_region = letter_image_regions[i]
        
        count = letter_region['count']
        
        aux = (letter_region['w'] // (count))
        
        for i in range(count):
        
            letters.append(img[:, (letter_region['x'] + i * aux):(letter_region['x'] + (i + 1) * aux)])
            
    for i in range(5):
        letter = letters[i]
        
        letter = letter[:, 0:40]
        
        square = np.zeros((50, 40))
        
        y, x = letter.shape 
        
        aux = (40 - x) // 2
        
        square[:, aux:(aux + x)] = letter
        
        letters[i] = square
        
    return letters
    
def set_number_letters(letter_image_regions):
    
    letter_image_regions.sort(key = lambda x: x['area'])
    
    regions_num = len(letter_image_regions)
    
    count = 0
    
    for i in range(regions_num):
        
        area = letter_image_regions[i]['area'] 
        
        if i != regions_num - 1:
            
            if area < 1550:
            
                aux = 1
            
            elif area < 3200:
            
                aux = 2
            
            elif area < 4200:
            
                aux = 3
            
            else:
            
                aux = 4
                
            count += aux
            
            letter_image_regions[i]['count'] = aux
        
        else:
        
            letter_image_regions[i]['count'] = 5 - count
    
    letter_image_regions.sort(key = lambda x: x['x'])
    
    return letter_image_regions
    
def break_letters(img):
    
    ret, thresh = cv2.threshold(img, 200, 255, cv2.THRESH_BINARY)
    
    contours, _ = cv2.findContours(thresh, cv2.RETR_EXTERNAL, cv2.CHAIN_APPROX_SIMPLE)
    
    letter_image_regions = []

    for contour in contours:

        x, y, w, h = cv2.boundingRect(contour)
        
        if int(w * h) > 500:
        
            letter_image_regions.append({'x': x, 'y': y, 'w': w, 'h': h, 'area': w * h, 'count': 0})
    
    letter_image_regions = set_number_letters(letter_image_regions)
    
    letters = get_letters(img, letter_image_regions)
    
    return letters

def train_model():

    files = sorted(os.listdir(settings.PREPROCESSED_CAPTCHA_FOLDER))

    captchas = [bool(re.match(settings.PREPROCESSED_CAPTCHA_PATTERN, i)) for i in files]

    captchas = list(compress(files, captchas))

    test_info = pd.read_csv(settings.TRAINING_ANSWERS_FILE)

    test_info.sort_values(by = 'sample_name', ascending=True, inplace = True)

    answers   = test_info['sample_answer']

    X = []

    y = []

    for i in range(len(captchas)):
        
        captcha = captchas[i]

        captcha = cv2.imread(settings.PREPROCESSED_CAPTCHA_FOLDER + captcha, cv2.IMREAD_GRAYSCALE)
        
        try:
            letters = break_letters(captcha)
        except:
            continue
                
        j = 0
        
        for letter in letters:

            letter =  np.array(letter, dtype="float") / 255.0

            letter  = letter.reshape(50, 40, 1)

            letter = letter.T

            X.append(letter)

            y.append(answers[i][j])
            
            j += 1

    X = np.array(X)

    y = np.array(y)

    (X_train, X_test, Y_train, Y_test) = train_test_split(X, y, test_size=0.2, random_state=0)

    chars = list(string.ascii_uppercase + string.ascii_lowercase + string.digits)

    chars = np.array(chars)

    lb = LabelBinarizer()

    lb.fit(chars)

    Y_train = lb.transform(Y_train)

    Y_test = lb.transform(Y_test)

    model = Sequential()

    model.add(Conv2D(20, (5, 5), padding="same", input_shape=(1, 40, 50), activation="relu", data_format='channels_first'))
    model.add(MaxPooling2D(pool_size=(2, 2), strides=(2, 2)))

    model.add(Conv2D(50, (5, 5), padding="same", activation="relu", data_format='channels_last'))
    model.add(MaxPooling2D(pool_size=(2, 2), strides=(2, 2)))

    model.add(Flatten())
    model.add(Dense(500, activation="relu"))

    model.add(Dense(62, activation="softmax"))

    model.compile(loss="categorical_crossentropy", optimizer="adam", metrics=["accuracy"])

    model.fit(X_train, Y_train, validation_data=(X_test, Y_test), batch_size=settings.BATCH_SIZE,
              epochs=settings.EPOCHS, verbose=settings.VERBOSE)

    if not os.path.isdir(settings.OUTPUT_MODEL_FOLDER):

        os.mkdir(settings.OUTPUT_MODEL_FOLDER)

    model.save(settings.CLASSIFIER_OUTPUT_FILE)

    dump(lb, settings.ENCODER_OUTPUT_FILE)
