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

import cv2, os, re

from itertools import compress

import numpy as np

from car.captcha import settings

# Removes red line from images

def preprocess_samples():

    if not os.path.isdir(settings.PREPROCESSED_CAPTCHA_FOLDER):

        os.mkdir(settings.PREPROCESSED_CAPTCHA_FOLDER)

    files = sorted(os.listdir(settings.GENERATED_CAPTCHA_FOLDER))

    captchas = [bool(re.match(settings.GENERATED_CAPTCHA_PATTERN, i)) for i in files]

    captchas = list(compress(files, captchas))

    for i in range(len(captchas)):

        captcha = captchas[i]

        captcha = cv2.imread(settings.GENERATED_CAPTCHA_FOLDER + captcha, cv2.IMREAD_UNCHANGED)

        kernel = np.ones((4, 4),np.uint8)
        erosion = cv2.erode(captcha, kernel, iterations = 1)
        captcha = cv2.dilate(erosion, kernel, iterations = 1)

        text = captcha[:,:,3]

        line = captcha[:,:,2]

        line2 = line > settings.GRAYSCALE_THRESHOLD

        line3 = [[np.uint8(y)*np.uint8(settings.NP_UINT8_MAX) for y in x] for x in line2]

        line3 = np.reshape(line3, (settings.IMG_HEIGHT, settings.IMG_WIDTH))

        text2 = text > settings.GRAYSCALE_THRESHOLD

        text3 = [[np.uint8(y)*np.uint8(settings.NP_UINT8_MAX) for y in x] for x in text2]

        text3 = np.reshape(text3, (settings.IMG_HEIGHT, settings.IMG_WIDTH))

        text_wo_line = text3 - line3

        text_wo_line = [[np.uint8(y > settings.GRAYSCALE_THRESHOLD)*np.uint8(settings.NP_UINT8_MAX) for y in x] for x in text_wo_line]

        text_wo_line = np.reshape(text_wo_line, (settings.IMG_HEIGHT, settings.IMG_WIDTH))

        # get the bounding rect
        if cv2.__version__[0] == '3':

            _, contours, _ = cv2.findContours(text_wo_line, cv2.RETR_TREE,cv2.CHAIN_APPROX_SIMPLE)

        elif cv2.__version__[0] == '4':

            contours, _ = cv2.findContours(text_wo_line, cv2.RETR_TREE,cv2.CHAIN_APPROX_SIMPLE)

        else:

            pass

        x1 = settings.IMG_WIDTH / 2

        x2 = settings.IMG_WIDTH / 2

        y1 = settings.IMG_HEIGHT / 2

        y2 = settings.IMG_HEIGHT / 2

        for c in contours:

            x, y, w, h = cv2.boundingRect(c)

            if x < x1:

                x1 = x

            if x2 < x + w:

                x2 = x + w

            if y < y1:

                y1 = y

            if y2 < y + h:

                y2 = y + h

        y1 = int(y1)
        y2 = int(y2)
        x1 = int(x1)
        x2 = int(x2)

        sample_cropped = text_wo_line[y1:y2, x1:x2]

        sample_cropped = cv2.resize(sample_cropped, (settings.IMG_WIDTH, settings.IMG_HEIGHT))

        sample_index = int(re.findall('\d+', captchas[i])[0])

        cv2.imwrite(settings.PREPROCESSED_CAPTCHA_FOLDER + \
                    settings.PREPROCESSED_NAME_FORMAT.format(sample_index) + '.bmp', sample_cropped)
