#								   `.,,,,,.`			 
#							  ,::::::::::::::::::::,.`   
#						   `::::::::::::::::,.`   `.,::::
#						  :::::::::::,`   .'''''''''''`  
#						.:::::::::.   ,+++''''''''''	 
#					   :::::::::   :++++++''''''''`	  
#					 `::::::::`  '++++++++'''''''		
#					::::::::,  ;++++++++++''''',		 
#				  .::::::::` .++++++++++++''''`		  
#				`:::::::::  +++++++++++++++''			
#			  .,::::::::` '++++++++++++++++:			 
#		   `,,,:::::::. :++++++++++++++++,			   
#		.,,,,,,::::.  ;#++++++++';;;:.				   
#   `,,...```											 
#   _____					  _			__	  __		_			
#  / ____|					| |		   \ \	/ /	   | |		   
# | |	 __ _ ___  __ _	__| | ___  ___   \ \  / /__ _ __ | |_ ___  ___ 
# | |	/ _` / __|/ _` |  / _` |/ _ \/ __|   \ \/ / _ \ '_ \| __/ _ \/ __|
# | |___| (_| \__ \ (_| | | (_| | (_) \__ \	\  /  __/ | | | || (_) \__ \
#  \_____\__,_|___/\__,_|  \__,_|\___/|___/	 \/ \___|_| |_|\__\___/|___/
#

from joblib	   import load
from keras.models import load_model
import numpy	  as np
import cv2
# from  car.captcha.settings as settings
from  .. import settings
# from  ..settings import  settings

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

def load_model_and_encoder():
	#  print(settings.CLASSIFIER_MODEL_FILE)

	model = load_model(settings.CLASSIFIER_MODEL_FILE)
	lb = load(settings.LABEL_ENCODER_FILE)

	return model, lb

def preprocess_img(img_input):

	captcha = cv2.imread(img_input, cv2.IMREAD_UNCHANGED)

	# preprocess image

	kernel = np.ones((4, 4),np.uint8)
	erosion = cv2.erode(captcha, kernel, iterations = 1)
	captcha = cv2.dilate(erosion, kernel, iterations = 1)
	
	text = captcha[:,:,3]

	line = captcha[:,:,2]

	line2 = line > 120

	line3 = [[np.uint8(y)*np.uint8(255) for y in x] for x in line2]

	line3 = np.reshape(line3, (50, 150))

	text2 = text > 120

	text3 = [[np.uint8(y)*np.uint8(255) for y in x] for x in text2]

	text3 = np.reshape(text3, (50, 150))

	text_wo_line = text3 - line3

	text_wo_line = [[np.uint8(y > 120)*np.uint8(255) for y in x] for x in text_wo_line]

	text_wo_line = np.reshape(text_wo_line, (50, 150))

	# get the bounding rect
	contours, _ = cv2.findContours(text_wo_line, cv2.RETR_TREE,cv2.CHAIN_APPROX_SIMPLE)

	x1 = 75

	x2 = 75

	y1 = 25

	y2 = 25

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

	sample_cropped = cv2.resize(sample_cropped, (150, 50))

	return sample_cropped

def captcha_breaker(img, model=None, lb=None):

	letters = break_letters(img)

	if model is None or lb is None:

		model, lb = load_model_and_encoder()

	letters_pred = []

	for idx_l, letter in enumerate(letters,1):

		letter =  np.array(letter, dtype="float") / 255.0

		letter = letter.reshape(50, 40, 1)

		letter = letter.T

		letters_pred.append(letter)

	letters_pred = np.array(letters_pred)

	print(f'captcha_breaker {3}')
	pred = model.predict(letters_pred)

	print(f'captcha_breaker {4}')
	pred = ''.join(list(lb.inverse_transform(pred)))

	return pred