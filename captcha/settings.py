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

import os
import platform

# CAPTCHA breaker settings
if platform.system() == 'Linux':
	CAPTCHA_MODEL_FOLDER = os.path.realpath(os.path.join(__file__, os.pardir, os.pardir, 'captcha_model'))
	# TRAINED_MODEL_FOLDER = os.path.abspath('/mnt/g/Drives compartilhados/Data Science/5. DADOS/CAPTCHA/modelo treinado/')
	# FONT_PATH = os.path.abspath('/mnt/g/Drives compartilhados/Data Science/5. DADOS/CAPTCHA/modelo treinado/')
	TRAINED_MODEL_FOLDER = os.path.join(CAPTCHA_MODEL_FOLDER, 'modelo treinado')
	FONT_PATH = os.path.join(CAPTCHA_MODEL_FOLDER, 'fonts')
# elif platform.system() == 'Windows':

	# TRAINED_MODEL_FOLDER = 'G:\\Drives compartilhados\\Data Science\\5. DADOS\\CAPTCHA\\modelo treinado/'

else:

	raise Exception('Platform not supported')

CLASSIFIER_MODEL_FILE	= os.path.join(TRAINED_MODEL_FOLDER, 'model2.h5')
LABEL_ENCODER_FILE		= os.path.join(TRAINED_MODEL_FOLDER, 'lb2.joblib')

# CAPTCHA training settings

# Training set size
NUM_SAMPLES		= 100000

# Image parameters
IMG_HEIGHT		= 50
IMG_WIDTH		= 150
NUM_CHANNELS	= 4

FONT_FACE			= os.path.join(FONT_PATH, 'verdanab.ttf')
FONT_SIZE			= 35
GRAYSCALE_THRESHOLD	= 10
NP_UINT8_MAX		= 255
POLYLINE_IS_CLOSED	= 0
POLYLINE_COLOR		= 255
POLYLINE_THICKNESS	= 2

# File names
# GENERATED_CAPTCHA_FOLDER	 = './samples/'
GENERATED_CAPTCHA_FOLDER		= os.path.join(CAPTCHA_MODEL_FOLDER, 'from_sicar')
PREPROCESSED_CAPTCHA_FOLDER  	= GENERATED_CAPTCHA_FOLDER
# PREPROCESSED_CAPTCHA_FOLDER = os.path.join(CAPTCHA_MODEL_FOLDER, 'from_sicar')
OUTPUT_MODEL_FOLDER				= TRAINED_MODEL_FOLDER
CLASSIFIER_OUTPUT_FILE			= os.path.join(OUTPUT_MODEL_FOLDER, 'model3.h5')
ENCODER_OUTPUT_FILE				= os.path.join(OUTPUT_MODEL_FOLDER, 'lb3.joblib')
TRAINING_ANSWERS_FILE			= os.path.join(GENERATED_CAPTCHA_FOLDER, 'answers.csv')

# Image naming settings
SAMPLE_NAME_FORMAT				= 'sample_{:06d}'
PREPROCESSED_NAME_FORMAT		= 'processed_sample_{:06d}'
GENERATED_CAPTCHA_PATTERN		= 'captcha_+([0-9]?)+\\.png'
PREPROCESSED_CAPTCHA_PATTERN	= 'captcha_+([0-9]?)+\\.bmp'

# Convolutional NN parameters
BATCH_SIZE	= 32
# BATCH_SIZE	= 8
EPOCHS		= 20
VERBOSE		= 1
