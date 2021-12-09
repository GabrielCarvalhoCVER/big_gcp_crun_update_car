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

import cv2, random, string, math, os

import numpy as np

import pandas as pd

from PIL import ImageFont, ImageDraw, Image

from captcha import settings

sample_names   = []

sample_answers = []

def create_samples():
	if not os.path.isdir(settings.GENERATED_CAPTCHA_FOLDER):

		os.mkdir(settings.GENERATED_CAPTCHA_FOLDER)

	for sample in range(settings.NUM_SAMPLES):
		# Generate random string
		captcha_code = ''.join(random.choice(string.ascii_uppercase + \
											 string.ascii_lowercase + \
											 string.digits) for _ in range(5))

		# Generate empty image, 150x50 pixels with 4 channels
		img	   = np.zeros((settings.IMG_HEIGHT, settings.IMG_WIDTH,
							  settings.NUM_CHANNELS), np.uint8)

		# Create text
		text_area = np.zeros((settings.IMG_HEIGHT, settings.IMG_WIDTH), np.uint8)

		font = ImageFont.truetype(settings.FONT_FACE, settings.FONT_SIZE)

		text_area_pil = Image.fromarray(text_area)

		draw = ImageDraw.Draw(text_area_pil)

		char_x = 20 + random.randint(-4, 4)

		for c in captcha_code:

			char_y = random.randint(-4, 4)

			char_width = font.getsize(c)[0]

			draw.text((char_x, char_y), c, font = font, fill = 255)

			char_x = char_x + char_width - 5

		text_area = np.array(text_area_pil)

		# Distort text on image
		text_distorted = np.zeros(text_area.shape, dtype=text_area.dtype)

		rows, cols = text_area.shape

		amplitude = 4 + random.uniform(-0.25, 0.25)

		sig	   = random.uniform(-1, 1)

		if sig >= 0:

			sig = 1

		else:

			sig = -1

		amplitude = amplitude*sig

		freq	  = 2 + random.uniform(-1, 1)

		for i in range(rows):

			for j in range(cols):

				offset_y = int(amplitude * math.sin(freq * math.pi * j / settings.IMG_WIDTH))

				if i+offset_y < rows:

					text_distorted[i,j] = text_area[(i+offset_y) % rows, j]

				else:

					text_distorted[i,j] = 0

		# Generate red line
		red_line = np.zeros((settings.IMG_HEIGHT, settings.IMG_WIDTH), np.uint8)

		x_start	= 20  + random.randint(-5, 5)

		x_stop	 = 135 + random.randint(-5, 5)

		num_points = random.randint(28, 34)

		pts_x	  = np.linspace(x_start, x_stop, num = num_points)

		pts_x	  = [int(p + random.randint(-2, 2)) for p in pts_x]

		slope	  = random.uniform(-0.2, 0.2)

		offset	 = random.randint(-4, 4) + 25

		pts_y = [np.clip(int(p*slope+offset + random.randint(-3, 3)), 0, 50) for p in pts_x]

		pts = np.array([pts_x, pts_y], np.int32)

		pts = pts.T

		red_line = cv2.polylines(red_line, [pts], settings.POLYLINE_IS_CLOSED,
								 settings.POLYLINE_COLOR, settings.POLYLINE_THICKNESS)

		# Add red line to image
		img[:,:,2] = red_line

		# Add red line to text layer
		text_distorted = np.maximum(text_distorted, red_line)

		# Add text channel to image
		img[:,:,3] = text_distorted

		# Write image file
		cv2.imwrite(settings.GENERATED_CAPTCHA_FOLDER + \
					settings.SAMPLE_NAME_FORMAT.format(sample) + '.png', img)

		sample_names.append(settings.SAMPLE_NAME_FORMAT.format(sample))

		sample_answers.append(captcha_code)

	training_info  = [sample_names, sample_answers]

	training_info = np.transpose(training_info)

	training_info = pd.DataFrame(training_info, columns = ['sample_name', 'sample_answer'])

	training_info.to_csv(settings.TRAINING_ANSWERS_FILE, index = False)
