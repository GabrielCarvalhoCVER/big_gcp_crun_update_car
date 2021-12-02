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
from datetime import datetime

# CAR parameters
DATA_FOLDER		   = './output_eligible_cities/'

if platform.system() == 'Linux':

	CHROMEDRIVER_FOLDER   = os.path.realpath(os.path.join(__file__,os.pardir,'chrome'))
	DOWNLOAD_PATH = os.path.abspath('/tmp/SHAPES CAR '+ datetime.today().strftime('%Y-%m-%d'))

# elif platform.system() == 'Windows':

	# CHROMEDRIVER_FOLDER   = 'G:\\Drives compartilhados\\Data Science\\5. DADOS\\SELENIUM WEBDRIVER\\CHROME/'
	# DOWNLOAD_PATH		 = 'G:\Drives compartilhados\\[BIG] Upload\\02. CAR\\SHAPES CAR ' + datetime.today().strftime('%Y-%m-%d')   #os.path.join(os.path.expanduser('~'), 'Downloads')

else:

	raise Exception('Platform not supported')
	
CAR_LAYERS = ['APP', 'AREA_IMOVEL', 'BANHADO', 'HIDROGRAFIA', 'MANGUEZAL',\
	'RESERVA_LEGAL', 'RESTINGA', 'SERVIDAO_ADMINISTRATIVA', 'USO_RESTRITO',\
	'VEREDA']

OUTPUT_FOLDER	= './output_car/'
CITY_COLUMN		= 'NOM_MUNICIP'
CITYID_COLUMN	= 'COD_MUNICIP'
STATE_COLUMN	= 'UF'
CAR_BASE_URL	= 'http://www.car.gov.br/publico/municipios/downloads?sigla='
# DOWNLOAD_PATH		 = '/mnt/c/Users/lucio.paiva/Downloads/' #os.path.join(os.path.expanduser('~'), 'Downloads')
MAX_WAIT_ITERATIONS   = 1000
CAR_GSHEETID	= '1Zjn875yj5srNH27c0Edy7-YwtyiT6nc7NeCr7skz9ww'
CAR_SHEETNAME	= 'Lista Geral'


# File names
PROSPECTED_CITIES_SHP = os.path.join(DATA_FOLDER, 'PROSPECTED_CTY.shp')
if platform.system() == 'Windows':

	CHROMEDRIVER_FILE   = os.path.join(CHROMEDRIVER_FOLDER, 'chromedriver.exe')

elif platform.system() == 'Linux':

	CHROMEDRIVER_FILE   = os.path.join(CHROMEDRIVER_FOLDER, 'chromedriver')
	 
 