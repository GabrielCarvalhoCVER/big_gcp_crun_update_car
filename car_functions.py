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

import base64, time, os, re
import geopandas as gpd
import pandas	as pd
import zipfile   as zf
from difflib	 import get_close_matches
from itertools   import compress
from selenium	import webdriver
from selenium.webdriver.chrome.options import Options
from shutil	  import rmtree
# from pyvirtualdisplay import Display
# import gspread
from oauth2client.service_account import ServiceAccountCredentials
import datetime
import pytz
import settings
from captcha.breaker.captcha_breaker import captcha_breaker, preprocess_img, load_model_and_encoder
# from captcha.breaker.captcha_breaker import captcha_breaker, preprocess_img, load_model_and_encoder

def download_captcha_img(driver):

	captcha_img = driver.find_element_by_id('img-captcha-base-downloads')

	# Stolen from the interwebs. Don't ask
	img_captcha_base64 = driver.execute_async_script("""
		var ele = arguments[0], callback = arguments[1];
		ele.addEventListener('load', function fn(){
		  ele.removeEventListener('load', fn, false);
		  var cnv = document.createElement('canvas');
		  cnv.width = this.width; cnv.height = this.height;
		  cnv.getContext('2d').drawImage(this, 0, 0);
		  callback(cnv.toDataURL('image/png').substring(22));
		}, false);
		ele.dispatchEvent(new Event('load'));
		""", captcha_img)

	with open(r"captcha.png", 'wb') as f:
		f.write(base64.b64decode(img_captcha_base64))
	
def get_url_twice(driver, url):

			print('Loading Page: First time')
			driver.get(url)

			time.sleep(2)

			print('Loading Page: Second time')
			driver.get(url)

def captcha_breaker_loop(driver, model, lb, file_name):

	i = 0 
	
	file_path = os.path.join(settings.DOWNLOAD_PATH, file_name)
	file_path_downloading = file_path + '.crdownload'

	file_downloaded = os.path.exists(file_path)
	file_downloading = os.path.exists(file_path_downloading)

		
	
	print('-')
	while not(file_downloaded or file_downloading):
		
		try:
			os.remove('captcha.png')
		except OSError:
			pass
		
		print('Looking for refresh button [' + str(i+1) + ']')
		refresh_button = driver.find_element_by_id('btn-atualizar-captcha')

		print('Clicking in refresh button [' + str(i+1) + ']')
		refresh_button.click()

		time.sleep(1)

		download_captcha_img(driver)

		print('Preprocessing Image [' + str(i+1) + ']')
		captcha_processed = preprocess_img('captcha.png')

		print('Breaking Captcha [' + str(i+1) + ']')
		captcha = captcha_breaker(captcha_processed, model, lb)

		print('Finding captcha input [' + str(i+1) + ']')
		captcha_input = driver.find_element_by_id('form-captcha-download-base')

		print('Clear captcha [' + str(i+1) + ']')
		captcha_input.clear()

		print('Sending keys for captcha : try[' + str(i+1) + ']')

		captcha_input.send_keys(captcha)
		print('Captcha sended : try [' + str(i+1) + ']')

		time.sleep(1)

		download_button = driver.find_element_by_id('btn-baixar-dados')

		download_button.click()
		
		print('Download Button Clicked : try [' + str(i+1) + ']')

		print('-')
		i += 1

		time.sleep(1)
		
		file_downloading = os.path.exists(file_path_downloading)
		file_downloaded = os.path.exists(file_path)

	if i == 0 and (file_downloaded or file_downloading):
		i=9999
	print('Captcha Breaked with ' + str(i) + ' tries')
	close_button = driver.find_element_by_xpath('//*[@id="modal-download-base"]/div/div/div[1]/button')

	close_button.click()

	os.remove('captcha.png')
	
	if file_downloading and not file_downloaded:
		file_state = 'Downloading'
	else:
		file_state = 'Downloaded'
	
	return i, file_state

def wait_for_download(file_name):

	waiting_iterations = 0
	
	file_path = os.path.join(settings.DOWNLOAD_PATH, file_name)
	file_path_downloading = file_path + '.crdownload'

	file_downloading = os.path.exists(file_path_downloading)
	file_downloaded = os.path.exists(file_path)

	#while waiting_iterations < settings.MAX_WAIT_ITERATIONS and not (file_downloaded):# or file_downloading):
	#while not (file_downloaded or file_downloading):
	while not (file_downloaded):

		time.sleep(1)
		
		file_downloaded = os.path.exists(file_path)
		file_downloading = os.path.exists(file_path_downloading)
		waiting_iterations += 1

	# if any([d == file_name for d in downloaded_files]):
	#	 state_folder = os.path.join(settings.OUTPUT_FOLDER, state) 

	#	 if not os.path.isdir(settings.OUTPUT_FOLDER):

	#		 os.mkdir(settings.OUTPUT_FOLDER)
			
	#	 if not os.path.isdir(state_folder):
			
	#		 os.mkdir(state_folder)

	#	 output_file = os.path.join(settings.OUTPUT_FOLDER, state, city + '.zip')

	#	 try:

	#		 os.rename(os.path.join(settings.DOWNLOAD_PATH, file_name), output_file)

	#	 except FileExistsError:

	#		 os.remove(os.path.join(settings.DOWNLOAD_PATH, file_name))

	#		 print('File already downloaded')

def dowload_car_with_driver(city, state, driver, model, lb):
	print('Loading Page')
	get_url_twice(driver, settings.CAR_BASE_URL + state)
	print('Page Loaded')
	
	car_city_elements = driver.find_elements_by_class_name('item-municipio')
   
	if(isinstance(city, str)):

		city_names = []

		for element in car_city_elements:
			city_names.append(element.text)

		# To avoid typos
		best_match = get_close_matches(city, city_names, 1)[0]

		index = city_names.index(best_match)

	elif (isinstance(city, int)):

		city_ids = []

		for element in car_city_elements:
			city_ids.append(int(element.find_element_by_tag_name('button').get_attribute('data-municipio')))
		
		index = city_ids.index(city)

	city_element = car_city_elements[index]
	
	print('City downloading : ' + str(city_element.text) + ' [' + state +']')
	button = city_element.find_element_by_tag_name('button')

	file_number = button.get_attribute('data-municipio')
	file_name = 'SHAPE_' + file_number + '.zip'

	button.click()

	time.sleep(1)

	email_input = driver.find_element_by_id('form-email-download-base')
	email_input.clear()
	email_input.send_keys('aaa@hotmail.com')

	count = 0
	
	file_path = os.path.join(settings.DOWNLOAD_PATH, file_name)

	file_path_downloading = file_path + '.crdownload'
	file_downloading = os.path.exists(file_path_downloading)

	print('Breaking captcha')
	while count <= 0:
		try:
			count, file_state = captcha_breaker_loop(driver, model, lb, file_name)

		except Exception as err:
			print('Error :' + str(err))
			count = -1

	return file_state 

	# print('Wait for Download')
	#wait_for_download(file_name)

def download_car(df=None, city=None, state=None):
	start_time = datetime.datetime.now((pytz.timezone('America/Sao_Paulo')))
	print('Start Time : '+str(start_time))
	
	print('Checking folder existence')

	if not(os.path.exists(settings.DOWNLOAD_PATH)): 
		os.makedirs(settings.DOWNLOAD_PATH)
		print('Folder created')
	else: 
		print('Folder already exists')

	print(f'Initialization of the driver {settings.CHROMEDRIVER_FILE}')
	chrome_options = Options()
	chrome_options.add_argument('--no-sandbox')
	chrome_options.add_argument("--headless")
	chrome_options.add_argument('--disable-dev-shm-usage')
	prefs = {}
	prefs["profile.default_content_settings.popups"]=0
	prefs["download.default_directory"]=settings.DOWNLOAD_PATH
	chrome_options.add_experimental_option("prefs", prefs)

	driver = webdriver.Chrome(settings.CHROMEDRIVER_FILE, options = chrome_options)
	print('Driver Initializated')

	print('Loading model and encoder')
	model, lb = load_model_and_encoder()
	print('Model and encoder loaded')

	count = 0

	if df is not None:
		# df = args[0]
		n_row = len(df)
		df['Download State'] =''
		if (settings.CITYID_COLUMN in df.columns):
			print('Selected Model via cityid_column')

			try: 
				city_list = df.loc[:, [settings.CITYID_COLUMN, settings.CITY_COLUMN, settings.STATE_COLUMN]].copy()
			except: 
				city_list = df.loc[:, [settings.CITYID_COLUMN, settings.STATE_COLUMN]].copy()

			city_list.sort_values(settings.STATE_COLUMN, axis=0, ascending=True, inplace=True)

			states = set(city_list[settings.STATE_COLUMN])
			df_file_state = pd.DataFrame()

			for state in states:

				print('')
				print('######################################')
				print('State selected :' + str(state))
				#The request must be performed twice, since the first one is redirected
				# get_url_twice(driver, settings.CAR_BASE_URL + state)

				current_cities = city_list[city_list[settings.STATE_COLUMN] == state]

				for idx_row , row  in current_cities.iterrows():

					count = count+1
					print('')
					print('---------------------------')
					print(str(count) +' of ' + str(n_row))

					city = row[settings.CITYID_COLUMN]
					try:
						print('City selected : ' + str(row[settings.CITY_COLUMN]) + ' [' + str(state) + '] ('+ str(city)+')')
					except:
						print('City selected : ' + str(city) + ' [' + str(state) + ']' )

					start_city_time = datetime.datetime.now((pytz.timezone('America/Sao_Paulo')))
					print('-Start City Time : '+str(start_city_time)+ '-')

					file_path = os.path.join(settings.DOWNLOAD_PATH, 'SHAPE_' + str(city) + '.zip')
					file_downloaded = os.path.exists(file_path)
					file_path_downloading = file_path + '.crdownload'
					file_downloading = os.path.exists(file_path_downloading)

					print('Checking file existence : ' + str(file_path))
					
					
					if file_downloaded:
						print('-File ALREADY EXISTS-')
					else:
						if file_downloading:
							print('-REMOVE File Downloading from Previous Try-')
							os.remove(file_path_downloading)
						try:
							print("-File DOES NOT EXISTS-")
							file_state = dowload_car_with_driver(int(city), state, driver, model, lb)
							df_file_state = df_file_state.append(pd.DataFrame({
																		settings.CITYID_COLUMN:[city], 
																		'file_path_downloading':[file_path_downloading], 
																		'file_state':[file_state]
																		}))#, columns = {settings.CITYID_COLUMN, 'File_Path_Downloding', 'File_State'}) )					 
							#df_teste = df_teste.append(pd.DataFrame( {'COD_MUNICIP' : [row['COD_MUNICIP']], 'File_State':['Teste']}))			  
							print(file_state)
							time.sleep(1)
						except Exception as err:
							print('-File DOWNLOAD ERROR : ' + str(err) + '-')
							time.sleep(1)

							
						
					end_city_time = datetime.datetime.now((pytz.timezone('America/Sao_Paulo')))
					print('-End City Time : ' + str(end_city_time) + '-')
					print('-Elapsed Time : ' + str(end_city_time - start_city_time)+ '-')
					print('-Elapsed Total Time : ' + str(end_city_time - start_time)+ '-')
			print('')
			print('Waiting for downloads to finish')
			for idx, row in df_file_state.iterrows():
				while (os.path.exists(row['file_path_downloading'])):
					print('Waiting for ' + str(row['file_path_downloading']))
					time.sleep(5)
					   
			end_time = datetime.datetime.now((pytz.timezone('America/Sao_Paulo')))
			print('')
			print('-End Time : '+str(end_time) + '-')
			print('-Elapsed Total Time : '+str(end_time - start_time)+ '-')
			# return df
					
		elif (settings.CITY_COLUMN in df.columns):
			print('city_column  :' + settings.CITY_COLUMN)
			city_list = df.loc[:, [settings.CITY_COLUMN, settings.STATE_COLUMN]]

			city_list.sort_values(settings.STATE_COLUMN, axis=0, ascending=True, inplace=True)

			states = set(city_list[settings.STATE_COLUMN])

			for state in states:

				#The request must be performed twice, since the first one is redirected
				get_url_twice(driver, settings.CAR_BASE_URL + state)

				current_cities = city_list[city_list[settings.STATE_COLUMN] == state]

				car_city_elements = driver.find_elements_by_class_name('item-municipio')

				city_names = []

				for element in car_city_elements:

					city_names.append(element.text.lower())

				for _, row in current_cities.iterrows():

					count = 0
					city = row[settings.CITY_COLUMN].lower()

					dowload_car_with_driver(city, state, driver, model, lb)

					time.sleep(1)

	elif (city is not None) and (state is not None):

		dowload_car_with_driver(city, state, driver, model, lb)
		
	else:

		driver.close()

		raise TypeError('You must input a a city and a state, or a shapefile')

	driver.close()

def filter_by_state(shp, state):

	return shp[shp[settings.STATE_COLUMN] == state]

def unzip_car_files():

	states_list  = os.listdir(settings.OUTPUT_FOLDER)

	regex = re.compile('^[A-Z]{2}$')

	states_index = [bool(re.search(regex, i)) for i in states_list]

	states_list  = list(compress(states_list, states_index))

	data_of_interest = settings.CAR_LAYERS

	for state in states_list:

		current_state = os.path.join(settings.OUTPUT_FOLDER, state)

		file_list	 = os.listdir(current_state)

		zip_folders   = ['zip' in i for i in file_list]

		file_list	 = list(compress(file_list, zip_folders))

		for i in range(len(file_list)):

			current_zip   = os.path.join(current_state, file_list[i])

			shape_name	= file_list[i][:-4]

			data_location = os.path.join(current_state, shape_name)

			with zf.ZipFile(current_zip, 'r') as zipped:

				zipped.extractall(data_location)

			for data in data_of_interest:

				current_data = os.path.join(data_location, data)

				current_file = current_data + '.zip'
				current_file = zf.ZipFile(current_file, 'r')

				output_dir = os.path.join(current_state, data)

				if not os.path.isdir(output_dir):

					os.mkdir(output_dir)

				current_file.extractall(output_dir)

				for exported in current_file.namelist():

					extension = exported.split('.')[-1]

					os.rename(os.path.join(output_dir, exported),\
							  os.path.join(output_dir, shape_name + '.' + extension))

				current_file.close()

			rmtree(data_location, ignore_errors=True)

def merge_shapes(*args):

	data_of_interest = settings.CAR_LAYERS

	if len(args) == 1 and isinstance(args[0], str):

		state = args[0]

		state_folder = os.path.join(settings.OUTPUT_FOLDER, state)

		for data in data_of_interest:

			current_folder = os.path.join(state_folder, data)

			files = os.listdir(current_folder)

			shapefiles = [x for x in files if x[-4:] == '.shp']

			shapefiles = [os.path.join(current_folder, x) for x in shapefiles]

			if len(shapefiles) > 0:

				state_shapefile = pd.concat([gpd.read_file(s) for s in shapefiles])

				state_shapefile = gpd.GeoDataFrame(state_shapefile)

				state_shapefile.to_file(os.path.join(current_folder, state + '.shp'))

	elif len(args) == 0:

		states_list  = os.listdir(settings.OUTPUT_FOLDER)

		regex = re.compile('^[A-Z]{2}$')

		states_index = [bool(re.search(regex, i)) for i in states_list]

		states_list  = list(compress(states_list, states_index))

		for state in states_list:

			state_folder = os.path.join(settings.OUTPUT_FOLDER, state)

			for data in data_of_interest:

				current_folder = os.path.join(state_folder, data)

				files = os.listdir(current_folder)

				shapefiles = [x for x in files if x[-4:] == '.shp']

				shapefiles = [os.path.join(current_folder, x) for x in shapefiles]

				if len(shapefiles) > 0:

					state_shapefile = pd.concat([gpd.read_file(s) for s in shapefiles])

					state_shapefile = gpd.GeoDataFrame(state_shapefile)

					state_shapefile.to_file(os.path.join(current_folder, state + '.shp'))

	else:

		raise Exception('Specify a state (two letters) or call the function without arguments ' + \
						'to run it for all of the states')

def test_model():
	import time
	
	start_time = time.time()
	
	cidades = pd.read_csv('cidades_ce.csv', sep=';')
	
	for index, row in cidades.iterrows():

		start_row = time.time()
			
		tentativas = download_car(row['cidade'], row['UF'])

		cidades.loc[index, 'tentativas'] = tentativas
		
		end_row = time.time()
		
		cidades.loc[index, 'tempo'] = end_row - start_row
		
		print('{} - {} tentivas - {} tempo'.format(row['cidade'], tentativas, end_row - start_row))
		
		
	cidades.to_csv('result.csv', sep = ';')
	
	end_time = time.time()
	
	print('Total time - {}'.format(end_time - start_time))
