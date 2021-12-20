import os
# from sqlalchemy import create_engine
from flask import Flask
import pandas as pd 
from access_db.postgresql import *
from gcp_functions.gcp_functions import *
from gcp_clients.gcp_client_gdrive import *
from gcp_clients.gcp_client_gsheet import *
from gcp_clients.gcp_client_iamcredentials import *
from geoserver_api.geoserver_api import *
import settings
from zipfile import ZipFile
import glob
from io import BytesIO
import shapefile
from car_functions import download_car

app = Flask(__name__)

@app.route('/', methods=['POST'])

def receive_request(request=None)->dict:
	
	try:

		if (request is not None):
			req_json = request_to_json(request)

			function_called = req_json.get('function','update')
			update = req_json.get('update',True)
			crud = req_json.get('crud',True)
			force_print = req_json.get('force_print',False)

			if function_called == 'get_cities':
				cities_list = req_json.get('cities_list',None)
				
				df_cities = get_df_cities_from_gsheet()
				if cities_list is not None:
					df_cities = filter_df_cities(df_cities=df_cities, cities_list=cities_list)

				df_cities = filter_df_cities(df_cities=df_cities, cities_list=cities_list)
				cities_list = df_cities[settings.CITYID_COLUMN].tolist()

				to_return = {
					"cities_list":cities_list
				}

				to_return = json.dumps(to_return)

			if function_called == 'update':

				cities_list = req_json.get('cities_list',None)
				
				df_cities = get_df_cities_from_gsheet()
				if cities_list is not None:
					df_cities = filter_df_cities(df_cities=df_cities, cities_list=cities_list)

				df_cities_already_runned = pd.DataFrame.from_dict(req_json.get('cities_already_runned',{}), orient='index')

				print_cities_already_runned = req_json.get('print_cities_already_runned', True)
		
				if len(df_cities[settings.CITYID_COLUMN]) >=1:

					personified_service_account = req_json.get('service_account')
					if personified_service_account is not None:
						creds = IAMCredentials_Client().get_service_account_creds(saccount_email=personified_service_account , saccount_scopes=['https://www.googleapis.com/auth/iam'])
					else:
						creds = None

					saccount_scopes =  ['https://www.googleapis.com/auth/drive' ]
					saccount_email = 'gabriel-functions-caller@gisbicnet-3.iam.gserviceaccount.com'
					subject = 'big@casadosventos.com.br'
					
					creds_gdrive = IAMCredentials_Client(credentials=creds).get_service_account_creds(saccount_email=saccount_email , saccount_scopes=saccount_scopes, subject=subject)

					gdrive_client = GDrive_Client(credentials=creds_gdrive)

					engrawconn_db, engrawconn_db_none = get_eng_and_rawconn()

					df_cities_runned = run_get_cars(df_cities[:10], gdrive_client=gdrive_client, engrawconn_db=engrawconn_db, update=update ,crud=crud , force_print=force_print)

					df_cities_already_runned = df_cities_runned.append(df_cities_already_runned)

					to_return = {
						'cities_already_runned':	json.loads(df_cities_already_runned.to_json(orient='index')),
						'cities_runned':			json.loads(df_cities_runned.to_json(orient='index'))
					}

					to_return= json.dumps(to_return)

					close_rawconn_and_disp_eng(engrawconn_db=engrawconn_db , engrawconn_db_none=engrawconn_db_none)

					if len(df_cities[settings.CITYID_COLUMN]) > 10:

						cities_list = df_cities[settings.CITYID_COLUMN][10:].to_list()
						print(f'Calling : {cities_list}')
						saccount_audience = ''
						data={
							'cities_list':cities_list, 
							'cities_already_runned':json.loads(df_cities_already_runned.to_json(orient='index')),
							'update':update,
							'crud':crud,
							'force_print':force_print
						}
						creds_call  = IAMCredentials_Client().get_service_account_creds( saccount_email='gabriel-functions-caller@gisbicnet-3.iam.gserviceaccount.com' , saccount_audience=saccount_audience )
						call_gcp_function( API_URL=saccount_audience , data=data , credentials=creds_call)

						# print('called')
					elif print_cities_already_runned:
						print(df_cities_already_runned.to_string())

				else:
					to_return = 'No cities found'

		return str(to_return), 200 
	
	except Exception as e:
		error_msg = f"error - {e}"
		print(error_msg)
		return error_msg,200
	 
	return "ok", 200

def get_cars_gdf(car_file:str, path_car_folder:str, df_cities:Union[pd.DataFrame,None]=None, return_bbox:bool=False)->gpd.GeoDataFrame:
	# Necessário alterar a função ring_sample, a solução paleativa foi alterar o último else para return(xmean, ymean). Necessário declarar ambas variáveis o inicio da função.

	if df_cities is None:

		path_car_folder = path_car_folder.replace(']','[]]')
		path_car_folder = path_car_folder.replace('\[','\[[]')
		files_folder_car = os.listdir(path_car_folder)
		files_car = glob.glob(path_car_folder +'\SHAPE_*.zip')
		files_car = [ file for file in files_folder_car]

	else:
		files_car = [path_car_folder + '/SHAPE_' + x for x in df_cities['COD_MUNICIP']]
		# print(files_car)

	n_row = len(files_car)
	gdfs_car = gpd.GeoDataFrame()
	if return_bbox: bbox_car = {}

	for idx, name in enumerate(files_car,1): 
		if idx%20==0:
			print(f'{idx}  of {n_row}')

		# print(f'path_car_folder {path_car_folder}')
		path_car_file = os.path.join(path_car_folder, name)
		try:
			gdf_car_row = get_car_gdf(car_file=car_file, path_car_file=path_car_file)
			if return_bbox: bbox_car[name] = gdf_car_row.bounds.to_dict(orient='index')[0] 
			gdfs_car = gdfs_car.append(gdf_car_row)
		except Exception as e:
			print(f'{idx}  of {n_row}')
			print(name)
			print(f'error : {e}')
		# print(name)
	# print('oi')
	# gdfs_car = gdfs_car.drop(columns=['gid'])
	gdfs_car = gdfs_car.reset_index(drop=True)
	if return_bbox:
		return gdfs_car, bbox_car
	else:
		return gdfs_car

def get_car_gdf_old(car_file:str, path_car_file:str)->gpd.GeoDataFrame:
	
	data = ZipFile(path_car_file, 'r')	
	# if car_file+'.zip' in zips_namelist:
	data2 = ZipFile(BytesIO(data.read(car_file.upper() + ".zip")),'r')
	muni1_cod = int(os.path.basename(path_car_file).replace('SHAPE_','').replace('.zip',''))
	namelist = data2.namelist()

	for row in namelist :
		if row[-3:] == 'dbf':
			dbfname = row 
		elif row[-3:] == 'shp':
			shpname = row 
		elif row[-3:] == 'shx':
			shxname = row 
		elif row[-3:] == 'prj':
			prjname = row 
	
	r = shapefile.Reader(
		shp=BytesIO(data2.read(shpname)),
		shx=BytesIO(data2.read(shxname)),
		prj=BytesIO(data2.read(prjname)),
		dbf=BytesIO(data2.read(dbfname))
	)

	fields = [x[0] for x in r.fields][1:]	
	
	gdf_car = gpd.GeoDataFrame(columns = fields, data = r.records(), geometry = r.shapes())
	gdf_car.columns = gdf_car.columns.str.lower()
	gdf_car = gdf_car.drop_duplicates()
	# gdf_car = gdf_car.reset_index().rename(columns ={'index':'gid'})
	# gdf_car['gid'] = gdf_car['gid']  + 1
	gdf_car = gdf_car.set_crs(epsg=4674)
	gdf_car = gdf_car.to_crs(epsg=4326)

	# df_car = pd.DataFrame(gdf_car)
	gdf_car['muni1_cod'] = muni1_cod
	return gdf_car

def get_car_gdf(car_file:str, path_car_file:str)->gpd.GeoDataFrame:
	
	data = ZipFile(path_car_file+'.zip', 'r')
	zips_namelist = data.namelist()
	zips_namelist_lower = [d.lower() for d in zips_namelist]
	muni1_cod = int(os.path.basename(path_car_file).replace('SHAPE_','').replace('.zip',''))
	
	# car_file = car_file.upper()

	pathzip_file = os.path.join(path_car_file, car_file+'.zip')

	if os.path.exists(pathzip_file):
		# print('a1')
		gdf_car = gpd.read_file(pathzip_file)
	else:
		if car_file.lower() + '.zip' not in zips_namelist_lower:
			# print('a2')
			# print(zips_namelist)
			gdf_car = gpd.GeoDataFrame()
			return gdf_car
		else:			
			# if car_file+'.zip' in zips_namelist:
			data.extract(zips_namelist[zips_namelist_lower.index(car_file.lower() + '.zip')], path=path_car_file)

			gdf_car = gpd.read_file(pathzip_file)

	gdf_car.columns = gdf_car.columns.str.lower()
	gdf_car = gdf_car.drop_duplicates()
	# gdf_car = gdf_car.reset_index().rename(columns ={'index':'gid'})
	# gdf_car['gid'] = gdf_car['gid']  + 1
	gdf_car = gdf_car.set_crs(epsg=4674)
	gdf_car = gdf_car.to_crs(epsg=4326)

	# df_car = pd.DataFrame(gdf_car)
	gdf_car['muni1_cod'] = muni1_cod

	return gdf_car

def filter_df_cities(df_cities:pd.DataFrame, cities_list:Union[list,str])->pd.DataFrame:

	if isinstance(cities_list, str):
		cities_list = [cities_list]

	cities_list = [get_clean_str(str(filter)) for filter in cities_list]

	df_cond1 = df_cities.loc[:,[settings.CITY_COLUMN, settings.STATE_COLUMN]].apply(lambda x: get_clean_str(x[0] +'/'+x[1]) ,axis=1).isin(cities_list)
	df_cond2 = df_cities.loc[:,settings.CITYID_COLUMN].apply(lambda x: get_clean_str(str(x))).isin(cities_list)
	df_cond3 = df_cities.loc[:,settings.STATE_COLUMN].apply(lambda x: get_clean_str(x)).isin(cities_list)
	df_cond4 = df_cities.loc[:,settings.CITY_COLUMN].apply(lambda x: get_clean_str(x)).isin(cities_list)

	df_cities = df_cities[df_cond1 | df_cond2 | df_cond3 | df_cond4]
	
	return df_cities 

def get_df_cities_from_gsheet(gsheet_client:Union[GSheet_Client,None]=None)->pd.DataFrame:
	
	if gsheet_client is None:
		gsheet_client = GSheet_Client(saccount_email='gspread-access@gisbicnet-3.iam.gserviceaccount.com')
	
	df_cities = gsheet_client.get_sheet_by_title(settings.CAR_GSHEETID, settings.CAR_SHEETNAME)
	df_cities = df_cities.sort_values(by=['UF','NOM_MUNICIP'], ascending=[True,True])

	return df_cities

def run_get_cars(df_cities:pd.DataFrame, gdrive_client:Union[GDrive_Client,None]=None, engrawconn_db=None, path_car_folder:str=settings.DOWNLOAD_PATH , update:bool=True ,crud:bool=True , force_print:bool=False):
	
	if gdrive_client is None:
		gdrive_client = GDrive_Client(subject='big@casadosventos.com.br', saccount_email = 'gabriel-functions-caller@gisbicnet-3.iam.gserviceaccount.com')

	########################################################################################################################

	download_car(df_cities=df_cities, download_path=path_car_folder, force_print=force_print)

	########################################################################################################################
	print()	
	print('#'*200)	
	print()	
	print(f'{str(get_now_sp())[:19]} - reading area_imovel')
	cars_gdf_imovel, bbox_imovel = get_cars_gdf(car_file='area_imovel', path_car_folder=path_car_folder, df_cities=df_cities, return_bbox=True)
	print(f'{str(get_now_sp())[:19]} - len {len(cars_gdf_imovel)}')
	
	if update:
		engrawconn_db, engrawconn_db_none = get_eng_and_rawconn(engrawconn_db=engrawconn_db)
		upd_cars_imovel = update_db(gdf=cars_gdf_imovel, schema='externo',table='carimovel_upload', crud=crud, crud_function='function_carimovel_crud', force_print=force_print, updateIdValues=df_cities[settings.CITYID_COLUMN].to_list() )

		print(f'{str(get_now_sp())[:19]} - upd_cars_imovel {upd_cars_imovel}')

	########################################################################################################################

	print()	
	print('#'*200)	
	print()	
	print(f'{str(get_now_sp())[:19]} - reading reserva_legal')
	cars_gdf_reserva = get_cars_gdf(car_file='reserva_legal', path_car_folder=path_car_folder, df_cities=df_cities)
	# print(cars_df)
	print(f'{str(get_now_sp())[:19]} - len {len(cars_gdf_reserva)}')
	
	if update:
		upd_cars_reserva = update_db(gdf=cars_gdf_reserva, schema='externo',table='carreserva_upload', crud=crud, crud_function='function_carreserva_crud', force_print=force_print, updateIdValues=df_cities[settings.CITYID_COLUMN].to_list())
	
		print(f'{str(get_now_sp())[:19]} - upd_cars_reserva {upd_cars_reserva}')

		close_rawconn_and_disp_eng(engrawconn_db=engrawconn_db , engrawconn_db_none=engrawconn_db_none)

	print()	
	print('#'*200)	

	########################################################################################################################

	df_cities_w_dtdownload = get_df_cities_with_dt_download(df_cities=df_cities , path_car_folder=path_car_folder)

	if update and crud:
		print(list(set(upd_cars_reserva + upd_cars_imovel)))
		if list(set(upd_cars_reserva + upd_cars_imovel)) == ['executed'] :
			upsert_db(gdf=df_cities_w_dtdownload , schema='externo', table='car_datadownload', force_print=False)
			print(f'{str(get_now_sp())[:19]} - upsert em car_datadownload ok')

	########################################################################################################################

def get_df_cities_with_dt_download(df_cities:pd.DataFrame, path_car_folder:str=settings.DOWNLOAD_PATH, )->pd.DataFrame:
	
	df_cities_w_dtdownload = df_cities.copy()

	for idx_df, row_df in enumerate(df_cities_w_dtdownload.itertuples(),1):
		# print(idx)
		# print(row_df)
			# print(row_df.COD_MUNICIP)
		cityid = row_df._asdict()[settings.CITYID_COLUMN]
		file_path = os.path.join(path_car_folder, f'SHAPE_{cityid}.zip')
		# print(f'file_path {file_path}')
		mod_time = datetime.fromtimestamp(os.path.getmtime(file_path)).replace(microsecond=0)
		# print(t)
		df_cities_w_dtdownload.loc[df_cities_w_dtdownload[settings.CITYID_COLUMN] == cityid, settings.DTDOWNLOAD_COLUMN] = mod_time 
			# print(f'os.path.getmtime(path) {datetime.fromtimestamp(t) }')

	df_cities_w_dtdownload[settings.CITY_COLUMN] = df_cities_w_dtdownload[settings.CITY_COLUMN].str.replace("'","''")
	df_cities_w_dtdownload.loc[:, settings.CITYID_COLUMN] = df_cities_w_dtdownload.loc[:, settings.CITYID_COLUMN].astype(int)
	df_cities_w_dtdownload.loc[:, settings.DTDOWNLOAD_COLUMN] = pd.to_datetime(df_cities_w_dtdownload.loc[:, settings.DTDOWNLOAD_COLUMN])

	df_cities_w_dtdownload.columns = get_clean_list(df_cities_w_dtdownload.columns)

	df_cities_w_dtdownload = df_cities_w_dtdownload.set_index([settings.CITYID_COLUMN.lower()])
	
	return df_cities_w_dtdownload	
