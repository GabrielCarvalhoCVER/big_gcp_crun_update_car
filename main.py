import os
# from sqlalchemy import create_engine
from flask import Flask, request
import pandas as pd 
import warnings
from access_db.postgresql import *
from gcp_functions.gcp_functions import *
from cver_arcgisapi import *
from cver_incra_requests import *
from cver_funai_requests import *
from gcp_clients.gcp_client_gdrive import *
from gcp_clients.gcp_client_iamcredentials import *
from geoserver_api.geoserver_api import *

app = Flask(__name__)

@app.route('/', methods=['POST'])

def receive_request(request=None)->dict:
	
	try:
		ignore_warning()

		if (request is not None):
			req_json = request_to_json(request)

			layers_list = req_json['layers']
			
			df_functions = get_df_functions()
			df_functions = filter_df_layers(df_functions, layers_list)

			function_called = req_json.get('function','update')

			if function_called == 'get_layers':
				layers_list = df_functions['layer'].tolist()

				to_return = {
					"layers":layers_list
				}

				to_return = json.dumps(to_return)

			elif function_called == 'update':

				getonlycrud = req_json.get('getonlycrud',True)
				update = req_json.get('update',True)
				crud = req_json.get('crud',True)
				force_print = req_json.get('force_print',False)
				df_layers_already_runned = pd.DataFrame.from_dict(req_json.get('layers_already_runned',{}), orient='index') 
				print_layers_already_runned = req_json.get('print_layers_already_runned', True)
		
				if len(df_functions['layer']) >=1:
					os.chdir('/tmp')

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

					df_layer_runned = run_get_geodataframe(df_functions[:1], gdrive_client=gdrive_client, engrawconn_db=engrawconn_db, getonlycrud=getonlycrud, update=update ,crud=crud , force_print=force_print)

					df_layers_already_runned = df_layer_runned.append(df_layers_already_runned)

					# df_layers_already_runned = df_layers_already_runned.reset_index(drop=True).sort_index()

					to_return = {
						'layers_already_runned':	json.loads(df_layers_already_runned.to_json(orient='index')),
						'layer_runned':			 	json.loads(df_layer_runned.to_json(orient='index'))
					}

					to_return= json.dumps(to_return)

					close_rawconn_and_disp_eng(engrawconn_db=engrawconn_db , engrawconn_db_none=engrawconn_db_none)

					if len(df_functions['layer']) >1:

						layer_to_call = df_functions['layer'][1:].to_list()
						print(f'Calling : {layer_to_call}')
						saccount_audience = 'https://us-central1-gisbicnet-3.cloudfunctions.net/update_big_layers_2'
						data={
							'layers':layer_to_call, 
							'layers_already_runned':json.loads(df_layers_already_runned.to_json(orient='index')),
							'getonlycrud':getonlycrud,
							'update':update,
							'crud':crud,
							'force_print':force_print
						}
						creds_call  = IAMCredentials_Client().get_service_account_creds( saccount_email='gabriel-functions-caller@gisbicnet-3.iam.gserviceaccount.com' , saccount_audience=saccount_audience )
						call_gcp_function( API_URL=saccount_audience , data=data , credentials=creds_call)

						# print('called')
					elif print_layers_already_runned:
						print(df_layers_already_runned.to_string())

				else:
					to_return = 'No layer found'

		return str(to_return), 200 
	
	except Exception as e:
		error_msg = f"error - {e}"
		print(error_msg)
		return error_msg,200
	 
	return "ok", 200

def filter_df_layers(df_function:pd.DataFrame, layers_list:Union[list,str])->pd.DataFrame:

	if isinstance(layers_list, str):
		layers_list = [layers_list]

	layers_list = [layer.strip().lower() for layer in layers_list]

	df_function = df_function[(df_function['layer'].str.lower().isin(layers_list)) | (df_function['layer_group'].str.lower().isin(layers_list))]     
	
	return df_function 

def ignore_warning() -> None:
	with warnings.catch_warnings():
		warnings.simplefilter("ignore")
		warnings.warn("deprecated", DeprecationWarning)

def run_get_geodataframe(df_functions:pd.DataFrame, gdrive_client:Union[GDrive_Client,None]=None, update:bool=True, crud:bool=True, engrawconn_db = None, getonlycrud:bool=False, force_print:bool=False) ->pd.DataFrame:
	msg_list = []
	time_start = get_now_sp() 
	# table_updatetime = str(datetime.now((pytz.timezone('America/Sao_Paulo'))))[:19]

	engrawconn_db_none = False
	
	engrawconn_db, engrawconn_db_none = get_eng_and_rawconn(engrawconn_db=engrawconn_db)

	engine_db, rawconn_db = engrawconn_db
	
	print('Starting at ' + str(time_start)[:19])
	for row in df_functions.itertuples():
		time_layer_start = get_now_sp() 
		# print('')run_get_geodataframe
		print('##############################################################################################################################################')
		# print('')
		print('Starting ' + str(row.file_name) + ' - Time: ' + str(time_layer_start)[:19])
	
	
		param = {
			'source' : row.source,
			'layer_group' :  row.layer_group ,
			'file_name' : row.file_name ,
			'rename_col_from' :  row.rename_col_from ,
			'rename_col_to' :  row.rename_col_to ,
			'remove_col' :  row.remove_col ,
			'force_col_order' : row.force_col_order ,				
			'force_int' : row.force_int ,				
			'schema' :  row.schema ,
			'table' :  row.table ,
			'driveId': row.driveId,
			'pk_cols' :  row.pk_cols ,
			'update' : update,
			'crud' : crud,
			'encoding' : row.encoding,
			'all_columns_to_json':row.all_columns_to_json,
			'filename_to_column':row.filename_to_column,
			'gdrive_client' : gdrive_client,
			'upload_foldername' : row.upload_foldername,
			'engrawconn_db' : engrawconn_db,
			'getonlycrud' : getonlycrud,
			'force_print':force_print,
			'force_geometry3d': row.force_geometry3d,
			'folderId':row.folderId_layer,
			'schemaData':row.schemaData
		}
		
		msg = get_geodataframe(**param)


		if (row.layer_group in ['SIGEF','SNCI']) and (msg[2] == 'executed'):

			from gcp_clients.gcp_client_secretmanager import SecretManager_Client

			geoserver_cver = SecretManager_Client().get_secret(secret_id = 'geoserver_cver' , project_id= 'gisbicnet-3')
			geoserver_cver['url'] = 'http://35.225.28.143/geoserver/gwc/rest/seed/'

			geoserver_api_seed(url=geoserver_cver['url'], layer = 'gtcver:incra', login=geoserver_cver['login'], password=geoserver_cver['password'], payloadtype = 'truncate' , zoomStart= 1, zoomStop=24)
			
		time_layer_end = get_now_sp() 
		time_layer_elapsed = str(time_layer_end - time_layer_start)[:7]
		print('Ending ' + str(row.file_name) + ' - Layer Time: ' + str(time_layer_end)[:19] + ' - Elapsed Layer Time: ' + time_layer_elapsed + ' - Elapsed Total Time: ' + str(time_layer_end - time_start)[:7])
		msg = [row.layer] + msg + [time_layer_elapsed]
		print(msg)
		msg_list.append(msg)

	df_msg_list = pd.DataFrame(
		columns=['layer', 'msg_df_read', 'msg_upload', 'msg_crud', 'length', 'time_elapsed'],
		data = msg_list
	)
	df_msg_list = df_msg_list.set_index('layer')

	# print(df_msg_list)
	engrawconn_db_none = False
	
	if engrawconn_db_none == True:
		rawconn_db.close()
		engine_db.dispose()

	time_end = get_now_sp()
	print('Ending' ' - Time: ' + str(time_end)[:19] + ' - Elapsed Total Time: ' + str(time_end - time_start)[:7])

	print()
	return df_msg_list

def get_geodataframe(
		source, layer_group, file_name, schema='', table='', driveId=None, folderId=None, update=False, crud=False,  
		encoding = None, remove_col = None, rename_col_from = None, rename_col_to = None, gdrive_client=None, upload_foldername = False, force_col_order = None, force_int = None, 
		download_txt = False, all_columns_to_json=False, filename_to_column=False, schemaData=True,
		engrawconn_db = None, getonlycrud = False , pk_cols=None,
		force_print=False, force_geometry3d=False
	)->gpd.GeoDataFrame:
	
	# print('source : '+str(source))
	# print('layer_group : '+str(file_name))
	# print('file_name : '+str(layer_group))
	# print('schema : '+str(schema))
	# print('table : '+str(table))
	# print('file_name : '+str(layer_group))
	try:	
		# if '' in rename_col_from:
		#	 rename_col_from.remove('')
		# if '' in rename_col_to:
		#	 rename_col_to.remove('')
		# if '' in remove_col:
		#	 remove_col.remove('')
		# if force_col_order == []:
		#	 force_col_order = None
		# if force_int == []:
		#	 force_int = None				   
		# if read_json == []:
		#	 read_json = None
		
		engrawconn_db_none = False
		
		if source == 'gdrive' :
			
			gdf = get_geodataframe_gdrive(
					folder=layer_group, file_name=file_name, table=table, driveId=driveId, folderId=folderId,
					encoding=encoding, remove_col=remove_col, rename_col_from=rename_col_from, rename_col_to=rename_col_to, gdrive_client=gdrive_client, upload_foldername=upload_foldername, 
					force_col_order=force_col_order , force_int=force_int, all_columns_to_json=all_columns_to_json, filename_to_column=filename_to_column , schemaData=schemaData,
					download_txt=download_txt, force_print=force_print
				)
			
		elif source == 'arcgisapi':
			# print('starting arcgis api')
			if ((engrawconn_db is None) and (update == True)):
				engrawconn_db, engrawconn_db_none = get_eng_and_rawconn(engrawconn_db=engrawconn_db)

			crud_function = 'function_' + table + '_crud'
			gdf = get_geodataframe_arcgisapi(file_name, table, remove_col=remove_col, force_int=force_int, engrawconn_db=engrawconn_db, getonlycrud=getonlycrud, schema=schema, update=update  ,crud_function=crud_function , force_print=force_print )
			# print('ending arcgis api')
		
		elif source == 'incra':			

			gdf = get_gdf_incra(layer=file_name , table=table, force_int=force_int)

		elif source == 'funai': 
			
			gdf = get_gdf_funai(layer=file_name, table=table)

		elif source == 'geoserver': 
			
			gdf = get_gdf_geoserver(layer_group=layer_group, layer=file_name , table=table)
			
			#		 print('crs : ' + str(gdf_idx.crs))
		# printa
		# gdf.to_csv('teste.csv')
		msg_df_read = 'executed'
		
		try:
			if update == True and schema != '' and table != '':
				# print(0)
				engrawconn_db, engrawconn_db_none = get_eng_and_rawconn(engrawconn_db=engrawconn_db)
					
				# print(1)
				crud_function = 'function_' + table + '_crud'
				msg_upload, msg_crud = update_db(gdf, schema, table=table+"_upload",  crud = crud, crud_function = crud_function, engrawconn_db=engrawconn_db, pk_cols=pk_cols, force_print=force_print, force_geometry3d=force_geometry3d)
				# print(1000)
				close_rawconn_and_disp_eng(engrawconn_db=engrawconn_db , engrawconn_db_none=engrawconn_db_none)

			else:
				msg_upload = 'not executed'
				msg_crud = 'not executed'
			
			# del gdf

			
		except Exception as e1:
			msg_upload = 'error get_geodataframe e1 - ' + str(e1)
			print(msg_upload)
			msg_crud = 'not executed'
			

	except Exception as e2:
		msg_df_read = 'error get_geodataframe e2 - ' + str(e2)
		print(msg_df_read)
		msg_upload = 'not executed'
		msg_crud = 'not executed'
	
	try: 
		length = len(gdf)
	except Exception as e3: 
		print(f'error get_geodataframe e3 - {e3}')
		length = 0

	if 'gdf' in vars():
		del gdf

	return [msg_df_read, msg_upload, msg_crud, length]

def get_gdf_geoserver(layer_group:str, layer:str,table:str)->gpd.GeoDataFrame:

	geoserver_api_dict = {
		'IPHAN':{
			'url_base':'http://portal.iphan.gov.br/geoserver/SICG/ows',
			'layers':{
				'IPHAN Sitios Arq. Pontos':{
					'typeName':'SICG:sitios'
				},
				'IPHAN Sitios Arq. Polígonos':{
					'typeName':'SICG:sitios_pol'
				}
			}			
		},
		'IBGE':{
			'url_base':'https://geoservicos.ibge.gov.br/geoserver/ows',
			'espg':4674,
			'layers':{
				'IBGE Vegetação':{
					'typeName':'BDIA:vege_area',
				},
				'IBGE Pedologia':{
					'typeName':'BDIA:pedo_area'
				}
			}
		}
	}
	layer_group_dict = geoserver_api_dict[layer_group]

	url_base = layer_group_dict['url_base']
	layer_dict = layer_group_dict['layers'][layer]

	param = {}
	param['service'] = layer_dict.get('service','WFS')
	param['version'] = layer_dict.get('version','2.0.0')
	param['request'] = 'GetFeature'
	param['outputFormat'] = layer_dict.get('outputFormat','json')
	param['typeName'] = layer_dict['typeName']
	
	epsg= layer_dict.get('epsg',layer_group_dict.get('epsg',4326))
	
	
	table_updatetime = get_now_sp()
	
	# req = requests.post(url_base, data=param)
	req = geoserver_request(url=url_base, **param)

	gdf=gpd.read_file(req.text)
	
	gdf[table + '_create_time'] = table_updatetime

	gdf.columns = get_clean_list(gdf.columns)

	gdf = gdf.to_crs(epsg=epsg)

	return gdf

def get_df_geom_col_type(schema:str, table:str, geometry_col:str,  engrawconn_db=None , db_secret:Union[str,None]=None):
	engrawconn_db, engrawconn_db_none  = get_eng_and_rawconn(engrawconn_db , db_secret=db_secret)
	
	query_psql_geom_col_type = '''
	select 
		case 
			when count(distinct(replace(lower(st_geometrytype("''' + geometry_col + '''")),'_multi',''))) > 1
				then 'geometry'
			when count(distinct(lower(st_geometrytype("geometry")))) = 1
				then string_agg(distinct(replace(lower(st_geometrytype("'''+geometry_col +'''")),'st_','')),'') 
			else 	
				string_agg(distinct('multi'||replace(replace(lower(st_geometrytype("'''+ geometry_col+ '''")),'_multi',''),'st_','')),'') 
			end as geometry_type
	from
		 "''' + schema + '"."' + table  +'"'
	
	df_psql_geom_col_type = get_df_sql(sql_query_string=query_psql_geom_col_type , engrawconn_db=engrawconn_db, db_secret=db_secret)

	close_rawconn_and_disp_eng(engrawconn_db=engrawconn_db, engrawconn_db_none=engrawconn_db_none )

	return df_psql_geom_col_type

def get_df_columns_type(schema:str, table:str, engrawconn_db=None , db_secret:Union[str,None]=None)->pd.DataFrame:
	query_psql_columns_type = '''
		select distinct
			c.column_name
		,	c.data_type 
		,	c.udt_name || coalesce( '(' || c.character_maximum_length  || ')','') as dbtype
		,	coalesce(geom_col.coord_dimension , geog_col.coord_dimension) as coord_dimension
		,	coalesce(geom_col.srid , geog_col.srid) as srid
		,	case when c.is_nullable = 'YES' then 'null' else 'not null' end as null
		,	coalesce(geom_col.type , geog_col.type) as geom_type
		from 
			information_schema.tables t
		left join information_schema.columns c on 
			c.table_name = t.table_name 
			and c.table_schema = t.table_schema
		left join
			pg_catalog.pg_class pgc on
				pgc.relname = t.table_name 
		left join
			pg_catalog.pg_description pgd on
				pgd.objoid = pgc.relfilenode 
		left join 
			geometry_columns geom_col on 
				geom_col.f_table_name = t.table_name
				and geom_col.f_table_schema = t.table_schema
				and geom_col.f_geometry_column = column_name
		left join 
			geography_columns geog_col on 
				geog_col.f_table_name = t.table_name
				and geog_col.f_table_schema = t.table_schema
				and geog_col.f_geography_column = column_name
		where 1 =1 
		and table_type = 'BASE TABLE'
		and t.table_name = ''' + "'" + table + "'" + '''
		and t.table_schema = ''' + "'" + schema + "'" +'''
		--and pgd.objsubid  = 0
		-- order by 
		-- c.udt_name
	'''

	# print(query_psql_column_type)

	df_psql_columns_type = get_df_sql(sql_query_string=query_psql_columns_type , engrawconn_db=engrawconn_db, db_secret=db_secret)
	return df_psql_columns_type

def create_table_from_upload_version(schema:str, table:str, pk_cols=None, engrawconn_db=None , db_secret=None ):

	engrawconn_db, engrawconn_db_none  = get_eng_and_rawconn(engrawconn_db , db_secret=db_secret)
	engine_db, rawconn_db = engrawconn_db
	cur_db = rawconn_db.cursor()
	
	table_upload = table + '_upload'
	
	df_psql_columns_type = get_df_columns_type(schema=schema, table=table_upload , engrawconn_db=engrawconn_db)

	for df_row in df_psql_columns_type.itertuples():
		if df_row.dbtype == 'geometry':
	#		 print(df_row)
			geom_col_type = get_df_geom_col_type(schema=schema , table=table_upload , geometry_col = df_row.column_name, engrawconn_db=engrawconn_db).values[0]
	#		 print(geom_col_type)

			if df_row.coord_dimension==2:
				z_dim =''
			elif df_row.coord_dimension==3:
				z_dim='z'
			elif df_row.coord_dimension==4:
				z_dim='zm'

			df_psql_columns_type.loc[df_psql_columns_type.index == df_row.Index, 'dbtype'] = 'geometry('+geom_col_type+z_dim+' , '+str(int(df_row.srid)) + ')'

	create_psql_table = '\t' + '\n,\t'.join(['"' + row_df.column_name + '" ' + row_df.dbtype + ' ' + row_df.null for row_df in df_psql_columns_type.itertuples()])
	create_psql_table = create_psql_table + "\n,\t\""+ table + "_update_time\" timestamp null"
	create_psql_table = 'create table "'+schema + '"."' + table +'" (\n'  + create_psql_table + '\n)'
	
	print(create_psql_table)
	cur_db.execute(create_psql_table)
	rawconn_db.commit()
	
	if pk_cols is not None:
		if isinstance(pk_cols,str):
			pk_cols = [pk_cols]

		sql_create_index = 'ALTER TABLE '+schema+'."'+table+'" add CONSTRAINT "'+schema+'_'+table + '" primary key ('+ ','.join(['"'+ pk_col + '"' for pk_col in pk_cols]) + ')'
		print(sql_create_index)
		try:
			cur_db.execute(sql_create_index) 
			rawconn_db.commit()
		except Exception as e:
			print('Error : '+str(e))
		
	close_rawconn_and_disp_eng(engrawconn_db=engrawconn_db, engrawconn_db_none=engrawconn_db_none )
	
def get_crud_sql(schema:str, table:str, pk_cols=None, upload_ids=False, engrawconn_db=None, db=None):
	engrawconn_db, engrawconn_db_none  = get_eng_and_rawconn(engrawconn_db=engrawconn_db , db=db)

	table_upload = table + '_upload'

	df_psql_columns_type = get_df_columns_type(schema=schema, table=table , engrawconn_db=engrawconn_db)
	df_psql_columns_type_upload = get_df_columns_type(schema=schema, table=table_upload , engrawconn_db=engrawconn_db)


	col_up = [row for row in df_psql_columns_type_upload.column_name.values]
	col_tb = [row for row in df_psql_columns_type.column_name.values]
	# col_tb_notpk = [row for row in df_psql_columns_type.loc[df_psql_columns_type['null'] == 'null' ,'column_name' ].values if row not in pk_cols]
	# col_tb_notnull = [row for row in df_psql_columns_type.loc[df_psql_columns_type['null'] == 'not null' ,'column_name' ].values if row not in pk_cols]
	# col_up_notpk = [row for row in df_psql_columns_type.loc[df_psql_columns_type_upload['null'] == 'null' ,'column_name' ].values if row not in pk_cols]
	# col_up_notnull = [row for row in df_psql_columns_type_upload.loc[df_psql_columns_type_upload['null'] == 'not null' ,'column_name' ].values if row not in pk_cols]

	col_tb_check = [row for row in df_psql_columns_type.loc[: ,'column_name' ].values if ((row not in pk_cols) and (row[-12:]!='_create_time') and (row[-12:]!='_update_time'))]

	if pk_cols is not None:
		if isinstance(pk_cols,str):
			pk_cols = [pk_cols]
	#	 pk_cols = pk_cols

		sql_drop_temp_table = """
			drop table if exists temp_ins_"""+table+""";
			drop table if exists temp_del_"""+table+""";
			drop table if exists temp_atu_"""+table+""";\n		
		"""

		sql_create_temp_ins_table = """
			create temp table temp_ins_"""+table+"""  as 
			select """ + ','.join(['"' + pk_col + '"' for pk_col in pk_cols]) + """
			from 
				"""+ schema + '."' + table_upload + '"' + """ up
			where 
				not(exists( select null from """ + schema +'."'+table + '" tb where ' + 'and'.join(['tb."' + pk_col + '"=' +'up."' + pk_col + '"' for pk_col in pk_cols]) + "));\n"

		
		if upload_ids: 
			sql_del_upload_ids = """ and not(exists (select null from """ + schema +'."'+table_upload + '_ids" up where ' + 'and'.join(['tb."' + pk_col + '"=' +'up."' + pk_col + '"' for pk_col in pk_cols]) + "))\n"
		else:
			sql_del_upload_ids=''
			
		sql_create_temp_del_table = """
			create temp table temp_del_"""+table+"""  as 
			select """ + ','.join(['"' + pk_col + '"' for pk_col in pk_cols]) + """
			from 
				"""+ schema + '."' + table + '"' + """ tb
			where 
				not(exists (select null from """ + schema +'."'+table_upload + '" up where ' + 'and'.join(['tb."' + pk_col + '"=' +'up."' + pk_col + '"' for pk_col in pk_cols]) + "))\n"+sql_del_upload_ids+";\n"
				

		sql_create_temp_atu_table = """
			create temp table temp_atu_"""+table+"""  as 
			select """ + ','.join(['tb."' + pk_col + '"' for pk_col in pk_cols]) + """
			from 
				"""+ schema + '."' + table + '"' + """ tb
			inner join 
				"""+ schema + '."' + table_upload + '"' + """ up on 
					""" + 'and'.join(['tb."' + pk_col + '"=' +'up."' + pk_col + '"' for pk_col in pk_cols]) +"""
			where
				1=1
				and not(
					1=1""" + ''.join(['\n\t\tand check_identity(tb."' + col + '",up."' + col + '")' for col in col_tb_check]) + """ 
				);

				"""

		sql_create_del_table = """
			delete from """+ schema + '."' + table + '"' + """ tb
			where 
				exists (select null from temp_del_"""+table+""" del where """ +'and'.join(['tb."' + pk_col + '"=' +'del."' + pk_col + '"' for pk_col in pk_cols]) + """) ;

		"""

		sql_create_ins_table = """
			insert into  """+ schema + '."' + table + '"' + """  
				(""" +', '.join(['"' + col + '"' for col in col_tb]) +""")
			select 
				""" + '\t'+'\n\t,\t'.join(['"' + col.replace('_update_time','_create_time') + '"' for col in col_tb]) + """
			from 
				"""+ schema + '."' + table_upload + '"' + """ up  
			where
				exists (select null from temp_ins_"""+table+""" ins where """ +'and'.join(['up."' + pk_col + '"=' +'ins."' + pk_col + '"' for pk_col in pk_cols]) + """) ;

		"""

		sql_create_atu_table = """
			update 
				"""+ schema + '."' + table + '"' + """ tb 
			set
				""" + '\t'+ '\n\t,\t'.join(['"'+col.replace('_create_time','_update_time')+'"=up."'+col + '"' for col in col_up]) +"""
			from 
				"""+ schema + '."' + table_upload + '"' + """ up  
			where
				exists (select null from temp_atu_"""+table+""" atu where """ +'and'.join(['up."' + pk_col + '"=' +'atu."' + pk_col+ '"' for pk_col in pk_cols]) + """) ;\n

		"""
		if upload_ids: 
			sql_trunc_upload_ids = '\ttruncate ' + schema +'."'+table_upload + '_ids";\n'
		else:
			sql_trunc_upload_ids = ''
		
		sql_truncate = '\ttruncate '+schema +'."'+table_upload + '";\n' + sql_trunc_upload_ids 
		sql_crud = sql_drop_temp_table + sql_create_temp_ins_table + sql_create_temp_del_table + sql_create_temp_atu_table + sql_create_del_table + sql_create_ins_table + sql_create_atu_table + sql_truncate+sql_drop_temp_table
		print(sql_crud)

	close_rawconn_and_disp_eng(engrawconn_db=engrawconn_db, engrawconn_db_none=engrawconn_db_none )

def get_df_functions()->pd.DataFrame:
	
	# df_functions = pd.DataFrame.from_dict(get_dict_functions(), orient='index').rename_axis('layer').reset_index().astype(object)
	df_functions =pd.concat({k0:pd.concat({k1:pd.DataFrame.from_dict(v1, orient='index') for k1,v1 in v0.items()},axis=0) for k0,v0 in get_dict_functions().items()},axis=0).reset_index().rename(columns={'level_0':'source', 'level_1':'layer_group', 'level_2':'layer'}).astype(object)

	df_functions.loc[:,'all_columns_to_json'] =  df_functions.loc[:,'all_columns_to_json'].fillna(False)
	df_functions.loc[:,'filename_to_column'] =  df_functions.loc[:,'filename_to_column'].fillna(False)
	df_functions.loc[:,'force_geometry3d'] =  df_functions.loc[:,'force_geometry3d'].fillna(False)
	df_functions.loc[:,'schemaData'] =  df_functions.loc[:,'schemaData'].fillna(True)

	df_functions = df_functions.where(pd.notnull(df_functions), None)

	return df_functions

def get_dict_functions()->dict:

	dict_functions = {
		"incra": {
			"SIGEF": {
				"sigef_MG": {
					"file_name": "SIGEF_MG",
					"schema": "externo",
					"table": "incra_sigef",
					"force_int": ["municipio_", "uf_id"]
				},
				"sigef_SP": {
					"file_name": "SIGEF_SP",
					"schema": "externo",
					"table": "incra_sigef",
					"force_int": ["municipio_", "uf_id"]
				},
				"sigef_GO": {
					"file_name": "SIGEF_GO",
					"schema": "externo",
					"table": "incra_sigef",
					"force_int": ["municipio_", "uf_id"]
				},
				"sigef_PR": {
					"file_name": "SIGEF_PR",
					"schema": "externo",
					"table": "incra_sigef",
					"force_int": ["municipio_", "uf_id"]
				},
				"sigef_RS": {
					"file_name": "SIGEF_RS",
					"schema": "externo",
					"table": "incra_sigef",
					"force_int": ["municipio_", "uf_id"]
				},
				"sigef_MT": {
					"file_name": "SIGEF_MT",
					"schema": "externo",
					"table": "incra_sigef",
					"force_int": ["municipio_", "uf_id"]
				},
				"sigef_MS": {
					"file_name": "SIGEF_MS",
					"schema": "externo",
					"table": "incra_sigef",
					"force_int": ["municipio_", "uf_id"]
				},
				"sigef_TO": {
					"file_name": "SIGEF_TO",
					"schema": "externo",
					"table": "incra_sigef",
					"force_int": ["municipio_", "uf_id"]
				},
				"sigef_BA": {
					"file_name": "SIGEF_BA",
					"schema": "externo",
					"table": "incra_sigef",
					"force_int": ["municipio_", "uf_id"]
				},
				"sigef_SC": {
					"file_name": "SIGEF_SC",
					"schema": "externo",
					"table": "incra_sigef",
					"force_int": ["municipio_", "uf_id"]
				},
				"sigef_MA": {
					"file_name": "SIGEF_MA",
					"schema": "externo",
					"table": "incra_sigef",
					"force_int": ["municipio_", "uf_id"]
				},
				"sigef_PA": {
					"file_name": "SIGEF_PA",
					"schema": "externo",
					"table": "incra_sigef",
					"force_int": ["municipio_", "uf_id"]
				},
				"sigef_PB": {
					"file_name": "SIGEF_PB",
					"schema": "externo",
					"table": "incra_sigef",
					"force_int": ["municipio_", "uf_id"]
				},
				"sigef_RO": {
					"file_name": "SIGEF_RO",
					"schema": "externo",
					"table": "incra_sigef",
					"force_int": ["municipio_", "uf_id"]
				},
				"sigef_PI": {
					"file_name": "SIGEF_PI",
					"schema": "externo",
					"table": "incra_sigef",
					"force_int": ["municipio_", "uf_id"]
				},
				"sigef_ES": {
					"file_name": "SIGEF_ES",
					"schema": "externo",
					"table": "incra_sigef",
					"force_int": ["municipio_", "uf_id"]
				},
				"sigef_AM": {
					"file_name": "SIGEF_AM",
					"schema": "externo",
					"table": "incra_sigef",
					"force_int": ["municipio_", "uf_id"]
				},
				"sigef_RJ": {
					"file_name": "SIGEF_RJ",
					"schema": "externo",
					"table": "incra_sigef",
					"force_int": ["municipio_", "uf_id"]
				},
				"sigef_PE": {
					"file_name": "SIGEF_PE",
					"schema": "externo",
					"table": "incra_sigef",
					"force_int": ["municipio_", "uf_id"]
				},
				"sigef_RN": {
					"file_name": "SIGEF_RN",
					"schema": "externo",
					"table": "incra_sigef",
					"force_int": ["municipio_", "uf_id"]
				},
				"sigef_CE": {
					"file_name": "SIGEF_CE",
					"schema": "externo",
					"table": "incra_sigef",
					"force_int": ["municipio_", "uf_id"]
				},
				"sigef_SE": {
					"file_name": "SIGEF_SE",
					"schema": "externo",
					"table": "incra_sigef",
					"force_int": ["municipio_", "uf_id"]
				},
				"sigef_AL": {
					"file_name": "SIGEF_AL",
					"schema": "externo",
					"table": "incra_sigef",
					"force_int": ["municipio_", "uf_id"]
				},
				"sigef_RR": {
					"file_name": "SIGEF_RR",
					"schema": "externo",
					"table": "incra_sigef",
					"force_int": ["municipio_", "uf_id"]
				},
				"sigef_AC": {
					"file_name": "SIGEF_AC",
					"schema": "externo",
					"table": "incra_sigef",
					"force_int": ["municipio_", "uf_id"]
				},
				"sigef_DF": {
					"file_name": "SIGEF_DF",
					"schema": "externo",
					"table": "incra_sigef",
					"force_int": ["municipio_", "uf_id"]
				},
				"sigef_AP": {
					"file_name": "SIGEF_AP",
					"schema": "externo",
					"table": "incra_sigef",
					"force_int": ["municipio_", "uf_id"]
				}
			},
			"SNCI": {
				"snci_MG": {
					"file_name": "SNCI_MG",
					"schema": "externo",
					"table": "incra_snci"
				},
				"snci_SP": {
					"file_name": "SNCI_SP",
					"schema": "externo",
					"table": "incra_snci"
				},
				"snci_GO": {
					"file_name": "SNCI_GO",
					"schema": "externo",
					"table": "incra_snci"
				},
				"snci_PR": {
					"file_name": "SNCI_PR",
					"schema": "externo",
					"table": "incra_snci"
				},
				"snci_RS": {
					"file_name": "SNCI_RS",
					"schema": "externo",
					"table": "incra_snci"
				},
				"snci_MT": {
					"file_name": "SNCI_MT",
					"schema": "externo",
					"table": "incra_snci"
				},
				"snci_MS": {
					"file_name": "SNCI_MS",
					"schema": "externo",
					"table": "incra_snci"
				},
				"snci_TO": {
					"file_name": "SNCI_TO",
					"schema": "externo",
					"table": "incra_snci"
				},
				"snci_BA": {
					"file_name": "SNCI_BA",
					"schema": "externo",
					"table": "incra_snci"
				},
				"snci_SC": {
					"file_name": "SNCI_SC",
					"schema": "externo",
					"table": "incra_snci"
				},
				"snci_MA": {
					"file_name": "SNCI_MA",
					"schema": "externo",
					"table": "incra_snci"
				},
				"snci_PA": {
					"file_name": "SNCI_PA",
					"schema": "externo",
					"table": "incra_snci"
				},
				"snci_PB": {
					"file_name": "SNCI_PB",
					"schema": "externo",
					"table": "incra_snci"
				},
				"snci_RO": {
					"file_name": "SNCI_RO",
					"schema": "externo",
					"table": "incra_snci"
				},
				"snci_PI": {
					"file_name": "SNCI_PI",
					"schema": "externo",
					"table": "incra_snci"
				},
				"snci_ES": {
					"file_name": "SNCI_ES",
					"schema": "externo",
					"table": "incra_snci"
				},
				"snci_AM": {
					"file_name": "SNCI_AM",
					"schema": "externo",
					"table": "incra_snci"
				},
				"snci_RJ": {
					"file_name": "SNCI_RJ",
					"schema": "externo",
					"table": "incra_snci"
				},
				"snci_PE": {
					"file_name": "SNCI_PE",
					"schema": "externo",
					"table": "incra_snci"
				},
				"snci_RN": {
					"file_name": "SNCI_RN",
					"schema": "externo",
					"table": "incra_snci"
				},
				"snci_CE": {
					"file_name": "SNCI_CE",
					"schema": "externo",
					"table": "incra_snci"
				},
				"snci_SE": {
					"file_name": "SNCI_SE",
					"schema": "externo",
					"table": "incra_snci"
				},
				"snci_AL": {
					"file_name": "SNCI_AL",
					"schema": "externo",
					"table": "incra_snci"
				},
				"snci_RR": {
					"file_name": "SNCI_RR",
					"schema": "externo",
					"table": "incra_snci"
				},
				"snci_AC": {
					"file_name": "SNCI_AC",
					"schema": "externo",
					"table": "incra_snci"
				},
				"snci_DF": {
					"file_name": "SNCI_DF",
					"schema": "externo",
					"table": "incra_snci"
				},
				"snci_AP": {
					"file_name": "SNCI_AP",
					"schema": "externo",
					"table": "incra_snci"
				}
			},
			"04. Assentamento": {
				"assentamento": {
					"file_name": "assentamento",
					"schema": "externo",
					"table": "incra_assentamento"
				}
			},
			"05. Quilombolas": {
				"quilombo": {
					"file_name": "quilombo",
					"schema": "externo",
					"table": "incra_quilombola"
				}
			}
		},
		"gdrive": {
			"06. LTs": {
				"lt_existente": {
					"file_name": "LT_EXISTENTE.shp",
					"schema": "externo",
					"table": "lts_lt_existente",
					"rename_col_from": ["Observaç"],
					"rename_col_to": ["Observação"],
					"remove_col": ["CEG","Id"]
				},
				"lt_planejada": {
					"file_name": "LT_PLANEJADA.shp",
					"schema": "externo",
					"table": "lts_lt_planejada"
				},
				"lt_projetos": {
					"file_name": "Linhas_projetos.shp",
					"schema": "externo",
					"table": "lts_linhas_projetos"
				}
			},
			"07. Subestação": {
				"sin": {
					"file_name": "SE.shp",
					"schema": "externo",
					"table": "ses_sin",
					"force_col_order": ["Nome da SE","Tensão","Margem A-4","Tust A-4_2","geometry"]
				},
				"dit": {
					"file_name": "DITs.shp",
					"schema": "externo",
					"table": "ses_dit"
				}
			},
			"Ambiental": {
				"app_declividade": {
					"file_name": "APP_DECLIVIDADE.shp",
					"schema": "externo",
					"table": "amb_app_declividade"
				},
				"app_espeleologia": {
					"file_name": "APP_ESPELEOLOGIA.shp",
					"schema": "externo",
					"table": "amb_app_espeleologia"
				},
				"app_edificacoes": {
					"file_name": "APP_EDIFICAÇÕES.shp",
					"schema": "externo",
					"table": "amb_app_edificacoes"
				},
				"app_hidrografia": {
					"file_name": "APP_HIDROGRAFIA.shp",
					"schema": "externo",
					"table": "amb_app_hidrografia",
					"remove_col": ["Id"]
				},
				"topo_morro": {
					"file_name": "TOPO_MORRO.shp",
					"schema": "externo",
					"table": "amb_topo_morro"
				},
				"arqueologia": {
					"file_name": "ARQUEOLOGIA.shp",
					"schema": "externo",
					"table": "amb_arqueologia"
				},
				"app_arqueologia": {
					"file_name": "APP_ARQUEOLOGIA.shp",
					"schema": "externo",
					"table": "amb_app_arqueologia"
				},
				"hidrografia": {
					"file_name": "HIDROGRAFIA.shp",
					"schema": "externo",
					"table": "amb_hidrografia"
				},
				"edf_area": {
					"file_name": "EDF_AREA.shp",
					"schema": "externo",
					"table": "amb_edificacoes_area"
				},
				"edf_ponto": {
					"file_name": "EDF_PONTO.shp",
					"schema": "externo",
					"table": "amb_edificacoes_ponto"
				},
				"div_poligonos": {
					"file_name": "Diversos_Poligono.shp",
					"schema": "externo",
					"table": "amb_div_poligono"
				},
				"div_pontos": {
					"file_name": "Diversos_Ponto.shp",
					"schema": "externo",
					"table": "amb_div_ponto"
				}
				# "ucstodas": {
				#	 "file_name": "ucstodas.shp",
				#	 "schema": "externo",
				#	 "table": "amb_ucstodas",
				#	 "rename_col_from": ["ID_UC0","NOME_UC1","ID_WCMC2","CATEGORI3","GRUPO4","ESFERA5","ANO_CRIA6","GID7","QUALIDAD8","ATO_LEGA9","DT_ULTIM10","CODIGO_U11","NOME_ORG12"],
				#	 "rename_col_to": ["ID_UC","OME_UC","ID_WCMC","CATEGORIA","GRUPO","ESFERA","ANO_CRIA","GID","QUALIDAD","ATO_LEGA","DT_ULTIM","CODIGO_U","NOME_ORG"]
				# }
			},
			"Ambiental - Lic Previa": {
				"amb_lic_previa": {
					"file_name": "*.shp",
					"schema": "externo",
					"table": "amb_licenca_previa",
					'all_columns_to_json':True,
					'filename_to_column':True,
					'force_geometry3d':False
				}
			},
			"Ambiental - Lic Implant": {
				"amb_lic_implant": {
					"file_name": "*.shp",
					"schema": "externo",
					"table": "amb_licenca_implantacao",
					'all_columns_to_json':True,
					'filename_to_column':True,
					'force_geometry3d':True
				}
			},
			"09. Implantação": {
				"impl_rmt": {
					"file_name": "RMT.shp",
					"schema": "externo",
					"table": "impl_rmt",
					"upload_foldername": True
				},
				"impl_suv": {
					"file_name": "SUV.shp",
					"schema": "externo",
					"table": "impl_suv",
					"upload_foldername": True
				},
				"impl_linhatrans": {
					"file_name": "LT.shp",
					"schema": "externo",
					"table": "impl_linhatrans",
					"upload_foldername": True
				},
				"impl_subestacao": {
					"file_name": "Subestacao.shp",
					"schema": "externo",
					"table": "impl_subestacao",
					"upload_foldername": True
				},
				"impl_plataforma": {
					"file_name": "Plataformas.shp",
					"schema": "externo",
					"table": "impl_plataforma",
					"upload_foldername": True
				},
				"impl_canteiro": {
					"file_name": "Canteiros.shp",
					"schema": "externo",
					"table": "impl_canteiro",
					"upload_foldername": True
				},
				"impl_acesso": {
					"file_name": "Acessos.shp",
					"schema": "externo",
					"table": "impl_acesso",
					"upload_foldername": True
				},
				"impl_poligonais": {
					"file_name": "Poligonais.shp",
					"schema": "externo",
					"table": "impl_poligonais",
					"upload_foldername": True
				}
			},
			"Rotas Torres": {
				"torres_rotas": {
					"file_name": "*.kmz",
					"schema": "externo",
					"table": "tor_rotas",
					"driveId":'0ALHViyzWStUcUk9PVA',
					"filename_to_column":True
				}
			},
			"SAI": {
				"sai_polig_aerolevant": {
					"file_name": "*.kmz",
					"schema": "externo",
					"table": "sai_polig_aerolevant",
					"folderId_layer":'1Pnw0EdYL8OrMAHXmIjp3C4KZkIfWw2wH',
					"filename_to_column":True, 
					'schemaData':False
				}
			},
			
			"Vortex": {
				"vortex": {
					"file_name": "Vortex-runs.kml",
					"schema": "externo",
					"table": "vortex_runs"
				}
			},
			"ICMBIO": {
				"icmbio_espeleologia": {
					"file_name": "CANIE.zip",
					"schema": "externo",
					"table": "icmbio_espeleologia"
				}
			},
			"Faixas de Servidão": {
				"lt_faixa_servidao": {
					"file_name": "*.kmz",
					"schema": "externo",
					"table": "lt_faixa_servidao",
					"upload_foldername": True
				}
			},
			"CDA": {
				"cda_quilombolas": {
					"file_name": "Discriminatórias Quilombolas.shp",
					"schema": "externo",
					"table": "cda_quilombolas"
				},
				"cda_fundosefechosdepastos": {
					"file_name": "Fundos e Fechos de Pastos.shp",
					"schema": "externo",
					"table": "cda_fundosefechosdepastos"
				},
				"cda_pts_quilombolas": {
					"file_name": "ponto_quilombola_palmares.shp",
					"schema": "externo",
					"table": "cda_pts_quilombolas"
				},
				"cda_pts_fundosefechosdepastos": {
					"file_name": "pontos_ffp.shp",
					"schema": "externo",
					"table": "cda_pts_fundosefechosdepastos"
				}
			},
			"Curvas de Nível": {
				"curvas_de_nivel": {
					"file_name": "*.shp",
					"schema": "externo",
					"table": "curvas_de_nivel",
					'all_columns_to_json':True,
					'filename_to_column':True,
					'force_geometry3d':True
				}
			},
			"IBAMA": {
				"denef_offshore": {
					"file_name": "Eolicas Offshore.kmz",
					"schema": "externo",
					"table": "ibama_eolicas_offshore"
				}
			},
			"MMA": {
				"ucs_todas": {
					"file_name": "ucstodas.shp",
					"schema": "externo",
					"table": "mma_ucs_todas",
					"rename_col_from": ["ID_UC0","NOME_UC1","ID_WCMC2","CATEGORI3","GRUPO4","ESFERA5","ANO_CRIA6","GID7","QUALIDAD8","ATO_LEGA9","DT_ULTIM10","CODIGO_U11","NOME_ORG12"],
					"rename_col_to": ["ID_UC","OME_UC","ID_WCMC","CATEGORIA","GRUPO","ESFERA","ANO_CRIA","GID","QUALIDAD","ATO_LEGA","DT_ULTIM","CODIGO_U","NOME_ORG"]
				}
			},
			"Unidades de Conservação Extras": {
				"ucs_extras": {
					"file_name": "*.shp",
					"schema": "externo",
					"table": "amb_ucsextras",
					'encoding':'utf8',
					'all_columns_to_json':True,
					'filename_to_column':True
				}
			}
		},
		"funai": {
			"FUNAI": {
				"TIND_Estudo": {
					"file_name": "TIND_Estudo",
					"schema": "externo",
					"table": "funai_tind_estudo"
				},
				"TIND_Principal": {
					"file_name": "TIND_Principal",
					"schema": "externo",
					"table": "funai_tind_principal"
				}
			}
		},
		"arcgisapi": {
			"SIGEL": {
				"sig_usina_ftv": {
					"file_name": "SIGEL Usinas UFV",
					"schema": "externo",
					"table": "sigel_usina_ftv",
					"pk_cols": "objectid",
					# "read_json": True,
					"force_int": ["OBJECTID","ID_EMPREENDIMENTO",'codmun']
				},
				"sig_usina_eel": {
					"file_name": "SIGEL Usinas EOL",
					"schema": "externo",
					"table": "sigel_usina_eel",
					"pk_cols": "objectid",
					"force_int":["codmun"],
					# "read_json": True,
					"force_int": ["ID_EMPREENDIMENTO", 'eol_versao_id','id_lt','id_se']
				},
				"sig_usina_tel": {
					"file_name": "SIGEL Usinas UTE",
					"schema": "externo",
					"table": "sigel_usina_tel",
					"pk_cols": "objectid"
					# "read_json": True
				},
				"sig_usina_tnc": {
					"file_name": "SIGEL Usinas UTN",
					"schema": "externo",
					"table": "sigel_usina_tnc",
					"pk_cols": "objectid"
					# "read_json": True
				},
				"sig_usina_hel": {
					"file_name": "SIGEL Usinas UHE",
					"schema": "externo",
					"table": "sigel_usina_hel",
					"pk_cols": "objectid",
					"force_int":['n_unid_gerad'],
					# "read_json": True,
					"remove_col": ["TABELA_SERIE"]
				},
				"sig_aerog": {
					"file_name": "SIGEL Aerogeradores",
					"schema": "externo",
					"table": "sigel_aerogerador",
					"pk_cols": "objectid"
					# "read_json": True
				},
				# "sig_lt_ons": {
					# "file_name": "SIGEL Linha Trans ONS",
					# "schema": "externo",
					# "table": "sigel_linhatrans_ons",
					# "pk_cols": "objectid"
					# # "read_json": True
				# },
				"sig_reg_int": {
					"file_name": "SIGEL Região Interf",
					"schema": "externo",
					"table": "sigel_regiao_interf",
					"pk_cols": "objectid",
					# "read_json": true,
					"force_int": ["OBJECTID"]
				},
				"sig_peq_centr_hel": {
					"file_name": "SIGEL PCH",
					"schema": "externo",
					"table": "sigel_peq_centr_hel",
					"pk_cols": "objectid",
					# "read_json": true,
					"remove_col": ["TABELA_SERIE"],
					'force_int':['n_unid_gerad']
				},
				"sig_parque_eel": {
					"file_name": "SIGEL Políg Parque EOL",
					"schema": "externo",
					"table": "sigel_parque_eel",
					"pk_cols": "objectid",
					"force_int":["eol_versao_id"]
					# "read_json": true
				},
				"sig_dup": {
					"file_name": "SIGEL DUP",
					"schema": "externo",
					"table": "sigel_decl_util_publ",
					"pk_cols": "objectid",
					# "read_json": true,
					"force_int": ["ID_EMPREENDIMENTO"]
				}
			},
			"EPE": {
				"epe_gasoduto_distrib": {
					"file_name": "EPE Gasodutos de distribuição",
					"schema": "externo",
					"table": "epe_gasoduto_distrib"
					# "read_json": true
				},
				"epe_gasoduto_transp": {
					"file_name": "EPE Gasodutos de transporte",
					"schema": "externo",
					"table": "epe_gasoduto_transp"
					# "read_json": true
				},
				"epe_duto_escoam": {
					"file_name": "EPE Dutos de escoamento",
					"schema": "externo",
					"table": "epe_duto_escoam"
					# "read_json": true
				},
				"epe_planta_etanol": {
					"file_name": "EPE Plantas de etanol",
					"schema": "externo",
					"table": "epe_planta_etanol"
					# "read_json": true
				},
				"epe_planta_biodiesel": {
					"file_name": "EPE Plantas de biodiesel",
					"schema": "externo",
					"table": "epe_planta_biodiesel"
					# "read_json": true
				},
				"epe_planta_biogas": {
					"file_name": "EPE Plantas de biogás",
					"schema": "externo",
					"table": "epe_planta_biogas"
					# "read_json": true
				},
				"epe_lt_plan": {
					"file_name": "EPE Linhas de Transmissão - Expansão Planejada",
					"schema": "externo",
					"table": "epe_lt_planej"
					# "read_json": true
				},
				"epe_lt_exist": {
					"file_name": "EPE Linhas de Transmissão - Base Existente",
					"schema": "externo",
					"table": "epe_lt_exist"
					# "read_json": true
				},
				"epe_subestacao_planej": {
					"file_name": "EPE Subestações - Expansão Planejada",
					"schema": "externo",
					"table": "epe_subestacao_planej"
					# "read_json": true
				},
				"epe_subestacao_exist": {
					"file_name": "EPE Subestações - Base Existente",
					"schema": "externo",
					"table": "epe_subestacao_exist"
					# "read_json": true
				}
			},
			"VEOL": {
				"veol_parque_eolico": {
					"file_name": "VEOL Parque Eólico",
					"schema": "externo",
					"table": "veol_parque_eolico",
					"pk_cols": "objectid"
					# "read_json": true
				},
				"veol_linha_de_trans": {
					"file_name": "VEOL Linha Transmissão",
					"schema": "externo",
					"table": "veol_linha_de_trans",
					"pk_cols": "objectid"
					# "read_json": true
				},
				"veol_regiao_interf": {
					"file_name": "VEOL Região Interf",
					"schema": "externo",
					"table": "veol_regiao_interf",
					"pk_cols": "objectid"
					# "read_json": true
				},
				"veol_subestacao": {
					"file_name": "VEOL Subestação",
					"schema": "externo",
					"table": "veol_subestacao",
					"pk_cols": "objectid"
					# "read_json": true
				},
				"veol_estacao_medicao": {
					"file_name": "VEOL Estação Medição",
					"schema": "externo",
					"table": "veol_estacao_medicao",
					"pk_cols": "objectid"
					# "read_json": true
				},
				"veol_impactados": {
					"file_name": "VEOL Impactados",
					"schema": "externo",
					"table": "veol_impactados",
					"pk_cols": "objectid"
					# "read_json": true
				}
			},
			"SNIRH": {
				"snirh_hidrografia": {
					"file_name": "SNIRH Hidrografia",
					"schema": "externo",
					"table": "snirh_hidrografia"
					# "read_json": true
				}
			},
			"ANM": {
				"anm_proc_minerarios_ativos": {
					"file_name": "ANM Proc. Minerários Ativos",
					"schema": "externo",
					"table": "anm_proc_minerarios_ativos"
					# "read_json": true
				}
			}
		},
		"geoserver": {
			"IPHAN": {
				"iphan_sit_arq_pts": {
					"file_name": "IPHAN Sitios Arq. Pontos",
					"schema": "externo",
					"table": "iphan_sit_arq_pts"
				},
				"iphan_sit_arq_polys": {
					"file_name": "IPHAN Sitios Arq. Polígonos",
					"schema": "externo",
					"table": "iphan_sit_arq_polys"
				}
			},
			"IBGE": {
				"ibge_vegetacao": {
					"file_name": "IBGE Vegetação",
					"schema": "externo",
					"table": "ibge_vegetacao"
				},
				"ibge_pedologia": {
					"file_name": "IBGE Pedologia",
					"schema": "externo",
					"table": "ibge_pedologia"
				}
			}
		}
	}

	return dict_functions

def get_gdrive_folderid(folder:str)->str:
	dic = {
		'00. Python': '1PKK18rrJb8m5PhTjH4VLMfRaaO6Wj1Ri',
		'01. SIGEF': '18jBfGaF15oIfAOE5JqCT5cr1s3FXzBJp',
		'02. CAR': '1u2_WM_8y0-sbJTd0-cVcbYmRdRAShvoH',
		'03. SNCI':'15qJG3SHaNGwO5UMZBj2SFBhrvtfUOweo',
		'04. Assentamento':'1tdW7Z2VPXjeNLM5eb5MAK0BJjqJdE3TV',
		'05. Quilombolas':'1t_YLP1Ap9PqsETZNfVXSSpTCSlc9sySW',
		'06. LTs':'1zbFMVVxi4SfmwDRRj1vYEl2vc1Z-BzVT',
		'07. Subestação':'1znr-64pRPEcC38qnrXj6ICRuseFBdRnP',
		'Ambiental':'1-EZFoa7ClH4k4lKz6m4ZKIx6QkHoVezG',
		'Ambiental - Lic Previa':'1SbSBdbYRagCTakd99A7_6hiTZBgLQ6xX',
		'Ambiental - Lic Implant':'1Scdq03_DioCB2vVTeUHjQ9iqdftdj90v',
		'09. Implantação':'1_L10euiFz69bxrVKFsUoomlb2o2BblcN',
		'Rotas Torres':'19HuswlNqQD0dZQhtAo9rshlbQ5cp4QDA',
		'11. FUNAI':'1xMb7MjVCV7htByv1vAwB0cDXyuHauPll',
		'12. SIGEL':'1XO_eUYG2p_kuVpc-FdoIiGqoTlQcfhwV',
		'13. EPE':'1HgwI1U_fZjejVWyx7j9YpuFt4pBW0pgF' ,
		'Vortex':'1vEMotG8KOOWWQSQSmh4-lcae7LbcCzmn',
		'ICMBIO':'12W7YQNV-VJfkqOUXzy5GpzHQiW2tP9Ju',
		'MMA':'18Mgd8cB3vynkQocUcxup31dQXEnHlDsO',
		'Faixas de Servidão':'1AzlfPocFeVcjAKzcPFuXuTl1-Bq642qa',
		'CDA':'1E-nI3q2I-dqi7hQmKa6LilwoobkpXXa8',
		'Curvas de Nível':'1vNyjRe9bY1DKYnIAm0xvOCVVF5BIZCf4',
		'IBAMA':		  '1vknagkxXcKqEHprUP0aq3HTnazJ4PT4M',
		'Unidades de Conservação Extras': '1RzhIRw3oNvIz0mzX4MYOOnWklcyLMh5_',
		'SAI - Polig. de Aerolevant.':'1Pnw0EdYL8OrMAHXmIjp3C4KZkIfWw2wH'
		# 'Curvas de Níveis':'1HWmJ-l4Q8qhN4Ooq-mPLH_QJzYmLu2Ey'
		
	}
	if folder in dic.keys():
		return dic[folder]
	else :
		return folder

def get_geodataframe_gdrive(
		folder:str, file_name:str, table:str, driveId=None , folderId=None,
		encoding = None, remove_col = None, rename_col_from = None, rename_col_to = None, gdrive_client=None, upload_foldername = False, force_col_order = None, force_int = None,
		download_txt = False, read_json = False, all_columns_to_json=False, filename_to_column=False, schemaData=True, force_print=False
	)->gpd.GeoDataFrame:
		# print('Starting Download of : ' + str(file_name))

		downloads_paths = set(drive_dowloadfile_subfolder(gdrive_client=gdrive_client , folder=folder , file_name=file_name  , force_print=force_print, driveId=driveId, folderId=folderId, download_path=os.path.join('/tmp',folder)))

		file_extension = file_name.rsplit('.',1)[1]
		file_wo_extension = file_name.rsplit('.',1)[0]
		if force_print: print()
		if force_print: print('downloads_paths')
		if force_print: print(downloads_paths)
		if force_print: print()

		gdf = gpd.GeoDataFrame()
		# print('Os.curdir: ' + str(os.curdir))
		for idx, downloads_path in enumerate(downloads_paths):

			downloads_folder = downloads_path.rsplit('/',1)[-1] 
			if file_wo_extension != '*':
				file_name_list = [file_name]
			else:
				file_name_list = [ file_name_row for file_name_row in os.listdir(downloads_path) if file_extension == file_name_row.rsplit('.',1)[1] ]

			if force_print: print()
			if force_print: print('file_name_list')
			if force_print: print(file_name_list)
			if force_print: print()

			for file_name_row in file_name_list:
				if force_print: print()
				if force_print: print(f'file_name_row : {file_name_row}')

				file_path = os.path.join(downloads_path, file_name_row)
				# if force_rpinprint('Reading file : ' + str(file_path))
				# print('Checkin file : ' + str(os.path.exists(file_path)))

				if file_extension in ('kmz','kml'):
					from kmzkml_to_gdf.kmzkml_to_gdf import kmzkml_to_gdf
					gdf_idx = kmzkml_to_gdf( file_path , file_extension=file_extension,  schemaData=schemaData)

				elif file_extension == 'zip':
					if encoding is None:
						gdf_idx = gpd.read_file('zip://' + file_path)
					else:
						gdf_idx = gpd.read_file('zip://' + file_path, encoding = encoding)
					#  print('Zip file read')
					os.remove(file_path)
				else:
					if encoding is None:
						# time.sleep(1)
						gdf_idx = gpd.read_file(file_path)
					else:
						gdf_idx = gpd.read_file(file_path, encoding = encoding)

				gdf_idx = gdf_idx[gdf_idx['geometry'] != None]

				if upload_foldername:
					gdf_idx['folder'] = downloads_folder

				if remove_col is not None:
					gdf_idx = gdf_idx.drop(columns= remove_col)


				if force_col_order is not None:
					gdf_idx = gdf_idx[force_col_order]

				if force_int is not None:
					for col in force_int:
						gdf_idx[col] = gdf_idx[col].astype('Int64')

					gdf_idx = gdf_idx.rename(columns=dict(zip(rename_col_from, rename_col_to)))

				if read_json == True:
					json_file = json.loads(open(os.path.join(downloads_path, file_name.rsplit('.',1)[0] + '.json'), 'r').read())
					for json_i in json_file:
						gdf_idx[table + '_' + json_i] = json_file[json_i]

				gdf_idx.columns = get_clean_list(gdf_idx.columns)
				gdf_idx = gdf_idx.replace({'\t':' ','\n':' ','\r':' '}, regex=True)#.replace('\n','')
				gdf_obj_col = [col for col in gdf_idx.select_dtypes(['object']).columns if str(gdf_idx.loc[:,col].dtype) != 'geometry']
				gdf_idx[gdf_obj_col] = gdf_idx[gdf_obj_col].apply(lambda x: x.str.strip() if isinstance(x,str) else x)

				#gdf_idx.loc[:,col].dtype) == 'geometry'				
				if all_columns_to_json:
					gdf_idx = gdf_columns_to_json(gdf_idx)

				if filename_to_column:
					gdf_idx.loc[:,'filename'] = file_name_row   

				if force_print: print('gdf_idx.columns')
				if force_print: print(gdf_idx.columns)
				if force_print: print()

				
				gdf_idx = gdf_idx.to_crs(epsg=4326)
				gdf = gdf.append(gdf_idx)
				del gdf_idx

		# clean_tmp()
		return gdf

def gdf_columns_to_json(gdf:gpd.GeoDataFrame)->gpd.GeoDataFrame:

	cols_to_dict = [col for col in gdf.columns if str(gdf.loc[:,col].dtype) != 'geometry']
	gdf.loc[:,cols_to_dict] = gdf.loc[:,cols_to_dict].fillna('') 
	gdf.loc[:,'json_columns'] = gdf.loc[:,cols_to_dict].apply(lambda x: {cols_to_dict[x_idx]:x_row for x_idx,x_row in enumerate(x)},axis=1)
	gdf = gdf.drop(columns= cols_to_dict)

	return gdf

def drive_dowloadfile_subfolder(gdrive_client , folder, file_name, folderId=None, download_path = '/tmp', driveId=None, force_print=False):
	
	if force_print: print(f'file_name : [{file_name}]')
	if force_print: print(f'folder : [{folder}]')
	if force_print: print(f'folderId : [{folderId}]')
	if force_print: print(f'driveId : [{driveId}]')

	if driveId is None:
		driveId = '0ALzUMue4j074Uk9PVA'

	if folderId is None :
		folderId = get_gdrive_folderid(folder)

		# os.chdir(folder)

	if not(os.path.exists(download_path)):
		os.mkdir(download_path)


	# elif folder_format =='id':
	# 	folderid = folder

	

	# print(f'folderid : [{folderid}]')

	file_name_wo_extension = file_name.rsplit('.',1)[0]
	file_name_extension = file_name.rsplit('.',1)[1]

	if force_print: print(f'file_name_wo_extension : {file_name_wo_extension}')
	if force_print: print(f'file_name_extension : {file_name_extension}')

	download_path_return = []
	
	q_list = [] 
	q_list.append(f"'{folderId}' in parents")
	q_list.append('trashed=false')

	if file_name_wo_extension !='*':
		q_list.append(f"((name contains '{file_name_wo_extension}') and (mimeType != 'application/vnd.google-apps.folder')) or (mimeType = 'application/vnd.google-apps.folder')")
	else:
		pass 
		# q_list.append(f"((name contains '{file_name_wo_extension}' and mimeType != 'application/vnd.google-apps.folder') or mimeType = 'application/vnd.google-apps.folder')")

	q_list = [f'({q_row})' for q_row in q_list] 
	q = ' and '.join(q_list)
	if force_print: print()

	if force_print: print(f'file_name_wo_extension : [{file_name_wo_extension}]')	
	if force_print: print(f'q : [{q}]')

	gdrivelist = gdrive_client.get_files(
		q = q , 
		driveId=driveId 
	)

	for row in gdrivelist:
		# print()
		row_title = row['name']
		
		if row['mimeType'] == 'application/vnd.google-apps.folder':
			if not(os.path.exists(os.path.join(download_path, row_title))):
				os.mkdir(os.path.join(download_path, row_title))
				
			# os.chdir(row_title)
			download_path_subfolder = drive_dowloadfile_subfolder( gdrive_client=gdrive_client , file_name=file_name, folder=row['id'], folderId=row['id'], download_path=os.path.join(download_path, row_title) , driveId=driveId, force_print=force_print )
			download_path_return = download_path_return + download_path_subfolder
			
		else:
			file_to_download_extension = row_title.rsplit('.',1)[-1]
			# if force_print:  print(f'row_title : [{row_title}]')
			# print(f'row["id"] : [{row["id"]}]')
			if force_print: print(f'''row["name"] : [{row["name"]}] [{row["id"]}]''')

			if file_name_extension  == 'shp':
				if (file_to_download_extension in ['prj','dbf','shx', 'cpg' , 'sbn', 'xml', 'json','sbx','CPG']) and ((file_name_wo_extension + '.' in row_title) or (file_name_wo_extension=='*')):
					gdrive_client.download_file(fileId = row['id'], filename_download=row['name'], path=download_path)

				elif ((file_name == row['name']) or (file_name_wo_extension=='*')) and (file_to_download_extension=='shp'):
					gdrive_client.download_file(fileId = row['id'], filename_download=row['name'], path=download_path)
					download_path_return.append(download_path)

			else: 
				if (file_name == row['name']) or ((file_to_download_extension==file_name_extension) and (file_name_wo_extension=='*')):
					gdrive_client.download_file(fileId = row['id'], filename_download=row['name'], path=download_path)
					download_path_return.append(download_path)

				elif (file_name_wo_extension + '.json' ==  row['name']):
					gdrive_client.download_file(fileId = row['id'], filename_download=row['name'], path=download_path)



	# if folder_format == 'name':
	#	 os.chdir('..')

	return download_path_return
