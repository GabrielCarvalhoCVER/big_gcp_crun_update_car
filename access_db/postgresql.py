
try:
	from access_db.access_db import *
except:
	from github.access_db.access_db import *


################################################################################################################################################################################################

def do_trunc_or_create_table(df:Union[pd.DataFrame,gpd.GeoDataFrame], table:str, schema:str, engrawconn_db=None , db_json:Union[dict,None]=None, db_env:Union[str,None]=None , db_secret:Union[dict,None]=None , truncate=False , updateIdValues=[] , updateColumnName='' , pk_cols:Union[None,str,list]=None , force_clean_columns:bool=False):

	engrawconn_db, engrawconn_db_none  = get_eng_and_rawconn(engrawconn_db=engrawconn_db , db_json=db_json , db_env=db_env, db_secret=db_secret)
	engine_db, rawconn_db = engrawconn_db
	cur_db = rawconn_db.cursor()
	
	# print('f0')]

	# print('before check_table_exists')
	table_exists = check_table_exists(table=table, schema=schema, engrawconn_db = engrawconn_db)
	# print('schema.table : ' + str(schema)+'.'+str(table))
	# print('table_exists : ' + str(table_exists))
	
	if not(table_exists):
		
		create_table(df=df , schema=schema, table=table , pk_cols=pk_cols , force_clean_columns=force_clean_columns)

	# :ALTER table externo.epe_gasoduto_transp_upload ALTER COLUMN geometry TYPE geometry(geometryz,4326) USING st_force3d(geometry);

				
	elif truncate :
		cur_db.execute("truncate " + schema + "." + table)
		rawconn_db.commit() 
	
	elif updateColumnName != '' :
		idValues_type = list_type(updateIdValues)
		if idValues_type in ['int','float']:
			cur_db.execute(
			"""
			delete from """ + schema + "." + table + """ 
			where """ + '"' + updateColumnName + '"' + " in " + "( " + (',').join([str(val) for val in updateIdValues]) + """ )
			""")
		elif idValues_type == 'str':
			cur_db.execute(
			"""
			delete from """ + schema + "." + table + """ 
			where """ + '"' + updateColumnName + '"' + " in " + "( " + (',').join(["'" + (val) + "'" for val in updateIdValues]) + """ )
			""")
			
		rawconn_db.commit()
	
	else:
		pass
	
	close_rawconn_and_disp_eng(engrawconn_db=engrawconn_db, engrawconn_db_none=engrawconn_db_none )

def create_table(df:Union[pd.DataFrame,gpd.GeoDataFrame], schema:str, table:str, engrawconn_db=None, db_json:Union[dict,None]=None, db_env:Union[str,None]=None , db_secret:Union[dict,None]=None , pk_cols:Union[None,str,list]=None, force_clean_columns:bool=True):
	
	engrawconn_db, engrawconn_db_none  = get_eng_and_rawconn(engrawconn_db=engrawconn_db , db_json=db_json , db_env=db_env, db_secret=db_secret)
	engine_db, rawconn_db = engrawconn_db
	cur_db = rawconn_db.cursor()
 
	# print('df.columns')
	# print(list(df.columns))
	if force_clean_columns: 
		df.columns = get_clean_list(df.columns)
	# print(list(df.columns))

	if isinstance(df, gpd.GeoDataFrame) or isinstance(df, pd.DataFrame):
			# df.head(0).to_postgis(table, engine_db, if_exists='replace',index=False, schema = schema)
		sep = '\n,\t'
		create_table_query = f'''create table {schema}.{table} (\n\t{ sep.join([ '"' + col + '"' +' ' + get_dbtype_from_dftype(df[col]) + ' ' for col in df.columns])} \n)'''
		# create_table_query = f'create table ' + schema + '.' + table + '(\n\t' + '\n,\t'.join([ '"' + col + '"' +' ' + get_dbtype_from_dftype(df[col]) + ' ' for col in df.columns]) + '\n)'
		print(create_table_query)
		cur_db.execute(create_table_query)
		rawconn_db.commit()

	# elif isinstance(df, pd.DataFrame):
	#		 # df.head(0).to_sql(table, engine_db, if_exists='replace',index=False, schema = schema)

	#	 create_table_query = 'create table ' + schema + '.' + table + '(\n\t' + '\n,\t'.join([ '"' +col +'"' + ' ' + get_dbtype_from_dftype(df[col]) + ' null' for col in df.columns]) + '\n)'
	#	 print(create_table_query)
	#	 cur_db.execute(create_table_query)
	#	 rawconn_db.commit()
	
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

	for col in df.columns:
		
		if (str(df[col].dtype)=='geometry'): 
			# print('df.geometry.has_z : ' + str(df.geometry.has_z))
			has_z = False
			for row_df in df[col].has_z:
				if row_df:
					haz_z = True
					# print('break of has_z')
					break
			
			if (has_z):
				print('has_z')
				cur_db.execute('ALTER TABLE '+schema+'.'+table+' ALTER COLUMN '+col+' TYPE geometry(geometryz,4326) USING st_force3d(geometry);')
				rawconn_db.commit()
				
	close_rawconn_and_disp_eng(engrawconn_db=engrawconn_db, engrawconn_db_none=engrawconn_db_none )
	
def upsert_db(gdf:Union[pd.DataFrame,gpd.GeoDataFrame], schema:str, table:str, engrawconn_db =None, db_json:Union[dict,None]=None, db_env:Union[str,None]=None , db_secret:Union[dict,None]=None , force_print:bool=False):
	# table_updatetime = str(datetime.now((pytz.timezone('America/Sao_Paulo'))))[:19]
	dict_dtype = {
		'object':'text',
		'int64':'int8',
		'int32':'int4',
		'int16':'int2',
		'float64':'numeric',
		'float32':'numeric',
		'float16':'numeric',
		'bool':'boolean',
		'datetime64[ns]':'timestamp',
		'datetime64[ns, america/sao_paulo]':'timestamptz'
		
	}
	
	col_db_type = [dict_dtype[str(dt).lower()] for dt in gdf.reset_index().dtypes]
	
	# gdf.index = ['"' +col + "'"  for col in gdf.columns]
	
	engrawconn_db, engrawconn_db_none  = get_eng_and_rawconn(engrawconn_db=engrawconn_db , db_json=db_json , db_env=db_env, db_secret=db_secret)
	engine_db, rawconn_db = engrawconn_db
	
	do_trunc_or_create_table(df=gdf.reset_index(), table=table , schema=schema , engrawconn_db=engrawconn_db , truncate=False )
	
	cur_db = rawconn_db.cursor()
	gdf.columns = [f'"{col}"' for col in gdf.columns]
	gdf.index = gdf.index.rename(f'"{gdf.index.name}"')
	# print('),\n\t\t('.join([','.join(["'"+str(val)+"'::"+col_db_type[idx] for  idx,val in enumerate(row)]) for ind, row in gdf.reset_index().iterrows()]) )
	bt = '\t'
	bn = '\n'

	sql_str = f"""
		INSERT INTO {schema}.{table} ( {gdf.index.name} , {','.join(gdf.columns)} ) 
		select * from (values{bn}{bt}{bt} ( {f'),{bn}{bt}{bt}('.join([','.join([ f"'{str(val)}'::{col_db_type[idx]}" for idx,val in enumerate(row)]) for ind, row in gdf.reset_index().iterrows()])} )
		) as t({gdf.index.name} , {','.join(gdf.columns)} )
		on conflict ({gdf.index.name}) do update 
		set 
		{bt} {f',{bn}{bt}{bt}'.join(gdf.columns + ' = excluded.' + gdf.columns)} 
		;
	"""
	
	if force_print: print()
	if force_print: print(table)
	if force_print: print(sql_str)
	if force_print: print()
	
	if force_print: print('Upsert Before Execute')
	cur_db.execute(sql_str)
	if force_print: print('Upsert Before Commit')
	rawconn_db.commit()
	if force_print: print('Upsert After Commit')
	if force_print: print()
	
	close_rawconn_and_disp_eng(engrawconn_db=engrawconn_db, engrawconn_db_none=engrawconn_db_none )

def get_dbtype_from_dftype (df_series): 
	
	dtype = str(df_series.dtype).lower()
	
	dict_dtype = {
		'object':							['text',		'not null'], 
		'int64':							 ['int8',		'null'], 
		'int32':							 ['int4',		'null'], 
		'int16':							 ['int2',		'null'],  
		'float64':						   ['numeric',	 'null'], 
		'float32':						   ['numeric',	 'null'],  
		'float16':						   ['numeric',	 'null'], 
		'bool':							  ['boolean',	 'null'], 
		'datetime64[ns]':					['timestamp',   'null'], 
		'datetime64[ns, america/sao_paulo]': ['timestamptz', 'null']		
	}
	
	if dtype == 'geometry' :
		srid = str(df_series.crs.to_epsg())
		ndim = max([get_ndim(geom) for geom in df_series])
		if ndim == 3:
			db_type = 'geometry(geometryz,' + srid + ') null'
		elif ndim == 2:
			db_type = 'geometry(geometry,' + srid + ') null'
	
	else:
		db_type = dict_dtype[dtype][0] + ' ' + dict_dtype[dtype][1]
		
	return db_type

def update_db(gdf:Union[pd.DataFrame,gpd.GeoDataFrame], schema:str, table:str, truncate:bool=True , crud:bool = False, crud_function:Union[str,None] = None, engrawconn_db =None, force_update:bool=True , db_json:Union[dict,None]=None, db_env:Union[str,None]=None , db_secret:Union[dict,None]=None , updateIdValues:list=[] , updateColumnName:str='' , pk_cols:Union[None,str,list]=None , force_print:bool=False , force_geometry3d:bool=False):
	
	if force_print: print(schema+'.'+table + ' - Starting update_db')

	if gdf is None:
		# print('gdf is None')
		len_gdf = 0
	else:
		# print('len gdf')
		len_gdf = len(gdf)

	# print('inicio 1')
		
	# len_gdf=len(gdf)
	
	if gdf is None:
		msg_upload = 'not executed (none)'
		msg_crud = 'not executed'

		return [msg_upload, msg_crud]
		
	elif not(len_gdf>0 or force_update):
		msg_upload = 'not executed (empty)'
		msg_crud = 'not executed'

		return [msg_upload, msg_crud]
	else:
		if force_print: print(f'{schema}.{table} - Start Upload [len:{len(gdf)}]')
		
		# print('inicio 2')
		table_updatetime = get_now_sp()
		# table_updatetime = str(datetime.now((pytz.timezone('America/Sao_Paulo'))))[:19]

		engrawconn_db, engrawconn_db_none  = get_eng_and_rawconn(engrawconn_db=engrawconn_db , db_json=db_json , db_env=db_env, db_secret=db_secret)
		# print(0)
		engine_db, rawconn_db = engrawconn_db
		# print(1)
		cur_db = rawconn_db.cursor()
		# print(2)

		try:
			do_trunc_or_create_table(df=gdf, table=table, schema=schema, engrawconn_db=engrawconn_db, truncate=truncate , updateIdValues=updateIdValues , updateColumnName=updateColumnName , pk_cols=pk_cols)

			gdf = gdf.reset_index(drop=True)
			gdf.columns = get_clean_list(gdf.columns)
			
			# print(0)			
			# if isinstance(gdf, gpd.GeoDataFrame):
			#	 for col_df in gdf.columns: 
			#		 if str(gdf.loc[:,col_df].dtype) == 'geometry':
			#			 if force_print: print(f'{schema}.{table} - Starting - {col_df} from geometry to wkt text')
			#			 srid = str(gdf.loc[:, col_df].crs.to_epsg())
			#			 gdf.loc[: , col_df] = gdf.loc[ : , col_df].apply(lambda x: 'SRID=' + srid  + '; ' + str(x) if str(x) != 'None' else 'null')
			#			 if force_print: print(f'{schema}.{table} - End - {col_df} from geometry to wkt text')
					# print('geom_cols_x_srid : ' + str(geom_cols_x_srid))
			# gdf = pd.DataFrame(gdf)

			if force_print: print(schema+'.'+table + ' - Starting Get df db ')
			df_db = pd.read_sql(sql='select * from '+schema+'.'+table+' limit 0', con=engine_db)
			df_db_col = get_clean_list(df_db.columns)

			if set(df_db_col) != set(gdf.columns):

				col_inters = set.intersection(set(df_db_col), set(gdf.columns))
				print('cols diferentes')
				print('cols no db : ' + str(set(df_db_col)))
				print('cols no df : ' + str(set(gdf.columns)))
				cur_db.execute("drop table " + schema + "."+ table) 
				rawconn_db.commit()
				create_table(df=gdf , schema=schema, table=table , pk_cols=pk_cols)
			else:
				gdf = gdf[df_db_col]

					# msg_upload = 'not executed - cols dif'
					# msg_crud = 'not executed'

					# return [msg_upload, msg_crud]

			# print('cols no db : ' + str(df_db_col))
			# print('cols no df : ' + str(gdf.columns))

			if force_print: print(schema+'.'+table + ' - Starting Get Object Columns')
			gdf_obj_col = gdf.select_dtypes(['object']).columns
			gdf_obj_col = [col for col in gdf_obj_col if str(gdf[col].dtype) != 'geometry']
			# if str(df_chunk.loc[:,col_df].dtype) == 'geometry':

			if force_print: print(schema+'.'+table + ' - Starting fillna Object Columns')
			gdf.loc[: , gdf_obj_col] = gdf[gdf_obj_col].fillna('').copy()
			
			# gdf[gdf_obj_col] = gdf[gdf_obj_col].replace(to_replace= {'\n':'\\\\n' , '\r':'\\\\r' , '\t':'\\\\t'} , regex=True )
			# gdf[gdf_obj_col] = gdf[gdf_obj_col].replace(to_replace= {'\n':r'\n' , '\r':r'\r' , '\t':r'\t', '\\':'\\\\\'} , regex=True )
			# sizekb_max = 16

			sizelen_max = 30*(10**5)
			# sizekb_acum = 0
			sizelen_acum = 0
			idx_ini = 0
			gdf_n_cols = range( 1 , 1 + len(gdf.columns))
			if force_print: print(schema+'.'+table + ' - Starting Table Upload')
			# print_mem_use()
			if force_print: print(f'force_geometry3d {force_geometry3d}')

			for idx, row in enumerate(gdf.itertuples(),1):

				sizelen_of_row = sum([len(str(row[gdf_n_col])) for gdf_n_col in gdf_n_cols])
				# print(str(row)[:100])


				if (sizelen_acum + sizelen_of_row > sizelen_max) or (idx == len(gdf)):
					if force_print: print(f'{schema}.{table} - Upload {idx} of {len_gdf}')

					update_db_output(gdf.iloc[idx_ini:idx].copy(), schema=schema, table=table, engrawconn_db = engrawconn_db , force_print=force_print , force_geometry3d=force_geometry3d)

					idx_ini = idx
					# sizekb_acum = sizekb_of_row
					sizelen_acum = sizelen_of_row
				else:
					# sizekb_acum = sizekb_acum + sizekb_of_row
					sizelen_acum = sizelen_acum + sizelen_of_row


			msg_upload = 'executed'


			df_tb_updt_time = pd.DataFrame(columns=['table_name' , 'update_time'], data=[[ table , table_updatetime ]]).set_index('table_name')
			
			if force_print: print(schema+'.'+table + ' - Starting Table Updatetime')

			upsert_db(gdf=df_tb_updt_time , schema=schema , table='table_updatetime' , engrawconn_db=engrawconn_db , force_print=force_print)

				# print(9)
			if(crud  and (crud_function is not  None) ):
				try:
					if force_print: print(schema+'.'+table + ' - Starting CRUD')

					cur_db.execute("select * from " + schema + "." + crud_function + "()")
					rawconn_db.commit()
					msg_crud = 'executed'

					if force_print: print(schema+'.'+table + ' - Starting CRUD Updatetime')
					cur_db.execute(
						"""
						INSERT INTO """ + schema + """.table_updatetime 
						select * from (values('""" + crud_function + "'::text, '" + str(table_updatetime) + """'::timestamp)) as t(table_name, update_time)
						on conflict (table_name) do update 
						set 
							table_name = excluded.table_name,
							update_time = excluded.update_time
						;
						""")
					if force_print: print(schema+'.'+table + 'Before commit')
					rawconn_db.commit()

					if force_print: print(schema+'.'+table + ' - End CRUD Update time')

				except Exception as e1:
					rawconn_db.rollback()
					msg_crud = 'error - ' + str(e1)
			else:
				msg_crud = 'not executed'

		except Exception as e2:
			# print(10)

			rawconn_db.rollback()

			msg_upload = 'error - ' + str(e2)
			msg_crud = 'not executed'
			print()
			print(msg_upload)

			close_rawconn_and_disp_eng(engrawconn_db=engrawconn_db, engrawconn_db_none=engrawconn_db_none )

		return [msg_upload, msg_crud]

def update_db_output(df_chunk:Union[pd.DataFrame,gpd.GeoDataFrame], schema:str, table:str, engrawconn_db , force_print:bool=False , force_geometry3d:bool=False):

	engine_db, rawconn_db = engrawconn_db
		
	cur_db = rawconn_db.cursor()
	
	dict_sep = {'\t':'\\t', '|':'|', ',':',', '\r':'\\r'}
	# dict_sep = {'\t':'\\t', '|':'|', ',':',', '\r':'\\r'}
	
	update_db_success = False
	erro_upload = ''

	if isinstance(df_chunk, gpd.GeoDataFrame):
		for col_df in df_chunk.columns: 
			# print(str(df_chunk.loc[:,col_df].dtype)
			if str(df_chunk.loc[:,col_df].dtype) == 'geometry':
				# if force_print: print(f'{schema}.{table} - Starting - {col_df} from geometry to wkt text')
				srid = str(df_chunk.loc[:, col_df].crs.to_epsg())

				df_chunk.loc[: , col_df] = df_chunk.loc[: , col_df].apply(lambda x: x if str(x)=='None' else (x if x.is_valid else make_valid(x))).copy()

				if force_geometry3d:
					df_chunk.loc[: , col_df] = df_chunk.loc[ : , col_df].apply(lambda x: st_force3d_asewkt(x ,srid )).copy()
				else:
					df_chunk.loc[: , col_df] = df_chunk.loc[ : , col_df].apply(lambda x: st_asewkt(x ,srid )).copy()

				# if force_print: print(f'{schema}.{table} - End - {col_df} from geometry to wkt text')

	df_chunk = pd.DataFrame(df_chunk.copy())
	df_chunk = df_chunk.replace(to_replace= {'\n':'\\\\n' , '\r':'\\\\r' , '\t':'\\\\t', r'\\':r'\\\\'} , regex=True ).copy()
	null_rep = 'null'

	if schema is None:
		schematable = f'"{table}"'
	else:
		schematable = f'"{schema}"."{table}"'

	for sep, sep_str in dict_sep.items():
		try:
			output = StringIO()
			df_chunk.to_csv(output, sep=sep , header=False, index=False , na_rep=null_rep  )
			output.seek(0)
			# contents = output.getvalue()
			sql_str = f'''COPY {schematable} FROM STDIN WITH NULL '{null_rep}' DELIMITER E{repr(sep)} '''
			# print(sql_str)
			# print(contents)
			cur_db.copy_expert(sql_str, output)  # null values become ''
			# cur_db.copy_from(output, schema + "." + table, null=null_rep , sep=sep  ) # null values become ''
			rawconn_db.commit()
			update_db_success=True
			del output 
			break
		except Exception as e1:
			rawconn_db.rollback()
			erro_upload = e1
			
			print(f'tryed with [{sep_str}] | msg error e1 : {e1}')
			
	if not(update_db_success):
		raise Exception(f'Erro no upload :[{erro_upload}]')

	del df_chunk

	return update_db_success

def get_table_lastupdatetime(schema:str, table_name:str, rawconn_db):
	cur_db = rawconn_db.cursor()
	# table_name = 'dro_parque_eolico_upload'
	cur_db.execute("SELECT update_time from " + schema + ".table_updatetime where table_name = '" + table_name + "';")
	table_updatetime = cur_db.fetchall()
		
	if len(table_updatetime) > 0:
		table_updatetime = (table_updatetime[0][0])
	else:   
		None
	# print(table_updatetime)
	# print(type(table_updatetime))
	return table_updatetime
