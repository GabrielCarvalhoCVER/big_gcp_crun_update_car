import pandas as pd
from googleapiclient.discovery import build
from google.oauth2 import service_account
from google.auth import default as default_credentials
import os
		
#################################################################################################		
#################################################################################################		
#################################################################################################		
#################################################################################################		


def get_creds_gsheet(keyfile=None , saccount_email:str=None, subject:str=None):
	scopes = ['https://www.googleapis.com/auth/drive' , 'https://www.googleapis.com/auth/spreadsheets']
	
	if (saccount_email is not None) and (keyfile is None):
		if 'gcp_client_iamcredentials.py' in os.listdir():
			from gcp_client_iamcredentials import IAMCredentials_Client
		else:
			from .gcp_client_iamcredentials import IAMCredentials_Client
		creds = IAMCredentials_Client().get_service_account_creds(saccount_email=saccount_email, saccount_scopes=scopes, subject=subject)

	elif keyfile is None:
		creds, project = default_credentials(scopes=scopes)
	 
	elif isinstance(keyfile,str):
		creds = service_account.Credentials.from_service_account_file(keyfile, scopes=scopes, subject=subject)
	
	elif isinstance(keyfile,dict):
		creds = service_account.Credentials.from_service_account_info(keyfile, scopes=scopes, subject=subject)
	
	else:
		raise Exception(f'keyfile type ({type(keyfile)}) is not permitted' )

	return creds

class GSheet_Client():
	def __init__(self, v='v4' , credentials=None , saccount_email:str=None , keyfile=None, subject:str=None ):
		'''
		Creates a GCP Google Sheet Client
		'''
		if credentials is None:
			credentials =  get_creds_gsheet(keyfile=keyfile , saccount_email=saccount_email, subject=subject)
		self.keyfile = keyfile
		self.credentials = credentials
		self.gsheet_client = build('sheets', v , credentials = credentials ).spreadsheets() 

	########################################################################################################################################################################
	
	def get_gsheetId_x_title(self, spreadsheetId):
		spreadsheet = self.gsheet_client.get(spreadsheetId=spreadsheetId ,  fields = 'sheets(properties(sheetId,title))').execute()
		dict_gsheet_title_x_sheetid = {row['properties']['sheetId']:row['properties']['title'] for row in spreadsheet['sheets']}
		return dict_gsheet_title_x_sheetid
 
	########################################################################################################################################################################
	
	def get_title_x_gsheetId(self, spreadsheetId):
		spreadsheet = self.gsheet_client.get(spreadsheetId=spreadsheetId ,  fields = 'sheets(properties(sheetId,title))').execute()
		dict_gsheet_title_x_sheetid = {row['properties']['title']:row['properties']['sheetId'] for row in spreadsheet['sheets']}
		return dict_gsheet_title_x_sheetid

	########################################################################################################################################################################
   
	def _get_title_x_gridProperties(self, spreadsheetId):
		spreadsheet = self.gsheet_client.get(spreadsheetId=spreadsheetId,  fields = 'sheets(properties(title,gridProperties))').execute()
		dict_gsheet_title_x_gridProperties = {row['properties']['title']:row['properties']['gridProperties'] for row in spreadsheet['sheets']}
		
		return dict_gsheet_title_x_gridProperties

	########################################################################################################################################################################
	
	def _get_gridProperties_of_title(self, spreadsheetId, title):
		gridProperties = self._get_title_x_gridProperties(spreadsheetId=spreadsheetId)
		if not(title in gridProperties.keys()):
				print(title + ' sheet does not in spreadsheet')
		
		gridProperties = gridProperties[title]
		
		return gridProperties

	########################################################################################################################################################################
	
	def _get_gridProperties_of_titles(self, spreadsheetId, titles):
		gridProperties = self._get_title_x_gridProperties(spreadsheetId=spreadsheetId)
		for title in titles:
			if not(title in gridProperties.keys()):
					print(title + ' sheet does not in spreadsheet')
			   
		gridProperties = {k:v  for k,v in gridProperties.items() if k in titles}
		
		return gridProperties

	########################################################################################################################################################################
	
	def get_ranges_of_titles(self, spreadsheetId, titles, ranges):
		if ranges==[]:
			gridProperties = self._get_gridProperties_of_titles(spreadsheetId=spreadsheetId, titles=titles)
			ranges = {title:'R1C1:R'+str(gridProperties[title]['rowCount'])+'C'+str(gridProperties[title]['columnCount']) for title in titles}
		else:
			pass
		
		return ranges

	########################################################################################################################################################################

	def get_range_of_title(self, spreadsheetId, title, range):
		if range=='':
			gridProperties = self._get_gridProperties_of_title(spreadsheetId=spreadsheetId, title=title)
			range = 'R1C1:R'+str(gridProperties['rowCount'])+'C'+str(gridProperties['columnCount'])
		else:
			pass
		
		return range

	########################################################################################################################################################################
	
	def get_title_of_gsheetId(self, spreadsheetId, gsheetId):
		
		dict_title = self.get_gsheetId_x_title(spreadsheetId=spreadsheetId)
		
		if not(gsheetId in dict_title.keys()):
				print(str(gsheetId) + ' gsheetId does not in spreadsheet')
			   
		title = dict_title[gsheetId]
		
		return title

	########################################################################################################################################################################

	def get_gsheetId_of_title(self, spreadsheetId, title ):
		

		dict_title = self.get_gsheetId_x_title(spreadsheetId=spreadsheetId)

		dict_gsheetId = {v:k for k,v in dict_title.items()}

		
		if not(title in dict_gsheetId.keys()):
				print(str(title) + ' title does not in spreadsheet')
		
		# print(2.4)
		gsheetId = dict_gsheetId[title]
		print(gsheetId)
		return gsheetId

	########################################################################################################################################################################

	def get_sheet_by_id(self, spreadsheetId, gsheetId, range='', majorDimension='ROWS' , skipRows=0 ):
		
		title = self.get_title_of_gsheetId(spreadsheetId=spreadsheetId, gsheetId=gsheetId , skipRows=skipRows )
		
		return self.get_sheet_by_title(spreadsheetId=spreadsheetId , title=title, range=range, majorDimension=majorDimension)

	########################################################################################################################################################################
		
	def get_sheet_by_title(self, spreadsheetId, title, range='', majorDimension='ROWS' , skipRows=0 ):
				
		range = self.get_range_of_title(spreadsheetId=spreadsheetId, title=title, range=range)

		dict_sheet = self.gsheet_client.values().get(spreadsheetId=spreadsheetId , range=title + '!' + range , majorDimension=majorDimension ).execute()
	
		gsheet_list_values = dict_sheet['values'] 
		
		max_len_row = max([len(row) for row in  gsheet_list_values])
		

		for idx, row in enumerate(gsheet_list_values,0) :
			while len(gsheet_list_values[idx]) < max_len_row:
				gsheet_list_values[idx] = gsheet_list_values[idx] + ['']

		df_gsheet = pd.DataFrame( columns=gsheet_list_values[skipRows], data=gsheet_list_values[skipRows+1:])
		
	
		return df_gsheet

	########################################################################################################################################################################
	
	def get_sheets_by_titles(self, spreadsheetId, titles, ranges=[], majorDimension='ROWS' , skipRows=0 ):
		
		ranges = self.get_ranges_of_titles(spreadsheetId=spreadsheetId, titles=titles, ranges=ranges)

		dict_sheets = self.gsheet_client.values().batchGet(spreadsheetId=spreadsheetId , ranges=["'"+title + "'!" + ranges[title] for title in titles] , majorDimension=majorDimension ).execute()

		gsheet_list_values = dict_sheets['valueRanges'] 
		gsheet_list_values

		dict_ret = {}
		for l1 in gsheet_list_values:
			sheet_name =  l1['range'].split('!')[0]
		#	 print(sheet)
			if "'" == sheet_name[0]:
				sheet_name = sheet_name[1:-1].replace("''","'")

			gsheet_list_values = l1['values'] 

			max_len_row = max([len(row) for row in  gsheet_list_values])


			for idx, row in enumerate(gsheet_list_values,0) :
				while len(gsheet_list_values[idx]) < max_len_row:
					gsheet_list_values[idx] = gsheet_list_values[idx] + ['']

			dict_ret[sheet_name] = pd.DataFrame( columns=gsheet_list_values[skipRows], data=gsheet_list_values[skipRows+1:])
		
		return dict_ret

	########################################################################################################################################################################
	
	def clear_by_id(self, spreadsheetId, gsheetId, range=''):
		title = self.get_title_of_gsheetId(spreadsheetId=spreadsheetId, gsheetId=gsheetId)
		return self.clear_by_title(spreadsheetId=spreadsheetId , title=title , range=range)

	########################################################################################################################################################################

	def clear_by_title(self, spreadsheetId, title, range=''):
		
		range = self.get_range_of_title(spreadsheetId=spreadsheetId, title=title, range=range)
		
		return self.gsheet_client.values().clear(spreadsheetId=spreadsheetId , range=title + '!' + range).execute()

	########################################################################################################################################################################
	
	def update_sheet_by_id(self, spreadsheetId, gsheetId, body,  range='' , valueInputOption='USER_ENTERED' , clear_gsheet=False):
		title = self.get_title_of_gsheetId(spreadsheetId=spreadsheetId,  gsheetId=gsheetId)
		return self.update_sheet_by_title(spreadsheetId=spreadsheetId , title=title , body=body , range=range, valueInputOption=valueInputOption , clear_gsheet=clear_gsheet)

	########################################################################################################################################################################

	def update_sheet_by_title(self, spreadsheetId, title, body, range='', valueInputOption='USER_ENTERED', clear_gsheet=False):
		
		if range=='':
			len_col = max([len(row) for row in  body['values']])
			len_row = len(body['values'])
			range = 'R1C1:R' + str(len_row) + 'C' + str(len_col)

		if clear_gsheet:
			self.clear_by_title( spreadsheetId=spreadsheetId, title=title)
		
		return self.gsheet_client.values().update(spreadsheetId=spreadsheetId , body=body,  range= title+'!'+range, valueInputOption=valueInputOption).execute()

	########################################################################################################################################################################
		
	def update_cell_by_id(self, spreadsheetId, gsheetId, row, column, value , valueInputOption='USER_ENTERED' ):
		
		title = self.get_title_of_gsheetId(spreadsheetId=spreadsheetId,  gsheetId=gsheetId)
		return self.update_cell_by_title(spreadsheetId=spreadsheetId , title=title , row=row , column=column , value=value,  valueInputOption=valueInputOption )

	########################################################################################################################################################################

	def update_cell_by_title(self, spreadsheetId, title, row , column , value , valueInputOption='USER_ENTERED'):
			
		range = 'R'+str(row)+'C'+str(column)
		body = {
			'majorDimension': "ROWS",
			'values':[[value]]
		}

		
		return self.gsheet_client.values().update(spreadsheetId=spreadsheetId , body=body,  range= title+'!'+range, valueInputOption=valueInputOption).execute()

	########################################################################################################################################################################

	def update_sheet_by_id_with_df(self, spreadsheetId, gsheetId, df,  range='' , valueInputOption='USER_ENTERED' , method='Simple Copy'):
		'''
		acceptable method = ['Simple Copy','Truncate', 'Simple Append' , 'Append']
		'''
		
		if method not in ['Simple Copy','Truncate', 'Simple Append' , 'Append']:
			raise Exception('Method not defined')
		
		title = self.get_title_of_gsheetId(spreadsheetId=spreadsheetId,  gsheetId=gsheetId)
		return self.update_sheet_by_title_with_df(spreadsheetId=spreadsheetId , title=title , df=df , range=range, valueInputOption=valueInputOption , method=method)

	########################################################################################################################################################################

	def update_sheet_by_title_with_df(self, spreadsheetId, title, df, range='', valueInputOption='USER_ENTERED', method='Simple Copy'):
		'''
		acceptable method = ['Simple Copy','Truncate', 'Simple Append' , 'Append']
		'''
		
		if method not in ['Simple Copy','Truncate', 'Simple Append' , 'Append']:
			raise Exception('Method not defined')

		df = df.fillna('')
		
		if method == 'Append':
			df_gsheet = self.get_sheet_by_title(spreadsheetId=spreadsheetId, title=title)
			set_df_cols = set(df.columns)
			set_gsheet_cols = set(df_gsheet.columns)
			
			if set_df_cols != set_gsheet_cols:
				raise Exception(f'''Columns names don''t match
				gsheet cols : {set_gsheet_cols}
				df cols : {set_df_cols}
				''')


			

			# pass

		for col in df.columns:
			if str(df[col].dtype)[:8]=='datetime':
				df[col] = df[col].astype(str)

		body = {
			'majorDimension':'ROWS',
			'values':[df.columns.values.tolist()] + df.values.tolist()
		}

		check_existence = self.check_sheet_existence(spreadsheetId=spreadsheetId , title=title)

		if not(check_existence):
			self.create_sheet(spreadsheetId=spreadsheetId , title=title)

		if method=='Truncate':
			self.clear_sheets_by_titles(spreadsheetId=spreadsheetId, titles=[title] )
			
		return self.update_sheet_by_title( spreadsheetId=spreadsheetId , title=title , body=body, range=range , valueInputOption=valueInputOption)

	########################################################################################################################################################################

	def update_sheets_by_titles_with_dfs(self, spreadsheetId, data:list , valueInputOption='USER_ENTERED' , majorDimension='ROWS',  method='Simple Copy' , force_print=False):
		'''
		acceptable method = ['Simple Copy','Truncate', 'Simple Append' , 'Append']
		data = [{'title':title1, 'df':df1, 'range':a1:g100},{'title':title2, 'df':df2}] range is optional
		'''
		if method not in ['Simple Copy','Truncate', 'Simple Append' , 'Append']:
			raise Exception('Method not defined')
		
		# title_x_gridProperties = self.get_title_x_gridProperties(spreadsheetId=spreadsheetId)
		gsheetId_x_title = {}
		data_input = []
		titles = []
		for row in data:
			if force_print: print(f'update_sheets_by_titles_with_dfs - Starting {row["title"]}')
			# row2 = { k:v for k,v in row.items() if v}
			
			title = row['title']

			titles.append(title)
				
			if force_print: print(f'update_sheets_by_titles_with_dfs - getting list')

			df = row['df'].fillna('')
			
			for col in df.columns:
				if str(df[col].dtype)[:8]=='datetime':
					df[col] = df[col].astype(str)

			values = [df.columns.values.tolist()] + df.values.tolist()
			
			if ('majorDimension' in row.keys()):
				majorDimension_input = row['majorDimension']
			else:
				majorDimension_input = majorDimension
			
			if force_print: print(f'update_sheets_by_titles_with_dfs - getting range')

			if not('range' in row.keys()):
				len_col = max([len(row_v) for row_v in values])
				len_row = len(values)
				range = 'R1C1:R' + str(len_row) + 'C' + str(len_col)
			
			if force_print: print(f'update_sheets_by_titles_with_dfs - creating data input')
			data_input =  data_input + [{ 'range':title+'!'+range , 'majorDimension':majorDimension_input , 'values':values }]
		
		if force_print: print(f'update_sheets_by_titles_with_dfs - creating body')
		body = {
			'valueInputOption': valueInputOption,
			'data': data_input
			}

		check_existences = self.check_sheets_existences(spreadsheetId=spreadsheetId , titles=titles)

		for check_title , check_existence in  check_existences.items():
			if not(check_existence):
				self.create_sheet(spreadsheetId=spreadsheetId , title=check_title)

		if method=='Truncate':
			if force_print: print('Starting Truncate')
			self.clear_sheets_by_titles(spreadsheetId=spreadsheetId, titles=titles  )
		
		if force_print: print(f'update_sheets_by_titles_with_dfs - starting batchupdate')

		return self.gsheet_client.values().batchUpdate( spreadsheetId=spreadsheetId , body=body).execute()

	########################################################################################################################################################################
	
	def _clear_sheets(self, spreadsheetId:str, data:dict, range:str=''):
		
		gsheetId_x_title = {}
		data_input = []
		# get_title_x_gridProperties
		title_x_gridProperties = {}

		for row in data:
			# print('row : ' +str(row))
			
			if 'title' in row.keys():
				title = row['title']
			elif 'gsheetId' in row.keys:
				if gsheetId_x_title == {}:
					gsheetId_x_title = self.get_gsheetId_x_title(spreadsheetId=spreadsheetId)
				title = gsheetId_x_title[row['gsheetId']]
			
			# print(title)
			# print(title_x_gridProperties)
			
			# self.get_range_input(spreadsheetId=spreadsheetId , title=title , )
			if range != '':
				# print(1)
				range_input=range
			elif 'range' in row.keys():
				# print(2)
				range_input = row['range']
			else:
				# print(3)
				# print(title_x_gridProperties)
				if title_x_gridProperties == {}:
					title_x_gridProperties = self._get_title_x_gridProperties(spreadsheetId=spreadsheetId)
					# print('oi')
			
				# print(title_x_gridProperties)
				gridProperties = title_x_gridProperties[title]
				range_input = 'R1C1:R'+str(gridProperties['rowCount'])+'C'+str(gridProperties['columnCount'])
				
			data_input =  data_input + [title+'!'+range_input]
		
		# print(data_input)
		body = {'ranges':data_input}
		
		return self.gsheet_client.values().batchClear(spreadsheetId=spreadsheetId , body=body).execute()
			
	########################################################################################################################################################################

	def clear_sheets_by_titles(self, spreadsheetId:str, titles:list):
		
		data=[]
		for title in titles:
			data = data + [{'title':title}]
			
		return self._clear_sheets( spreadsheetId=spreadsheetId, data=data )
	
	########################################################################################################################################################################
	
	def _batchUpdate(self, spreadsheetId:str ,  requests:dict):
		body = {
			'requests': requests
		}
		return self.gsheet_client.batchUpdate(spreadsheetId=spreadsheetId, body=body).execute()

	########################################################################################################################################################################

	def _batchGet(self, spreadsheetId:str ,  ranges:list, majorDimension='ROWS', valueInputOption='USER_ENTERED' , dateTimeRenderOption='SERIAL_NUMBER'):

		return self.gsheet_client.batchGet(spreadsheetId=spreadsheetId, ranges=ranges , majorDimension=majorDimension , valueInputOption=valueInputOption , dateTimeRenderOption=dateTimeRenderOption ).execute()

	########################################################################################################################################################################
	
	
	def get_list_titles(self, spreadsheetId:str):
		spreadsheet = self.gsheet_client.get(spreadsheetId=spreadsheetId ,  fields = 'sheets(properties(title))').execute()

		list_titles = [row['properties']['title'] for row in spreadsheet['sheets']]
		return list_titles

	########################################################################################################################################################################

	def check_sheet_existence(self, spreadsheetId:str, title:str):
		
		list_titles = self.get_list_titles(spreadsheetId=spreadsheetId)

		to_return = title in list_titles

		return to_return
	
	########################################################################################################################################################################
	
	def check_sheets_existences(self, spreadsheetId:str, titles:list):
		
		list_titles = self.get_list_titles(spreadsheetId=spreadsheetId)

		to_return = {title:(title in list_titles) for title in titles}

		return to_return

	########################################################################################################################################################################
	
	def create_sheet(self, spreadsheetId:str, title:str):
		requests = {
			"addSheet":{
				'properties':{
					'title':title
				}
			}
		}
		self._batchUpdate(spreadsheetId=spreadsheetId , requests=requests)

	########################################################################################################################################################################

	def delete_sheet(self, spreadsheetId:str, title:str):
		
		gsheetId = self.get_gsheetId_of_title(spreadsheetId=spreadsheetId , title=title )

		requests = {
			"deleteSheet":{
				'sheetId':gsheetId
			}
		}
		self._batchUpdate(spreadsheetId=spreadsheetId , requests=requests)

	########################################################################################################################################################################

	def rename_title(self, spreadsheetId:str , old_title:str , new_title:str):
		# print(2)
		gsheetId = self.get_gsheetId_of_title(spreadsheetId=spreadsheetId , title=old_title )
		# print(3)
		requests = {
			"updateSheetProperties": {
				"properties": {
					"sheetId": gsheetId,
					"title": new_title,
				},
				"fields": "title",
			}
		}

		return self._batchUpdate( spreadsheetId=spreadsheetId , requests=requests )

	########################################################################################################################################################################

	def protect_sheet(self, spreadsheetId:str , title:str , users:list):
		gsheetId = self.get_gsheetId_of_title(spreadsheetId=spreadsheetId , title=title )

		try:
			users.append(self.credentials.service_account_email)
		except:
			pass

		requests = {
			'addProtectedRange':{
				'protectedRange':{
					'range':{'sheetId':gsheetId},
					'editors':{'users':users}
				}
			}
		}

		return self._batchUpdate( spreadsheetId=spreadsheetId , requests=requests )

	########################################################################################################################################################################

	def get_protected_ranges(self, spreadsheetId:str , titles:list ):

		protected_ranges = self.gsheet_client.get(spreadsheetId=spreadsheetId , fields='sheets(properties(title),protectedRanges)' , ranges=titles).execute()

		protected_ranges = {row['properties']['title']:row.get('protectedRanges',None) for row in protected_ranges['sheets']} 

		return protected_ranges

	########################################################################################################################################################################

	def protect_sheets(self, spreadsheetId:str , titles:list , users:list):
		title_x_gsheetId = self.get_title_x_gsheetId(spreadsheetId=spreadsheetId)

		try:
			users.append(self.credentials.service_account_email)
		except:
			pass 

		to_return = {}
		for title in titles:

			gsheetId = title_x_gsheetId[title]

			requests = {
				'addProtectedRange':{
					'protectedRange':{
						'range':{'sheetId':gsheetId},
						'editors':{'users':users}
					}
				}
			}
			to_return[title] = self._batchUpdate( spreadsheetId=spreadsheetId , requests=requests )


		return to_return

	########################################################################################################################################################################

	def create_spreadsheet(self,  name:str , parentId:str, title:str='Sheet1', df:pd.DataFrame()=None ):
		gdrive_client = build('drive', 'v3' , credentials = self.credentials)

		body = {
			'mimeType': 'application/vnd.google-apps.spreadsheet',
			'name': name,
			'parents':[parentId]
		}
		f_created = gdrive_client.files().create(body=body, supportsAllDrives=True).execute()
		
		spreadsheetId = f_created['id']
		# print(0)
		if title != 'Sheet1':
			# print(1)
			self.rename_title(spreadsheetId=spreadsheetId, old_title='Sheet1' , new_title=title)
		
		if df is not None:
			self.update_sheet_by_title_with_df(spreadsheetId=spreadsheetId , title=title , df=df)
		
				
		return f_created

	########################################################################################################################################################################
