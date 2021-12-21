import pandas as pd
import os 
from io import FileIO
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload
from googleapiclient.http import MediaFileUpload
from google.oauth2 import service_account
from google.auth import default as default_credentials
from apiclient import errors
from math import ceil as math_ceil 
from typing import Union
import json

def get_creds_gdrive(keyfile:Union[dict,str,None]=None, saccount_email:Union[str,None]=None, subject:Union[str,None]=None):
	scopes = ['https://www.googleapis.com/auth/drive']
	
	if (saccount_email is not None):
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

#################################################################################################		
#################################################################################################		
#################################################################################################		
#################################################################################################		

class GDrive_Client():
	def __init__(self, v:str='v3', credentials=None , saccount_email:Union[str,None]=None , keyfile:Union[dict,str,None]=None, subject:Union[str,None]=None ):
		if credentials is None:
			credentials =  get_creds_gdrive(keyfile=keyfile , saccount_email=saccount_email, subject=subject)
		self.gdrive_client = build('drive', v , credentials = credentials)  

	#################################################################################################		

	def get_files(self, dict_param:dict={}, force_print:bool=False, **param)->dict:

		result = []
		page_token = None
		if (dict_param != {}) and isinstance(dict_param,dict):
			param = dict_param

		param = {k: v for k, v in param.items() if v}

		if 'driveId' in param.keys():
			param['includeItemsFromAllDrives'] =True
			param['corpora'] ='drive'
			param['supportsAllDrives']=True

		if not('pageSize' in param.keys()):
			param['pageSize'] =1000

		if 'files_fields' in param.keys():
			param['fields'] = "nextPageToken, files(" + param.pop('files_fields') + ")"			
		elif not('fields' in param.keys()):
			param['fields'] = "nextPageToken, files(id,name,driveId,parents,trashed,mimeType,version,size,modifiedTime,createdTime,properties,description,trashed,headRevisionId,md5Checksum)"
		
		
		while True:
			try:
				if not(page_token is None):
					param['pageToken'] = page_token
				if force_print:
					print(json.dumps(param, indent='  '))

				files = self.gdrive_client.files().list(**param).execute()

				result.extend(files['files'])

				page_token = files.get('nextPageToken')
				# print(page_token)
				if page_token is None:
					# print('Fim')
					break
			except errors.HttpError as  error:
				print ('An error occurred: ' + str(error))
				break
			
		return result

	#################################################################################################		

	def get_allfiles_of_parents(self, parents:Union[str,list], force_print:bool=False, return_path:bool=False, **param)->dict:
		
		parents_chunk_size = 400
		if isinstance(parents,str):
			parents=[parents]
			
		len_parents_id = len(parents)
		# print('len_parents_id : '+ str(len_parents_id))
		drivelist=[]
		
		parents_chunk_ns = math_ceil(len_parents_id/parents_chunk_size)
		
		for parents_idx in range(1,1+parents_chunk_ns):
			
			parents_qs_list = parents[(parents_idx-1)*parents_chunk_size:(parents_idx)*parents_chunk_size]
			
			# print('[' + str((parents_idx-1)*parents_chunk_size) + ':'+str((parents_idx)*parents_chunk_size) + ']')
			# print(folders_qs_list)
			parents_qs =  '(' + ' or '.join([f"'{parent_row}' in parents" for parent_row in parents_qs_list]) + ')'
			
			param_q = param.copy()
			if 'q' in param_q.keys():
				param_q['q'] = f"(({param_q['q']}) or (mimeType='application/vnd.google-apps.folder')) and ( {parents_qs})"
			else:
				param_q['q'] = parents_qs
				   
			# print('['+str((parents_idx-1)*parents_chunk_size)+':'+str((parents_idx)*parents_chunk_size)+']')

			# print(f'param_q : {param_q}')
			drivelist_row = self.get_files(force_print=force_print, **param_q)
			
			drivelist = drivelist + drivelist_row

		len_drive = len(drivelist)
		# print('len_drive : '+str(len_drive))
		# parents_paths=[]
		folders_id=[]
	
		for idx_d, row in enumerate(drivelist,1):

			if row['mimeType'] == 'application/vnd.google-apps.folder':

				folders_id = folders_id + [row['id']]

		len_folders_id = len(folders_id)
		
		# print('len_row_folders_id : ' + str(len_folders_id))
		if len_folders_id >0:
			drivelist = drivelist + self.get_allfiles_of_parents(parents=folders_id, force_print=force_print, return_path=return_path, **param)
		
		return drivelist
		
	#################################################################################################		

	def get_df_of_allfiles_of_parents(self, parents:list, columns:list=['id','name','driveId','parents','trashed','mimeType','version','size','modifiedTime','createdTime','properties','description','trashed'], force_print:bool=False, return_path:bool=False, **param)->pd.DataFrame:

		param['fields'] = "nextPageToken, files(" + ','.join(columns) + ")"
				
		drivelist = self.get_allfiles_of_parents(parents=parents, force_print=force_print ,return_path=return_path, **param)
		
		if return_path:
			columns.append('path')

		df_ret = pd.DataFrame(data=drivelist , columns=columns)
		
		return df_ret 

	#################################################################################################		

	def list_subfolders(self, parentId:str, trashed:bool=False , files_fields:str='id,name,parents')->pd.DataFrame:


		driveId = self.get_file(fileId = parentId, fields='driveId')
		q = []
		q = q + ["'" + parentId + "' in parents"]
		q = q + ["mimeType = 'application/vnd.google-apps.folder'"]

		if not(trashed is None):
			q = q + ['trashed=' + str(trashed).lower()]

		q = ' and '.join(q)

		if 'driveId' in driveId.keys():
			return_dict =  self.get_files( q=q , driveId=driveId['driveId'], files_fields=files_fields )
		else:
			return_dict = self.get_files( q=q , files_fields=files_fields )

		return pd.DataFrame.from_dict(return_dict)

	#################################################################################################		

	def get_file(self, fileId:str, **param)->dict:

		param['fileId'] = fileId
		param['supportsAllDrives'] = True
		if not('fields' in param.keys()):
			param['fields'] = "id,name,driveId,parents,trashed,mimeType,version,size,modifiedTime,createdTime,properties,description,trashed,headRevisionId,md5Checksum"

		file = self.gdrive_client.files().get(**param).execute()

		return file

	#################################################################################################		

	def list_permissions(self, fileId:str, **param)->dict:

		param['fileId'] = fileId
		param['supportsAllDrives'] = True
		if not('fields' in param.keys()):
			param['fields'] = "*"

		permissions = self.gdrive_client.permissions().list(**param).execute()

		return permissions

	#################################################################################################		

	def share_file(self, fileId:str, role:str, emailAddress:str, sendNotificationEmail:bool=False, **param)->dict:
		'''role must be writer, commenter, reader'''

		param['fileId'] = fileId
		param['supportsAllDrives'] = True
		param['sendNotificationEmail'] = sendNotificationEmail

		if not('fields' in param.keys()):
			param['fields'] = "*"
		
		body={
			'role':role,
			'type':'user',
			'emailAddress':emailAddress
		}
		param['body'] = body

		shared_file = self.gdrive_client.permissions().create(**param).execute()

		return shared_file

	#################################################################################################		

	def list_revisions(self, fileId:str, **param)->dict:

		param['fileId'] = fileId
		# param['supportsAllDrives'] = True
		if not('fields' in param.keys()):
			param['fields'] = "*"

		revisions = self.gdrive_client.revisions().list(**param).execute()

		return revisions

	#################################################################################################		
	
	def get_first_modifying_user(self, fileId:str)->dict:
		
		revisions = self.list_revisions(fileId=fileId , fields='revisions(modifiedTime, lastModifyingUser(displayName))')		

		min_modifiedTime = min([row['modifiedTime'] for row in revisions['revisions']])
		ModifyingUser = [row['lastModifyingUser']['displayName'] for row in revisions['revisions'] if row['modifiedTime'] == min_modifiedTime][0]

		to_return = {'modifyingUser':ModifyingUser , 'modifiedTime':min_modifiedTime}

		return to_return

	#################################################################################################		

	def get_allparents(self, parentId:str, **param)->dict:

		parents ={}

		if not('fields' in param.keys()):
			param['fields'] = "id,name,parents"

		parent_id_temp = parentId

		while True:
			parent_temp = self.get_file(fileId = parent_id_temp, **param)
			parent_name_temp = parent_temp['name']
			# print(row_parent_id_temp)
			# print(row_parent_temp)
			# print(row_parent_name_temp)
			# print(row_parent_id_temp)
			parents[parent_name_temp] =  parent_id_temp  #=  row_parents + [row_parent_temp]
			if not ('parents' in parent_temp.keys()):
				break

			parent_id_temp = parent_temp['parents'][0]

		return parents

	#################################################################################################		

	def check_parenthood(self, parentId:str, parents_checklist:dict, **param)->dict:

		# parents ={}

		parents_checklist = {v:f for f,v in parents_checklist.items()}

		if not('fields' in param.keys()):
			param['fields'] = "id,name,parents"

		parent_id_temp = parentId

		while True:
			# print('parent_id_temp : ' + parent_id_temp)
			if parent_id_temp in parents_checklist.keys():
				result = parents_checklist[parent_id_temp]
				break
			parent_temp = self.get_file(fileId = parent_id_temp, **param)

			if not ('parents' in parent_temp.keys()):
				result = ''
				break

			parent_id_temp = parent_temp['parents'][0]

		return result

	#################################################################################################		

	def update_file(self, fileId:str, updated_data:dict, **param)->dict:

		param['fileId'] = fileId

		param['body'] = updated_data
		param['supportsAllDrives'] = True


		return self.gdrive_client.files().update(**param).execute()	

	#################################################################################################

	def rename_file(self, fileId:str, old_name_check:str, new_name:str, force_print:bool=False)->dict:

		# param['fileId'] = fileId

		# param['body'] = updated_data
		# param['supportsAllDrives'] = True
		old_name = self.get_file( fileId=fileId , fields = 'name')['name']

		if old_name_check == old_name:

			updated_data = {'name':new_name}

			result_return =  self.update_file( fileId=fileId, updated_data=updated_data)
			if force_print:
				print('Old Name : ' + str(old_name))
				print('New Name : ' + str(new_name))
				print()
			return result_return
		else:
			print('ERROR : Old Name Check does not match')
			print()

	################################################################################################# 
			
	def change_parent_of_file(self, fileId:str, old_parentId_check:str, new_parentId:str, force_print:bool=False)->dict:

		# param['fileId'] = fileId

		# param['body'] = updated_data
		# param['supportsAllDrives'] = True
		updated_data = {}
		# fields = 'parents'
		checks_ok  = True				

		old_file = self.get_file( fileId=fileId , fields = 'parents')

		old_parentId = old_file['parents'][0]

		if old_parentId_check == old_parentId:
			# updated_data['parents'] = [new_parentId]
			pass

		else:
			checks_ok = False
			print('ERROR : Old Parent id Check does not match')
			print()


		if checks_ok:
			result_return = ''
			result_return =  self.update_file( fileId=fileId, updated_data=updated_data , addParents=new_parentId , removeParents=old_parentId )
			if force_print:
				print('Old Parent : ' + str(old_parentId))
				print('New Parent : ' + str(new_parentId))

				print()

			return result_return

	################################################################################################# 

	def copy_file(self, fileId:str, old_parentId_check:str, new_parentId:str, updated_data:dict={}, force_print:bool=False , **param)->dict:

		# param['fileId'] = fileId

		# param['body'] = updated_data
		# param['supportsAllDrives'] = True
		
		# fields = 'parents'
		checks_ok  = True				

		old_file = self.get_file( fileId=fileId , fields = 'parents')	
		old_parentId = old_file['parents'][0]

		if old_parentId_check == old_parentId:
			updated_data['parents'] = [new_parentId]			
		else:
			checks_ok = False
			print('ERROR : Old Parent id Check does not match')
			print()

		if not('fields' in param.keys()):
			param['fields'] = "id,name,driveId,parents,trashed,mimeType,version,size,modifiedTime,createdTime,properties,description,trashed,headRevisionId,md5Checksum"

		if checks_ok:
			# result_return = ''
			param['fileId'] = fileId
			param['body'] = updated_data
			param['supportsAllDrives'] = True

			result_return =  self.gdrive_client.files().copy( **param).execute()
			if force_print:
				print('Old Parent : ' + str(old_parentId))
				print('New Parent : ' + str(new_parentId))

				print()

			return result_return

	#################################################################################################		

	def untrash_file(self, fileId:str)->dict:

		return self.update_file(fileId, updated_data = {'trashed': False})

	#################################################################################################		

	def trash_file(self, fileId:str)->dict:

		return self.update_file(fileId, updated_data = {'trashed': True})	

		#################################################################################################		

	#################################################################################################		

	def download_file(self, fileId:str, filename_download:Union[str,None]=None, path:str='')->str:

		request = self.gdrive_client.files().get_media(fileId=fileId)

		# filename_download = '/tmp/'+old_title
		if filename_download is None:
			filename_download = self.get_file(fileId=fileId, fields = 'name')['name']
		# print(f'download_file : filename_download {[filename_download]}')

		# print(filename_download)
		filename_download = os.path.join(path, filename_download)
		fh = FileIO(filename_download, "wb")

		downloader = MediaIoBaseDownload(fh, request)

		done = False
		while done is False:
			status, done = downloader.next_chunk()
		
		return filename_download

	#################################################################################################		

	def update_media(self, fileId:str , media_path:str, update_name:bool=False, **param)->dict:
		
		media_body = MediaFileUpload(
			media_path, 
			resumable=True
		)

		# param = {}
		print(f'param inicial {param}')
		param['fileId'] = fileId
		param['media_body'] = media_body

		body={}

		if update_name:
			print(f'media_path {media_path}')
			
			if '/' in media_path:
				new_name = media_path.rsplit('/',1)[1]
			else: 
				new_name = media_path

			body = {
				'name':new_name
			}
		
		param['updated_data']=body

		
		updated_file = self.update_file(**param)

		return updated_file

	#################################################################################################		

	def copy_media(self, fileId:str, fileId_to_copy:str, update_name:bool=False , **param)->dict:
		
		media_path = self.download_file(fileId=fileId_to_copy, path='/tmp' )
		
		copied_file = self.update_media(fileId=fileId , media_path=media_path, update_name=update_name, **param )

		os.remove(media_path)

		return copied_file

	#################################################################################################		

	def get_bytes_str_of_file(self, fileId:str)->str:

		self.download_file( fileId=fileId , path='/tmp' , filename_download=fileId)
		gdrive_str_bytes = open(os.path.join('/tmp', fileId), 'rb').read()
		os.remove(os.path.join( '/tmp' , fileId))

		return gdrive_str_bytes

	#################################################################################################		

	def get_str_of_file(self, fileId:str, encoding:str=None)->str:

		self.download_file( fileId=fileId , path='/tmp' , filename_download=fileId)
		gdrive_str = open(os.path.join('/tmp', fileId), 'r' , encoding=encoding).read()
		os.remove(os.path.join( '/tmp' , fileId))

		return gdrive_str

	#################################################################################################		

	def create_folder(self, folder_name:str, parentId:str, driveId:Union[str,None]=None)->dict:

		body = {'name' : folder_name, 'mimeType' : 'application/vnd.google-apps.folder', 'parents' : [parentId]}

		if not(driveId is None) :
			body['driveId'] = driveId


		# print(body)
		folder = self.gdrive_client.files().create( body=body,  supportsAllDrives=True).execute()

		return  folder

	#################################################################################################		

	def createget_folder(self, driveId:str, folder_name:str, parentId:str)->dict:

		drive_list = self.get_files(
			q = "'" + parentId + "' in parents and trashed=false and name = '" + folder_name + "'", 
			driveId= driveId
		)

		if len(drive_list) > 1:
			print('Múltiplas Pastas existem')
			folder = drive_list[0]
		elif len(drive_list) == 1:
			folder = drive_list[0]
		else:
			folder = self.create_folder(folder_name =  folder_name, parentId = parentId , driveId = driveId)

		return  folder

	#################################################################################################		

	def get_changes(self, dict_param:dict={}, **param)->dict:
		'''
		pageToken= str , driveId= str
		'''

		if (dict_param != {}) and isinstance(dict_param,dict):
			param = dict_param

		param = {k: v for k, v in param.items() if v}

		if 'driveId' in param.keys():
			# print('p_driveid : ' + param['driveId'])
			param['includeItemsFromAllDrives'] =True
			# param['corpora'] ='drive'
			param['supportsAllDrives']=True
		# else:
		#	 print('driveId nulo')
		#	 pass
		#	 param['restrictToMyDrive']=True

		if not('pageSize' in param.keys()):
			param['pageSize'] =1000

		if not('fields' in param.keys()):
			param['fields'] = "newStartPageToken, nextPageToken, changes(kind, type, changeType, removed, time, fileId,  file(id,name,driveId,parents,trashed,mimeType,version,size,modifiedTime,createdTime,properties,description,headRevisionId,md5Checksum))"
		result = []
		page_token = None
		newStartPageToken = None
		while True:
			try:
				if not(page_token is None):
					param['pageToken'] = page_token

				changes = self.gdrive_client.changes().list(**param).execute()
				# print(changes)
				result.extend(changes['changes'])
				page_token = changes.get('nextPageToken')
				newStartPageToken = changes.get('newStartPageToken')

				if page_token is None:
					break
			except errors.HttpError as  error:
				print ('An error occurred: ' + str(error))
				break
		return newStartPageToken, result

	#################################################################################################		

	def get_StartPageToken(self, driveId:Union[str,None]=None )->dict:
		"""
		driveId = None
		Colocar driveId se for relativo a um drive
		"""

		try:
			# param_stpgtk = {}
			param = {}

			if driveId is not None:
				param['driveId']=driveId
				# param['includeItemsFromAllDrives'] =True,
				# param['corpora'] ='drive'
				param['supportsAllDrives'] = True

			param['fields'] = 'startPageToken'

			# for col, val in param.items():
			#	 if col in ('driveId','fields','supportsAllDrives'):
			#		 param_stpgtk[col] = val
			# if 
			StartPageToken = self.gdrive_client.changes().getStartPageToken(**param).execute()['startPageToken']

		except errors.HttpError as  error:
			print ('An error occurred: ' + str(error))

		return StartPageToken

	#################################################################################################		
