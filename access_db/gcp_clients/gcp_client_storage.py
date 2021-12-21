import pandas as pd
from googleapiclient.discovery import build
from google.oauth2 import service_account
from google.auth import default as default_credentials
from io import  BytesIO
from apiclient import errors
import os
		
#################################################################################################		
#################################################################################################		
#################################################################################################		
#################################################################################################		


def get_creds_storage(keyfile=None , saccount_email:str=None, subject:str=None):
	scopes = ['https://www.googleapis.com/auth/devstorage.full_control']
	
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

class Storage_Client():
	def __init__(self, v='v1' , credentials=None , saccount_email:str=None , keyfile=None, subject:str=None ):
		'''
		Creates a GCP Storage Client
		'''
		if credentials is None:
			credentials =  get_creds_storage(keyfile=keyfile , saccount_email=saccount_email, subject=subject)

		self.credentials = credentials
		self.storage_client = build('storage', v , credentials=credentials)

	###################################################################################################################################################################
	def list_objects(self, bucket, object_keys=None ,**param):
		param['bucket'] = bucket
		
		result = []
		page_token = None
		
			
		print('Starting List')
		while True:
			try:
				if not(page_token is None):
					param['pageToken'] = page_token

				
				objects = self.storage_client.objects().list(**param).execute()
				page_token = objects.get('nextPageToken')
				objects = objects['items']
				
				print('len_objects : ' + str(len(objects)) )
				if object_keys is not None:
					for idx_obj, object in enumerate(objects,0):
						# print(idx_obj)
									# param = {k: v for k, v in param.items() if v}

						objects[idx_obj] = {k:v for k,v in object.items() if k in object_keys}
						# objects[idx_obj] = {(k if k in object_keys):(v if k in object_keys) for k,v in object.items() }
						# obj_dict 
					print('Ending get only object_keys')
					
				
				result.extend(objects)

				
				if page_token is None:
					break
			except errors.HttpError as  error:
				print ('An error occurred: ' + str(error))
				break
		return result
		
	def get_object(self, bucket, object,  **param):
		param['bucket'] = bucket
		param['object'] = object
		
		# page_token = None
		
		object = self.storage_client.objects().get(**param).execute()
		return object

	def get_media(self, bucket, object,  **param):
		param['bucket'] = bucket
		param['object'] = object
		
		
		object = BytesIO(self.storage_client.objects().get_media(**param).execute())
		return object	
