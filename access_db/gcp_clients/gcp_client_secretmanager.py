import os 
import json 
from google.oauth2 import credentials, service_account 
from google_auth_oauthlib.flow import InstalledAppFlow
from google.cloud import secretmanager
from google.auth.transport.requests import Request
from google.auth import default as default_credentials


def get_creds_secretmanager(keyfile=None , saccount_email:str=None, subject:str=None):
	
	if (saccount_email is not None) and (keyfile is None):
		if 'gcp_client_iamcredentials.py' in os.listdir():
			from gcp_client_iamcredentials import IAMCredentials_Client
		else:
			from .gcp_client_iamcredentials import IAMCredentials_Client
		creds = IAMCredentials_Client().get_service_account_creds(saccount_email=saccount_email, subject=subject)
	
	elif keyfile is None:
		creds, project = default_credentials()
	
	elif isinstance(keyfile,str):
		creds = service_account.Credentials.from_service_account_file(keyfile, subject=subject)
	
	elif isinstance(keyfile,dict):
		creds = service_account.Credentials.from_service_account_info(keyfile, subject=subject)
	
	else:
		raise Exception(f'keyfile type ({type(keyfile)}) is not permitted' )

	return creds 

class SecretManager_Client():
	def __init__(self , credentials=None , saccount_email:str=None , keyfile=None, subject:str=None ):
		'''
		Creates a GCP Secret Manager Client 

		It's possibile to have as project_id defined by enviroment variable GCP_PROJECTID_DEFAULT.
		The logic above applies as well for secret_id and GCP_SECRETID_DEFAULT
		'''
		if credentials is None:
			credentials =  get_creds_secretmanager(keyfile=keyfile , saccount_email=saccount_email, subject=subject)
		self.client_secretmanager = secretmanager.SecretManagerServiceClient(credentials=credentials)
		self.project_id_default = os.getenv("GCP_PROJECTID_DEFAULT")
		self.secret_id_default = os.getenv("GCP_SECRETID_DEFAULT")

	def get_secret(self , secret_id=None, project_id =None, version_id="latest" ):
		# Create the Secret Manager client.

		if project_id is None :
			project_id = self.project_id_default
		if secret_id is None :
			secret_id = self.secret_id_default


		name = f"projects/{project_id}/secrets/{secret_id}/versions/{version_id}"

		
		response = self.client_secretmanager.access_secret_version(name=name)

		return json.loads(response.payload.data.decode('UTF-8'))

	def update_secret(self , new_secret, secret_id=None, project_id =None):
		# Create the Secret Manager client.
		if project_id is None :
			project_id = self.project_id_default
		if secret_id is None :
			secret_id = self.secret_id_default
		
		parent = self.client_secretmanager.secret_path(project_id, secret_id)

		new_secret = json.dumps(new_secret).encode('UTF-8')
		response = self.client_secretmanager.add_secret_version(
			request={"parent": parent, "payload": {"data": new_secret}}
		)
		return response

	def list_secret_versions(self , secret_id=None, project_id=None , return_only_name=True , return_only_enabled = True ):
		# Create the Secret Manager client.
		if project_id is None :
			project_id = self.project_id_default
		if secret_id is None :
			secret_id = self.secret_id_default

		
		parent = self.client_secretmanager.secret_path(project_id, secret_id)

		response = self.client_secretmanager.list_secret_versions(request={"parent": parent})

		# if 

		if return_only_enabled:
			response = [row for row in response if row.state.name=='ENABLED']

		if return_only_name:
			to_return = [row.name for row in response]
		else:
			to_return = response
		# for version in :
		
		return to_return

	def destroy_secret_version(self , secret_id=None, project_id=None , version_id=None):

		if version_id is not None:

			name = f"projects/{project_id}/secrets/{secret_id}/versions/{version_id}"

			# Destroy the secret version.
			response = self.client_secretmanager.destroy_secret_version(request={"name": name})
		else:
			response = 'version_id not declared'
		# print("Destroyed secret version: {}".format(response.name))
		return response
			
	def _get_keyfile_from_installed_keyfile(self, keyfile_dict , scopes):

		if os.path.exists('/tmp/keyfile_temp'):
			os.remove('/tmp/keyfile_temp')

		f = open('/tmp/keyfile_temp.json','w')
		
		f.write(json.dumps(keyfile_dict))
		f.close()
		flow = InstalledAppFlow.from_client_secrets_file('/tmp/keyfile_temp.json', scopes=scopes)
		
		creds = flow.run_local_server(port=0)
		
		keyfile_dict =  json.loads(creds.to_json())
		# keyfile_dict = keyfile_dict['installed'].copy()
		# keyfile_dict['refresh_token'] = creds.refresh_token
		return keyfile_dict

	def get_creds_from_secret(self, scopes, secret_id=None, project_id=None, version_id="latest" ):
		
		keyfile_dict = self.get_secret(secret_id=secret_id , project_id=project_id , version_id=version_id)

		
		if 'type' in keyfile_dict.keys():
			if keyfile_dict['type'] == 'service_account':
				creds = service_account.Credentials.from_service_account_info(keyfile_dict, scopes=scopes)
		else:
			if 'installed' in keyfile_dict.keys():
			
				
				keyfile_dict = self._get_keyfile_from_installed_keyfile(keyfile_dict=keyfile_dict , scopes=scopes)
				
				self.update_secret(new_secret=keyfile_dict , secret_id=secret_id, project_id=project_id )

			creds = credentials.Credentials.from_authorized_user_info(keyfile_dict, scopes=scopes )
			# creds.refresh(Request())

			if (not(creds.valid) and (creds.expired) and (creds.refresh_token)) or (set(scopes) != set(keyfile_dict['scopes'])):
				# print(f'creds.valid 2 {creds.valid}')
				# print(f'creds.expired 2 {creds.expired}')

				# if :
					# print(f'creds.valid 3 {creds.valid}')
					# print(f'creds.refresh_token {creds.refresh_token}')
					# print(f'creds.has_scopes(scopes) [{creds.has_scopes(scopes)}]')

				try:
					# print(f"scopes cred old {json.loads(creds.to_json())['scopes']}")
					# print(f"scopes req {scopes}")

					creds.refresh(Request())
					keyfile_dict = json.loads(creds.to_json())

				except Exception as e:
					print('#### Except ####')
					# print(f'Erro [{e}]')
					new_keyfile_dict = {'installed' : keyfile_dict}
					# print(f'new_keyfile_dict : {new_keyfile_dict}')
					# print(f' type new_keyfile_dict : {type(new_keyfile_dict)}')
					
					keyfile_dict = self._get_keyfile_from_installed_keyfile( keyfile_dict=new_keyfile_dict , scopes=scopes )
					# print('oi')


					self.update_secret(new_secret=keyfile_dict , secret_id=secret_id , project_id=project_id )
		
		return creds

	def get_secret_version(self, secret_id=None , project_id=None , version_id='latest' , return_only_name=True):
		
		if project_id is None :
			project_id = self.project_id_default
		if secret_id is None :
			secret_id = self.secret_id_default

		# client = secretmanager.SecretManagerServiceClient()

		name = f"projects/{project_id}/secrets/{secret_id}/versions/{version_id}"
		
			
		response = self.client_secretmanager.get_secret_version(request={"name": name})

		if return_only_name:
			response = response.name



		return response

	def get_latest_secret_version(self, secret_id=None , project_id=None , return_only_name=True):
		
		response = self.get_secret_version(secret_id=secret_id , project_id=project_id , return_only_name=return_only_name)

		return response

	def destroy_all_but_latest_secret_versions(self, secret_id=None , project_id=None ):
		
		latest_secret_version = self.get_latest_secret_version(secret_id=secret_id , project_id=project_id)

		secret_versions = self.list_secret_versions(secret_id=secret_id , project_id=project_id , return_only_enabled=False )

		to_return = []
		for secret_version in secret_versions:
			if secret_version not in latest_secret_version:
				response = self.client_secretmanager.destroy_secret_version(request={"name": secret_version})
				to_return = to_return + [response]
			# else:
				# print(F'Keep {secret_version}')

		# response = 'oi'
		
		return to_return
