from google.oauth2 import service_account
from googleapiclient.discovery import build
from google.auth import default as default_credentials
import os
		
#################################################################################################		
#################################################################################################		
#################################################################################################		
#################################################################################################		

def get_creds_people(keyfile=None, saccount_email:str=None , secret=None , subject:str=None ):

	scopes = ['https://www.googleapis.com/auth/contacts.readonly']
	if (saccount_email is not None):
		if 'gcp_client_iamcredentials.py' in os.listdir():
			from gcp_client_iamcredentials import IAMCredentials_Client
		else:
			from .gcp_client_iamcredentials import IAMCredentials_Client
		creds = IAMCredentials_Client().get_service_account_creds(saccount_email=saccount_email, saccount_scopes=scopes)

	elif (secret is not None):
		from .gcp_client_secretmanager import SecretManager_Client
		if isinstance(secret, str):
			secret_id = secret
			project_id =None
		elif isinstance(secret, dict):
			secret_id = secret.get('secret_id')
			project_id = secret.get('project_id')
		else: 
			raise Exception(f'secret type ({type(secret)}) is not permitted' )

		creds = SecretManager_Client().get_creds_from_secret(scopes=scopes, secret_id=secret_id , project_id=project_id)

	elif keyfile is None:
		creds, project = default_credentials(scopes=scopes)
	
	elif isinstance(keyfile,str):
		creds = service_account.Credentials.from_service_account_file(keyfile, scopes=scopes, subject=subject )
	
	elif isinstance(keyfile,dict):
		creds = service_account.Credentials.from_service_account_info(keyfile, scopes=scopes, subject=subject )
	
	else:
		raise Exception(f'keyfile type ({type(keyfile)}) is not permitted' )

	return creds

class People_Client():
	def __init__(self,  v='v1', saccount_email:str=None , credentials=None , secret=None, keyfile=None, subject:str=None ):
		if credentials is None:
			credentials =  get_creds_people(keyfile=keyfile , saccount_email=saccount_email , secret=secret, subject=subject)
		self.keyfile = keyfile
		self.credentials = credentials
		self.people_client = build('people', v , credentials=credentials )
	
	def list_people(self, personFields='names'):

		to_return = self.people_client.people().connections().list(resourceName='people/me', personFields=personFields).execute()

		return to_return