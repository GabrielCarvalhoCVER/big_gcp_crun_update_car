from google.oauth2 import service_account
from googleapiclient.discovery import build
from google.auth import default as default_credentials
import base64
from email.mime.text import MIMEText
import os 
        
#################################################################################################        
#################################################################################################        
#################################################################################################        
#################################################################################################        

def get_creds_gmail(keyfile=None, saccount_email:str=None , secret=None  ):
    scopes = ['https://mail.google.com/']
    
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
            project_id = None
        elif isinstance(secret, dict):
            secret_id = secret.get('secret_id')
            project_id = secret.get('project_id')
        else: 
            raise Exception(f'secret type ({type(secret)}) is not permitted' )

        creds = SecretManager_Client().get_creds_from_secret(scopes=scopes, secret_id=secret_id , project_id=project_id)

    elif keyfile is None:
        creds, project = default_credentials(scopes=scopes)
    
    elif isinstance(keyfile,str):
        creds = service_account.Credentials.from_service_account_file(keyfile, scopes=scopes )
    
    elif isinstance(keyfile,dict):
        creds = service_account.Credentials.from_service_account_info(keyfile, scopes=scopes )
    
    else:
        raise Exception(f'keyfile type ({type(keyfile)}) is not permitted' )

    return creds

class GMail_Client():
    def __init__(self, keyfile=None, v='v1', saccount_email:str=None , credentials=None , secret=None):
        if credentials is None:
            credentials =  get_creds_gmail(keyfile , saccount_email=saccount_email , secret=secret)
        self.keyfile = keyfile
        self.credentials = credentials
        self.gmail_client = build('gmail', v , credentials = credentials ).users()

    ########################################################################################################################################################################
    
    def send_email(self , to , message , subject=None , cc=None , userId='me'):

        msg = MIMEText(message)
        msg ['to'] = to
        
        if cc is not None:
            msg ['cc'] = cc

        if subject is not None:
            msg ['subject'] = subject
        

        raw = base64.urlsafe_b64encode(msg.as_bytes())

        body = {'raw': raw.decode('UTF-8')}
        


        return self.gmail_client.messages().send(userId=userId,body=body).execute()