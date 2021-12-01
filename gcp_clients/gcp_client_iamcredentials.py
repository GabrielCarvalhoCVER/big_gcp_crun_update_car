from google.auth import default as default_credentials
from google.oauth2 import credentials 
from googleapiclient.discovery import build
from google.oauth2 import service_account 
import json 
import time 
import requests 

def get_creds_iamcredentials(keyfile=None , saccount_email:str=None, subject:str=None):
    scopes = ['https://www.googleapis.com/auth/iam']

    if (saccount_email is not None) and (keyfile is None):
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

class IAMCredentials_Client():
    def __init__(self, v='v1', credentials=None , saccount_email:str=None,  keyfile=None, subject:str=None ):
        if credentials is None:
            credentials =  get_creds_iamcredentials(keyfile=keyfile , saccount_email=saccount_email, subject=subject)

        self.iamcredentials_client = build('iamcredentials', v , credentials=credentials)
    #################################################################################################        

    def generate_access_token(self , saccount_email , saccount_scopes ,hours=1, delegates=None):
        body = {'scope':saccount_scopes,'lifetime':str(3600*hours)+'s'}
        if delegates is not None:
            body['delegates']=delegates
        name = f'projects/-/serviceAccounts/{saccount_email}'
        
        saccount_access_token = self.iamcredentials_client.projects().serviceAccounts().generateAccessToken(name=name , body=body ).execute()
        return saccount_access_token 

    def generate_access_token_with_subject(self, saccount_email, saccount_scopes, subject):
        url_token_auth = 'https://accounts.google.com/o/oauth2/token'

        name = f'projects/-/serviceAccounts/{saccount_email}'

        jwt = json.dumps(
            {
                "sub": subject,
                "iat": int(time.time()),
                'exp': int(time.time()) + 60*60,
                'scope': ' '.join(saccount_scopes),
                "aud":url_token_auth,
                "iss":saccount_email,
                'email':saccount_email
            }
        )

        jwt_ret = self.iamcredentials_client.projects().serviceAccounts().signJwt(name=name, body={"payload":jwt}).execute()
        data = {
            'grant_type': "urn:ietf:params:oauth:grant-type:jwt-bearer",
            'assertion': jwt_ret['signedJwt']
        }
        headers = {"alg": "RS256", "typ": "JWT", 'kid':jwt_ret['keyId']}
        req = requests.post(url_token_auth,data=data,headers=headers)
        saccount_access_token = req.json()
        
        return saccount_access_token

    def generate_id_token(self , saccount_email , saccount_audience, delegates=None):
        body = {'audience':saccount_audience}
        if delegates is not None:
            body['delegates']=delegates

        name = f'projects/-/serviceAccounts/{saccount_email}'
        
        saccount_id_token = self.iamcredentials_client.projects().serviceAccounts().generateIdToken(name=name , body=body ).execute()
        return saccount_id_token 

    def get_service_account_creds(self , saccount_email , saccount_scopes=None , saccount_audience=None, hours=1, delegates=None , subject=None):
        '''Get credentials of service account. It is necessary Google SDK Installed and logged in'''
        
        if (saccount_scopes is None) and (saccount_audience is None):
            raise Exception('saccount_scopes or saccount_audience should be declared')
        elif (saccount_scopes is not None) and (saccount_audience is not None):
            raise Exception('saccount_scopes or saccount_audience should be declared')
        elif saccount_scopes is not None: 
            if (subject is None):
                saccount_token = self.generate_access_token(saccount_email=saccount_email , saccount_scopes=saccount_scopes , hours=hours, delegates=delegates)['accessToken']
            else:
                saccount_token = self.generate_access_token_with_subject(saccount_email=saccount_email , saccount_scopes=saccount_scopes , subject=subject )['access_token']
        elif saccount_audience is not None: 
            saccount_token = self.generate_id_token(saccount_email=saccount_email , saccount_audience=saccount_audience, delegates=delegates)['token']
        # print()
        saccount_creds = credentials.Credentials(token=saccount_token)

        return saccount_creds 
