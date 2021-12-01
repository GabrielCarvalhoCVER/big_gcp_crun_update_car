import requests

def geoserver_request(url:str, service:str='WFS', version:str='2.0.0', outputFormat:str='json', request:str='getFeature',  **params)->requests.models.Response:

    param = {}
    param['service'] = service
    param['version'] = version
    param['request'] = request
    param['outputFormat'] = outputFormat
    
    if 'typeName' in params.keys():
        param['typeName'] = params.pop('typeName')
        
    if 'layers' in params.keys():
        param['layers'] = params.pop('layers')
    
    param.update(params)

    req = requests.post(url, data=param)
    
    return req

def geoserver_api_seed(url:str, layer:str, login:str, password:str, payloadtype:str, zoomStart:int= 0, zoomStop:int=25, gridSetId:str = 'EPSG:900913', tileFormat:str='image/png', minX:str='', minY:str='', maxX:str='', maxY:str='', threadCount:int=1)->requests.models.Response:
    api_entry = url + layer
    credential = (login , password )
    '''payloadType truncate,seed,reseed'''

    headers = {"Content-type":"application/x-www-form-urlencoded"}

    payload = {
        'threadCount': threadCount
        ,'type': payloadtype
        ,'gridSetId': gridSetId
        ,'tileFormat': tileFormat 
        ,'zoomStart': zoomStart
        ,'zoomStop': zoomStop
        ,'minX': minX
        ,'minY': minY
        ,'maxX': maxX 
        ,'maxY': maxY 
        ,'tileFailureRetryCount': -1
        ,'tileFailureRetryWaitTime': 100
        ,'totalFailuresBeforeAborting': 1000 
    }

    r = requests.post(
        api_entry,
        data=payload,
        headers=headers,
        auth=credential
    )
    
    return r

def geoserver_api_check_threads(url:str, layer:str, login:str, password:str)->dict:
    
    credential = (login , password )
    '''payloadType truncate,seed,reseed'''

    headers = {"Content-type":"application/x-www-form-urlencoded"}

    r_threads = requests.get(
        url + layer + '.json' , 
        headers=headers, 
        auth=credential
    ).json()

    return r_threads
