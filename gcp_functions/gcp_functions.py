import os
import shutil
import json
import base64
import sys
from google.oauth2 import service_account
from google.auth.transport.requests import AuthorizedSession
from numpy.lib.arraysetops import isin
import pytz
from datetime import datetime, timedelta
import requests
from unidecode import unidecode as unaccent
import urllib
import re
import numpy as np


def gdrive_time_to_datetime(gdrive_time, timezone=-3):
    '''timezone default -3 referrring to SP'''
    datetetime_ = datetime.strptime(gdrive_time.replace('T', ' ')[:-1], '%Y-%m-%d %H:%M:%S.%f') + timedelta(hours = -timezone)
    return datetetime_

def list_type(list):
    n_str = 0
    n_int = 0
    n_float = 0
    for row in list:
        if isinstance(row,str):
            n_str = n_str +1
        elif isinstance(row,int):
            n_int = n_int +1
        elif isinstance(row,float):
            n_float = n_float  +1
        else:
            raise Exception(f'Tipo {type(row)} Não Identificado')
            
        
    if n_str > 0 and (n_int ==0 and n_float==0):
        return 'str'
    elif n_float > 0 and (n_str ==0):
        return 'float'
    elif n_int > 0 and (n_str ==0 and n_float==0):
        return 'int'
    else:
        return 'nd'

def get_cols_unique(cols):
    new_cols = []
    dict_rename_cols_count={}
    for col in cols:
        if list(cols).count(col)>1:
            count = dict_rename_cols_count.get(col,1)

            # dict_rename_cols[col] = col+'_'+str(count)
            new_col = col+'_'+str(count)
            dict_rename_cols_count[col]=count+1
        else: 
            new_col = col
        new_cols.append(new_col)
    return new_cols

def get_clean_list(list):
    return [get_clean_str(row) for row in list]

def get_clean_str(str):
    return multiple_replace(unaccent(str).lower(),{' ':'_' , '\n':'_' , '\r':'_', '\t':'_','__':'_'})

def multiple_replace(text, param):
    new_text = text
    for r1,r2 in param.items():
        new_text = new_text.replace(r1,r2)
        
    return new_text

def windows_path_to_linux_path(windows_path):
    
    linux_path = windows_path.split('\\')
    
    for idx_f, folder in enumerate(linux_path,0):
        if (folder[-1:] == ':') and (idx_f==0) :
            linux_path[idx_f] = '/mnt/' + folder.replace(':','').lower()
    
    linux_path = '/'.join(linux_path)
    
    return linux_path

def deg_min_sec_to_deg(coord):
    # print(coord)
    coord = coord.replace(' ','')
    coord = coord.replace(',','.')
    coord = coord.replace('’',"'")
    coord = coord.replace("''",'"')
    coord = coord.replace("”",'"')
    # print(coord)
    deg, minutes, seconds, direction =  re.split('[°\'"]', coord)
    
    deg = (float(deg) + float(minutes)/60 + float(seconds)/(60*60)) * (-1 if direction.upper() in ['W','O', 'S'] else 1)    
    return deg

def coalesce(*arg):
  for el in arg:
    if el is not None:
      return el
  return None

def check_substrings(substrings, a_string):
    return any([substring in a_string for substring in substrings])

def get_tags_of_str(str_x, return_int=False):
    bracket_1 = str_x.find('[')
    bracket_2 = str_x.find(']')

    tags = []
    str_x_wotags = str_x
    date_tag = ''
    
    
    n_int = 0
    int_tag=None
    
    while bracket_1 >= 0 and  bracket_2 >= 0 and bracket_2 > bracket_1:    
        tag_tmp = str_x_wotags[bracket_1+1:bracket_2]
        tag_tmp = tag_tmp.replace(',',';')
        if (tag_tmp[:3] == 'ANO') and (len(tag_tmp)==7) and (tag_tmp[3:].isdigit()):
            now_year = get_now_sp().year
            if (int(tag_tmp[3:])<=now_year+1) and (int(tag_tmp[3:])>=1900):
                date_tag = tag_tmp
            else:
                tags = tags + [tag_tmp]
                if tag_tmp.isdigit():
                    n_int = n_int+1
                    int_tag = tag_tmp
                    
        elif check_datetime(tag_tmp, '%d.%m.%Y'):
            date_tag = datetime.strptime(tag_tmp, '%d.%m.%Y')
        else:
            tags = tags + [tag_tmp]
            if tag_tmp.isdigit():
                n_int = n_int+1
                int_tag = int(tag_tmp)
        
        

        str_x_wotags = str_x_wotags.replace('[' + tag_tmp + ']', '')

        str_x_wotags = str_x_wotags.lstrip()
        bracket_1 = str_x_wotags.find('[')
        bracket_2 = str_x_wotags.find(']')

        str_x_wotags = str_x_wotags.strip()
    
    if n_int >1:
        int_tag=None
        
    # print('str_x : [' + str_x + ']')
    # print('date_tag : [' + str(date_tag) + ']')
    if return_int:
        return str_x_wotags, tags, date_tag, int_tag
    else:
        return str_x_wotags, tags, date_tag

def get_now_sp(): 
    return datetime.strptime(str(datetime.now((pytz.timezone('America/Sao_Paulo')))).rsplit('-',1)[0],'%Y-%m-%d %H:%M:%S.%f')

def get_datetime_str(var_dt = None):
    if var_dt == None: var_dt = get_now_sp()
        
    return str(var_dt)[:19]

def print_request(request=None):
    try:
        try:
            print('form() ' + str(request.form()))
        except:
            print('form ' + str(request.form))
        try:
            print('get_json() ' + str(request.get_json()) + ' || type: ' + str(type(request.get_json())))
        except:
            print('get_json ' + str(request.get_json))
        try:
            print('json() ' + str(request.json()) + ' || type: ' + str(type(request.json())))
        except:
            print('json ' + str(request.json))
        try:
            print('environ() ' + str(request.environ()))
        except:
            print('environ ' + str(request.environ))
        try:
            print('get_data() ' + str(request.get_data()) + ' || type: ' + str(type(request.get_data())))
        except:
            print('get_data ' + str(request.get_data)) 
        try:
            print('data() ' + str(request.data()) + ' || type: ' + str(type(request.data())))
        except:
            print('data ' + str(request.data))
        try:
            print('args() ' + str(request.args()))
        except:
            print('args ' + str(request.args))
    except:
        pass

def print_listdir():
    listdir1 = os.listdir('/')
    print('listdir1 : '+str(listdir1))
    print('listdir2')
    for ls1 in listdir1:
        print()
        print(ls1)
        try:
            print(os.listdir('/' + ls1))
        except Exception as e2:
            print('error : '+ str(e2))

def request_to_json(request=None , force_print=False):
    if force_print: print(f'request {request}')
    if force_print: print(f'request type {type(request)}')
    if isinstance(request, dict):
        if force_print: print(f'request keys {request.keys()}')
        # print('request is dict')
        req_json = request
        if (len(req_json) == 1) and ('data' in req_json.keys()):
            req_json = req_json['data']
    else:
        # print('request is not dict')
        req_json = request.get_json()
        if force_print: print(f'req_json keys {req_json.keys()}')
        if 'message' in req_json.keys():
            req_json = base64.b64decode(req_json['message']['data']).decode('utf-8')
            req_json = json.loads(req_json)
        elif (len(req_json) == 1) and ('data' in req_json.keys()):
            req_json = req_json['data']
 
    
    return req_json

# def json_to_df_layers_alrdeady_runned_list(req_json=None):
    
    # if req_json == None:
    #     layers_already_runned_list = []
    
    # elif 'layers_already_runned' in req_json.keys():
    #     layers_already_runned_str = req_json['layers_already_runned']
        
    # else:
    #     df = pd.DataFrame()
        
    #         layers_already_runned_str = req_json
            
    #     layers_already_runned_list = [i.strip().lower() for i in layers_already_runned_str.split(',')]    
            
    # return layers_already_runned_list
    
def clean_tmp():
    dir = '/tmp'
    for files in os.listdir(dir):
        path = os.path.join(dir, files)
        try:
            shutil.rmtree(path)
        except OSError:
            try:
                os.remove(path)
            except:
                pass
    
def on_gcp():
    # return True
    # return str(os.path.expanduser('~')) == '/tmp'
    return os.uname()[1] =='localhost'

def call_gcp_function(API_URL,  data = {}, timeout=1, keyfile=None , credentials=None):

    audience = API_URL

    if credentials is None:

        if isinstance(keyfile,str):
            credentials = service_account.IDTokenCredentials.from_service_account_file(keyfile , target_audience=audience )
        
        elif isinstance(keyfile,dict):
            credentials = service_account.IDTokenCredentials.from_service_account_info(keyfile , target_audience=audience )

        else:
            raise Exception(f'keyfile type ({type(keyfile)}) is not permitted')

    session = AuthorizedSession(credentials)

    try:
        return session.post(API_URL, json=data, timeout=timeout)
    except Exception as e:
        pass

def sizeof_fmt(num, suffix='B'):
    ''' by Fred Cirera,  https://stackoverflow.com/a/1094933/1870254, modified'''
    for unit in ['','Ki','Mi','Gi','Ti','Pi','Ei','Zi']:
        if abs(num) < 1024.0:
            return "%3.1f %s%s" % (num, unit, suffix)
        num /= 1024.0
    return "%.1f %s%s" % (num, 'Yi', suffix)

def print_mem_use():
    size_total_mem = [sys.getsizeof(value)/1024/1024 for name,value in globals().items()]
    #     print(sys.getsizeof(value)/1024)
    print('###########################')
    print('total mem use : ' + str(round(sum(size_total_mem),3)) + ' MB')
    print()
    for name, size in sorted(((name, sys.getsizeof(value)) for name, value in globals().items()),key= lambda x: -x[1])[:100]:
        print("{:>30}: {:>8}".format(name, sizeof_fmt(size)))
    print('###########################')

def print_vars():

    import pandas as pd 
    import numpy as np

    items = list(globals().items()) + list(locals().items())

    size = [sys.getsizeof(obj)/1024 for var, obj in items]
    var = [var for var, obj in items]
    vars = pd.DataFrame({'var':var, 'size':size}, index= np.arange(1, len(var) + 1)).sort_values('size', ascending= False)

    print('Print Vars')
    print('total_memory : '+ str(vars['size'].sum()))
    print(vars.head())
    print()
        
def check_datetime(str_date, format_date):
    try:
        datetime.strptime(str_date, format_date)
        return True
    except:
        return False

def force_date_func(str_date, list_format_date=['%d/%m/%Y','%d/%m/%y', '%d/%m/%Y %H:%M:%S' , '%d/%m/%y %H:%M:%S' ,'%-d/%-m/%y'] , replace_backslashs=True):
    
    str_date = str_date.strip()

    if replace_backslashs:
        str_date = str_date.replace('\\','/')

    if isinstance(list_format_date,str):
        list_format_date = [list_format_date]

    for format_date in list_format_date:
        if check_datetime(str_date=str_date , format_date=format_date):
            return  datetime.strptime(str_date , format_date)   
    
    if str_date != '':
        pass
        # print(f'not able to convert :{str_date}')
    
    return np.datetime64('NaT')

def ireplace(text, old, new):
    idx = 0
    # while idx < len(text):
    #     index_l = text.lower().find(old.lower(), idx)
    #     if index_l == -1:
    #         return text
    #     text = text[:index_l] + new + text[index_l + len(old):]
    #     idx = index_l + len(new) 
    while idx < len(text):
        index_l = unaccent(text.lower()).find(unaccent(old.lower()), idx)
        if index_l == -1:
            return text
        text = text[:index_l] + new + text[index_l + len(old):]
        idx = index_l + len(new) 
    return text

def get_latlong_from_address(address, API_KEY):
    url = 'https://maps.googleapis.com/maps/api/geocode/json'
    data = {
            'address':address,
            'key':API_KEY    
    }
    # print(urllib.parse.urlencode(data))
    req = requests.get(url +'?'+ urllib.parse.urlencode(data) )
    latlong = req.json()['results'][0]['geometry']['location']
    return latlong
