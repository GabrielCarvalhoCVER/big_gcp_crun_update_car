import os 
import geopandas as gpd
# from numpy.lib.arraysetops import isin 
import pandas as pd 
from io import StringIO
from sqlalchemy import create_engine
import os 
from unidecode import unidecode as unaccent
from datetime import datetime
import pytz
from shapely.validation import make_valid
from typing import Union
from requests.utils import quote as url_quote


def list_type(list:list):
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

def get_clean_list (list:list):
    return [get_clean_str(row) for row in list]

def get_clean_str(str:str):
    return multiple_replace(unaccent(str).lower(),{' ':'_' , '\n':'_' , '\r':'_', '\t':'_','__':'_'})

def coalesce(*arg):
  for el in arg:
    if el is not None:
      return el
  return None

def get_now_sp(): 
    return datetime.strptime(str(datetime.now((pytz.timezone('America/Sao_Paulo')))).rsplit('-',1)[0],'%Y-%m-%d %H:%M:%S.%f')

def multiple_replace(text:str, param:dict):
    new_text = text
    for r1,r2 in param.items():
        new_text = new_text.replace(r1,r2)
        
    return new_text

################################################################################################################################################################################################

def get_max_ndim(geom_series):
    max_ndim = 2
    for geom in geom_series:
        ndim_row = get_ndim(geom)
        if ndim_row > max_ndim:
            max_ndim = ndim_row
    return max_ndim

def get_ndim(geom):
    try:
        return coalesce(geom._ndim,0)
    except:
        return 0

def st_force3d_astxt(geom):
    # print(dir(geom))
    #         
    if geom.has_z:
        return geom.wkt
    
    geom = geom.wkt

    geom_split = geom.split(',')
    for idx,geom_split_row in enumerate(geom_split):
        if len(geom_split_row) == 0:
            pass
        elif geom_split_row[-1].isdigit():
            geom_split[idx] = geom_split_row + ' 0'
        else:
            pass
            # print('passou1')
            # print(geom_split_row)
    
    geom = ','.join(geom_split)
    
    geom_split2 = geom.split(')')
    for idx,geom_split2_row in enumerate(geom_split2):
        if len(geom_split2_row) == 0:
            pass
        elif geom_split2_row[-1].isdigit():
            geom_split2[idx] = geom_split2_row + ' 0'
        else:
            pass
            # print('passou2')
            # print(geom_split2_row)

    geom = ')'.join(geom_split2)
    return geom 

def st_force3d_asewkt(geom,srid:str):
    if str(geom) == 'None':
        return 'null'

    elif not(geom.is_valid):
        return 'null' 

    else:
        return ('SRID=' + srid  + '; ' + st_force3d_astxt(geom))

def st_asewkt(geom,srid:str):
    if str(geom) == 'None':
        return 'null'

    elif not(geom.is_valid):
        return 'null' 

    else:
        return ('SRID=' + srid  + '; ' + (geom.wkt))

################################################################################################################################################################################################

def get_engine_str_with_env(db_env:str, use_cloud_sql_name:Union[bool,None]=None, force_print:bool=False ):
    db_env = db_env.upper()
    user = os.getenv("DB_USER_" + db_env)
    pswd = os.getenv("DB_PSWD_" + db_env)
    database = os.getenv("DB_NAME_" + db_env)
    dialect = os.getenv("DB_DIALECT_" + db_env)
    host = os.getenv("DB_HOST_" + db_env ,None)
    cloud_sql_connection_name = os.getenv("DB_CLOUD_SQL",None)

    db_json = {
        'DB_USER':user,
        'DB_PSWD':pswd,
        'DB_NAME':database,
        'DB_DIALECT':dialect,
    }
    
    if host:
        db_json['DB_HOST']=host
    
    if cloud_sql_connection_name:
        db_json['DB_CLOUD_SQL']=cloud_sql_connection_name

    
    URL = get_engine_str_with_json(db_json, use_cloud_sql_name=use_cloud_sql_name, force_print=force_print)

    return URL

def get_engine_str_with_json(db_json:dict, use_cloud_sql_name:Union[bool,None]=None, force_print:bool=False ):
    user = url_quote(db_json['DB_USER'])
    pswd = url_quote(db_json['DB_PSWD'])
    database = db_json['DB_NAME']
    dialect = db_json['DB_DIALECT']
    host = db_json.get('DB_HOST')
    conn_option = db_json.get('DB_CONN_OPT')
    cloud_sql_conn_name = db_json.get('DB_CLOUD_SQL')

    if force_print: print(f'user [{user}]')
    if force_print: print(f'database [{database}]')
    if force_print: print(f'dialect [{dialect}]')
    if force_print: print(f'host [{host}]')
    if force_print: print(f'conn_option [{conn_option}]')
    if force_print: print(f'cloud_sql_conn_name [{cloud_sql_conn_name}]')
    
    if ((os.path.expanduser("~") != '/root') and (use_cloud_sql_name is None)) or (use_cloud_sql_name==False):
        URL = f"{dialect}://{user}:{pswd}@{host}/{database}"
    else:
        URL = f"{dialect}://{user}:{pswd}@/{database}?{conn_option}=/cloudsql/{cloud_sql_conn_name}"

    return URL

def get_engine_str_with_secret(db_secret_id:str, db_project_id:str, use_cloud_sql_name:Union[bool,None]=None, force_print:bool=False ):
    from access_db.gcp_clients.gcp_client_secretmanager import SecretManager_Client

    secretmanager_client = SecretManager_Client()
    secret_json = secretmanager_client.get_secret( secret_id=db_secret_id , project_id=db_project_id )

    URL = get_engine_str_with_json(db_json=secret_json, use_cloud_sql_name=use_cloud_sql_name, force_print=force_print)

    return URL

################################################################################################################################################################################################

def get_df_sql(sql_query_string:str, engrawconn_db=None , db_json:Union[dict,None]=None, db_env:Union[str,None]=None , db_secret:Union[dict,None]=None, return_type:str='df' , geom_cols:list = ['geometry'] , force_print:bool=False):
    
    engrawconn_db, engrawconn_db_none  = get_eng_and_rawconn(engrawconn_db=engrawconn_db , db_json=db_json , db_env=db_env , db_secret=db_secret )
    engine_db, rawconn_db = engrawconn_db
    # cur_db = rawconn_db.cursor()

    if force_print:
        # print('sql_query_string')
        print(sql_query_string)
        # print()
    
    if force_print: print('before read_sql_query')
    df_sql = pd.read_sql_query(sql_query_string, rawconn_db)
    
    if return_type == 'gdf':
        df_sql = gpd.GeoDataFrame(df_sql)
        
        if isinstance(geom_cols, str):
            geom_cols = [geom_cols]

        for geom_col in geom_cols:
            from shapely import wkt
            df_sql[geom_col] = df_sql[geom_col].apply(wkt.loads)
            df_sql=df_sql.set_geometry(geom_col)

            #df_sql[geom_col] = df_sql[geom_col].set_crs(epsg=4326)
    if force_print: print('before commit')
    rawconn_db.commit()
    if force_print: print('after read_sql_query')
    ##################################################

    close_rawconn_and_disp_eng(engrawconn_db=engrawconn_db, engrawconn_db_none=engrawconn_db_none )
    
    return df_sql

################################################################################################################################################################################################

def get_eng_and_rawconn(engrawconn_db = None , db_json:Union[dict,None]=None, db_env:Union[str,None]=None , db_secret:Union[dict,str,None]=None , use_cloud_sql_name:Union[bool,None]=None, force_print:bool=False ):
    engrawconn_db_none = False
    if (engrawconn_db is None):

        if (db_secret is None) and (db_json is None) and (db_env is None) :

            if ('GCP_DEFAULT_DB_SECRET_ID' in os.environ) and ('GCP_DEFAULT_DB_PROJECT_ID' in os.environ):
                db_secret_id = os.getenv('GCP_DEFAULT_DB_SECRET_ID')
                db_project_id = os.getenv('GCP_DEFAULT_DB_PROJECT_ID')
        
        if isinstance(db_secret,str):
            db_secret_id = db_secret 
            db_project_id = os.getenv('GCP_DEFAULT_DB_PROJECT_ID')                
        elif isinstance(db_secret,dict):
            db_secret_id = db_secret.get('secret_id', os.getenv('GCP_DEFAULT_DB_SECRET_ID'))
            db_project_id = db_secret.get('project_id', os.getenv('GCP_DEFAULT_DB_PROJECT_ID'))

        elif db_env is None:
            db_env = os.getenv('DEFAULT_DB')
        
        # print('open conn')
            
        if (db_project_id is not None ) and (db_secret_id is not None ):
            engine_str = get_engine_str_with_secret(db_secret_id=db_secret_id , db_project_id=db_project_id, use_cloud_sql_name=use_cloud_sql_name, force_print=force_print)

        elif db_env is not None:
            engine_str = get_engine_str_with_env(db_env, use_cloud_sql_name=use_cloud_sql_name, force_print=force_print)
        
        elif db_json is not None: 
            engine_str = get_engine_str_with_json(db_json, use_cloud_sql_name=use_cloud_sql_name, force_print=force_print)

        else:
            raise Exception ('Must declare db_env or db_json or (enviroment GCP_DEFAULT_DB_SECRET_ID or  db_secret[secret_id])+(enviroment GCP_DEFAULT_DB_PROJECT_ID or db_secret[project_id])')

        engine_db = create_engine(engine_str)

        if force_print: print(f'engine_db [{engine_db}]')

        rawconn_db = engine_db.raw_connection()
        engrawconn_db = [engine_db, rawconn_db]
        engrawconn_db_none = True

    return engrawconn_db , engrawconn_db_none 

def close_rawconn_and_disp_eng(engrawconn_db, engrawconn_db_none:bool = True):
    if engrawconn_db_none:
        engine_db, rawconn_db = engrawconn_db
        rawconn_db.close()
        engine_db.dispose()

def check_table_exists(table:str, schema:str, engrawconn_db=None , db_json:Union[dict,None]=None, db_env:Union[str,None]=None , db_secret:Union[dict,None]=None ):

    engrawconn_db, engrawconn_db_none  = get_eng_and_rawconn(engrawconn_db=engrawconn_db , db_json=db_json , db_env=db_env, db_secret=db_secret)
    engine_db, rawconn_db = engrawconn_db
    cur_db = rawconn_db.cursor()
    
    sql_q =f"""
        SELECT EXISTS (
            SELECT FROM information_schema.tables 
            WHERE  table_schema = '{schema}'
            AND    table_name   = '{table}'
        );
    """
    # print('check_table_exists sql:')
    # print(sql_q)
    cur_db.execute(sql_q)
    table_exists = cur_db.fetchall()[0][0]
    rawconn_db.commit()

    close_rawconn_and_disp_eng(engrawconn_db=engrawconn_db, engrawconn_db_none=engrawconn_db_none )

    return table_exists    
