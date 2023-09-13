# Import modules
import os
import json
import base64
import logging
import argparse
import requests
import datetime as dt
from urllib.parse import quote_plus

import yaml
import dotenv
import pymysql
import psycopg2
import numpy as np 
import pandas as pd
from tzlocal import get_localzone
from IPython.display import display
from sqlalchemy import create_engine


# Functions
def read_params(config_path):
    """
    read parameters from the params.yaml file
    input: params.yaml location
    output: parameters as dictionary
    """
    with open(config_path) as yaml_file:
        config = yaml.safe_load(yaml_file)
    return config


def load_credentials(credentials_path):
    """
    Load environment variables to path
    input: Path to .env file
    output: Load ENV variables
    """

    # Load ENV variables
    dotenv_path = os.path.join(os.getcwd(), credentials_path)
    dotenv.load_dotenv(dotenv_path)


def encrypt_credentials(dwh_credentials, prefix, project_dir):
    """
    encrypt dwh credentials
    input: dwh credebtials
    output: encrypted dwh credebtials
    """

    # Load ENV variables
    load_credentials(project_dir + dwh_credentials['path'])

    # Encrypt
    dwh_credentials_encrypted = {}
    for e in dwh_credentials[f'{prefix.lower()}_env']:
        dwh_credentials_encrypted[e] = base64.b64encode(os.getenv(e).encode("utf-8")).decode()
    
    print(dwh_credentials_encrypted)


def decrypt_credentials(dwh_credentials, prefix, project_dir):
    """
    decrypt dwh credentials
    input: encrypted dwh credebtials
    output: decrypted dwh credebtials
    """

    # # Load ENV variables
    load_credentials(project_dir + dwh_credentials['path'])

    # Decrypt
    dwh_credentials_decrypted = {}
    for e in dwh_credentials[f'{prefix.lower()}_env']:
        # print(e)
        # print(os.getenv(e))
        dwh_credentials_decrypted[e] = base64.b64decode(os.getenv(e)).decode("utf-8")
    
    return dwh_credentials_decrypted


def db_connection(dwh_credentials, prefix, project_dir):
    # Decrypt credentials
    dwh_credentials_decrypted = decrypt_credentials(dwh_credentials, prefix, project_dir)
    host = dwh_credentials_decrypted[f'{prefix}_HOST']
    port = dwh_credentials_decrypted[f'{prefix}_PORT']
    dbname = dwh_credentials_decrypted[f'{prefix}_DB_NAME']
    user = dwh_credentials_decrypted[f'{prefix}_USER']
    password = dwh_credentials_decrypted[f'{prefix}_PASSWORD']
    
    # Connect to DB
    if prefix in ['DWH']:
        conn_str = f'postgresql+psycopg2://{user}:{quote_plus(password)}@{host}:{port}/{dbname}'
    elif prefix in ['ISW', 'MIFOS']:
        conn_str = f'mysql+pymysql://{user}:{quote_plus(password)}@{host}:{port}/{dbname}'
    conn = create_engine(conn_str)

    # Logs
    logging.warning(f'Connection successful')
    
    return conn


def query_dwh(sql, dwh_credentials, prefix, project_dir, kwargs=None):
    conn = db_connection(dwh_credentials, prefix, project_dir)
    df = pd.read_sql(sql, conn, params=kwargs)

    return df


def post_to_dwh(df, dwh_credentials, upload_data_config, prefix, project_dir):
    schema = upload_data_config['schema']
    table = upload_data_config['table']
    if_exists = upload_data_config['if_exists']
    index = upload_data_config['index']
    chunksize = upload_data_config['chunksize']
    method = upload_data_config['method']
    
    conn = db_connection(dwh_credentials, prefix, project_dir)
    response = df.to_sql(name=table, con=conn, schema=schema, if_exists=if_exists, index=index, chunksize=chunksize, method=method)
    
    return response


def trigger_scoring_script(config_path, agent_id):  
    # Load configurations
    config = read_params(config_path)
    project_dir = config["project_dir"]
    airflow_credentials = config["db_credentials"]
    prefix = config["airflow_api_config"]['prefix']
    trigger_api = config["airflow_api_config"]['trigger_api']
    airflow_dag_url = config["airflow_api_config"]['airflow_dag_url']
    headers_content_type = config["airflow_api_config"]['headers_content_type']
    headers_accept = config["airflow_api_config"]['headers_accept']
    conf_is_initial_run = config["airflow_api_config"]['conf_is_initial_run']
    verify = config["airflow_api_config"]['verify']
    callback_url = config["airflow_api_config"]['callback_url']
    scoring_script_name = config["airflow_api_config"]['scoring_script_name']
    
    # Parameters
    local_tz = get_localzone()
    execution_date = str(dt.datetime.now().replace(tzinfo=local_tz))
    
    # Decrypt credentials
    airflow_credentials_decrypted = decrypt_credentials(airflow_credentials, prefix, project_dir)
    host = airflow_credentials_decrypted[f'{prefix}_HOST']
    user = airflow_credentials_decrypted[f'{prefix}_USER']
    password = airflow_credentials_decrypted[f'{prefix}_PASSWORD']

    # Logs
    print('')
    logging.warning(f'Triggering airflow {scoring_script_name.lower()} scoring script for {agent_id} ...')
    logging.warning(f'URL: {host}{airflow_dag_url}')
    
    # Trigger Airflow DAG
    if trigger_api == True:
        response = requests.post(url=f'{host}{airflow_dag_url}',
                                headers={'Content-type': f'{headers_content_type}',
                                         'Accept': f'{headers_accept}'},
                                json={"execution_date": execution_date,
                                      "conf": {'agent_id': agent_id}},
                                auth=requests.auth.HTTPBasicAuth(f"{user}", f'{password}'),
                                verify=verify)
        
        # Print response
        print('')
        logging.warning(f'--------------- Response ---------------\n status_code: {response.status_code}\n {response.text}')
        
        return response
    else:
        # Exit
        logging.warning(f"{scoring_script_name} scoring pipeline is NOT triggered because trigger_api flag = {trigger_api}")


def share_scoring_results(config_path, agent_id, callback_url, payload):  
    # Load configurations
    config = read_params(config_path)
    project_dir = config["project_dir"]
    api_credentials = config["db_credentials"]
    prefix = config["isw_api_config"]['prefix']
    share_limits = config["isw_api_config"]['share_limits']
    headers_content_type = config["isw_api_config"]['headers_content_type']
    scoring_script_name = config["airflow_api_config"]['scoring_script_name']
    verify = config["isw_api_config"]['verify']
    
    # Decrypt credentials
    isw_api_credentials_decrypted = decrypt_credentials(api_credentials, prefix, project_dir)
    isw_api_username = isw_api_credentials_decrypted[f'{prefix}_USER']
    isw_api_password = isw_api_credentials_decrypted[f'{prefix}_PASSWORD']

    # Logs
    print('')
    logging.warning(f'Sharing {scoring_script_name.lower()} limits for {agent_id} ...')
    logging.warning(f'URL: {callback_url}')
    
    # Share Limits
    if share_limits == True:
        response = requests.post(
            url=callback_url,
            headers={'Content-Type': f'{headers_content_type}'},
            json=payload,
            auth=requests.auth.HTTPBasicAuth(f'{isw_api_username}', f'{isw_api_password}'),
            verify=verify
            )

        # Print response
        print('')
        logging.warning(f'--------------- Response ---------------\n status_code: {response.status_code}\n {response.text}')
        
        return response
    else:
        # Exit
        logging.warning(f"{scoring_script_name} limits for {agent_id} NOT shared because share_limits flag = {share_limits}")


# Run code
if __name__ == "__main__":
    # Parameter arguments
    args = argparse.ArgumentParser()
    args.add_argument("--config", default="params.yaml")
    parsed_args = args.parse_args()

    # DB connection
    print(db_connection(read_params(parsed_args.config)["db_credentials"], 'DWH', read_params(parsed_args.config)["project_dir"]))