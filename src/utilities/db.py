# Import modules
import os
import base64
import argparse
import datetime as dt
from urllib.parse import quote_plus

import yaml
import dotenv
import pymysql
import psycopg2
import numpy as np 
import pandas as pd
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
    print("Connection successful")
    
    return conn


def query_dwh(sql, dwh_credentials, prefix, project_dir, kwargs):
    conn = db_connection(dwh_credentials, prefix, project_dir)
    df = pd.read_sql(sql, conn, params=kwargs)

    return df


# Run code
if __name__ == "__main__":
    # Parameter arguments
    args = argparse.ArgumentParser()
    args.add_argument("--config", default="params.yaml")
    parsed_args = args.parse_args()

    # DB connection
    print(db_connection(read_params(parsed_args.config)["db_credentials"], 'DWH', read_params(parsed_args.config)["project_dir"]))