import pandas as pd
import argparse
from sqlmodel import create_engine
from sqlalchemy.types import *
import boto3, logging, botocore
from botocore.config import Config
import io, re

print("Hello")

def get_s3_client(endpoint_url, access_key_id, secret_access_key):
    s3 = boto3.client('s3',
                      endpoint_url=endpoint_url,
                      aws_access_key_id=access_key_id,
                      aws_secret_access_key=secret_access_key,
                      config=Config(signature_version='s3v4'))
    return s3
def load_df_from_s3(bucket, key, s3, sep=",", index_col=None, usecols=None):
    ''' Read a csv from a s3 bucket & load into pandas dataframe'''
    try:
        logging.info(f"Loading {bucket, key}")
        obj = s3.get_object(Bucket=bucket, Key=key)
        return pd.read_csv(obj['Body'], sep=sep, index_col=index_col, usecols=usecols, low_memory=False)
    except botocore.exceptions.ClientError as err:
        status = err.response["ResponseMetadata"]["HTTPStatusCode"]
        errcode = err.response["Error"]["Code"]
        if status == 404:
            logging.warning("Missing object, %s", errcode)
        elif status == 403:
            logging.error("Access denied, %s", errcode)
        else:
            logging.exception("Error in request, %s", errcode)

# Python function do the job/cleaning
def data_cleaner(df):

    def clean_store_location(st_loc):
        return re.sub(r'[^\w\s]', '', st_loc).strip()

    def clean_product_id(pd_id):
        matches = re.findall(r'\d+', pd_id)
        if matches:
            return matches[0]
        return pd_id

    def remove_dollar(amount):
        return float(amount.replace('$', ''))

    df['STORE_LOCATION'] = df['STORE_LOCATION'].map(lambda x: clean_store_location(x))
    df['PRODUCT_ID'] = df['PRODUCT_ID'].map(lambda x: clean_product_id(x))

    for to_clean in ['MRP', 'CP', 'DISCOUNT', 'SP']:
        df[to_clean] = df[to_clean].map(lambda x: remove_dollar(x))

    return df

if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("-eu", "--endpoint_url", required=True, type=str, default="http://minio:9000",
                    help="S3 endpoint url. Default: 'http://minio:9000'")
    
    ap.add_argument("-b", "--bucket", required=True, type=str, default="dataops-bronze",
                    help="Your bucket name. Default: 'dataops-bronze'")    
    
    ap.add_argument("-k", "--key", required=True, type=str, default="raw/dirty_store_transactions.csv",
                    help="The object path in bucket. Default: 'raw/dirty_store_transactions.csv'")
    
    ap.add_argument("-aki", "--access_key_id", required=True, type=str, default="dataopsadmin",
                    help="Your access_key_id. Default: 'dataopsadmin'")
    
    ap.add_argument("-sak", "--secret_access_key", required=True, type=str, default="dataopsadmin",
                    help="Your secret_access_key. Default: 'dataopsadmin'")
    
    ap.add_argument("-c", "--conn", required=True, type=str, default="postgresql://train:Ankara06@postgres:5432/traindb",
                    help="Your DB connection string. Example postgresql://user:password@localhost:5432/mydb. Default: ''")
    
    args = vars(ap.parse_args())

    endpoint_url = args['endpoint_url']
    bucket = args['bucket']
    key = args['key']
    access_key_id = args['access_key_id']
    secret_access_key = args['secret_access_key']
    SQLALCHEMY_DATABASE_URL = args['conn']

    s3 = get_s3_client(endpoint_url, access_key_id, secret_access_key)
    
    df_raw = load_df_from_s3(bucket=bucket, key=key, s3=s3)
    df_clean = data_cleaner(df=df_raw)

    engine = create_engine(SQLALCHEMY_DATABASE_URL)
    df_schema = {
        "STORE_ID": Text,
        "STORE_LOCATION": Text,
        "PRODUCT_CATEGORY": Text,
        "PRODUCT_ID": Integer,
        "MRP": Float,
        "CP": Float,
        "DISCOUNT": Float,
        "SP": Float,
        "Date_Casted": Date
    }

    df_clean.to_sql("clean_data_transactions", index=False, 
               con=engine, if_exists="replace", dtype=df_schema)