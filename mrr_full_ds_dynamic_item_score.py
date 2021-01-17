import boto3
import configparser
import argparse
import pandas as pd
import psycopg2
from datetime import datetime
import os as os

# read values from ini
config = configparser.ConfigParser(interpolation=None)
config.read("//home//ec2-user//scripts//Talend_2_python//config//mrr_config.ini")

## AWS_CONFIG_FILE
KEY_ID = config.get('AWS_CONFIG_FILE', 'AWS_ACCESS_KEY_ID')
KEY_SECRET = config.get('AWS_CONFIG_FILE', 'AWS_ACCESS_KEY_SECRET')

## target: REDSHIFT_ADMIN
username_red = config.get('REDSHIFT_ADMIN', 'User')
password_red = config.get('REDSHIFT_ADMIN', 'Pass')

## connections
con_red = psycopg2.connect(dbname= 'dwh_dev', host='redshift-cluster-1.coczxpfcmk83.us-east-1.redshift.amazonaws.com',port= '5439', user= username_red, password= password_red)

## args
bucket_name = 'worthy-datasci'
#v_model = "018"
v_today_rel = datetime.today().strftime('%Y-%m-%d')

tgt_tablename = 'mrr_dynamic_item_scores_tag'
tgt_schema = 'mrr'

## get model
def argument_parser():
    parser = argparse.ArgumentParser(description='get ds model id ')
    parser.add_argument("--model", help='018')
    args = parser.parse_args()
    return args

## from s3 to redshift
def truncate_mrr_redshift(dbcon, tgt_schema, tgt_tablename):
    tablename = tgt_schema+'.'+tgt_tablename
    cur = dbcon.cursor()
    sql = f"truncate table {tablename} "
    cur.execute(sql)
    con_red.commit()
    cur.close()

## select last directory
def upload_list_of_directories_aws(bucket, str_prefix):
    s3 = boto3.resource('s3', aws_access_key_id=KEY_ID, aws_secret_access_key=KEY_SECRET)
    my_bucket = s3.Bucket(bucket)
    datelist = []
    for file in my_bucket.objects.filter(Prefix=str_prefix):
        datelist.append(file.last_modified)
    # get max date from date list model
    return max(datelist)

## from s3 to redshift looping foreach file in selected directory
def copy_s3_data_to_redshift(dbcon, tgt_schema, tgt_tablename, s3_file):
    tablename = tgt_schema+'.'+tgt_tablename
    cur = dbcon.cursor()
    sql = """copy {}
             from 's3://worthy-datasci/{}'
             access_key_id '{}'
             secret_access_key '{}'
             delimiter '\\t' gzip
             ignoreheader 1
             dateformat 'auto'
             timeformat 'auto'""".format(str(tablename), str(s3_file), str(KEY_ID), str(KEY_SECRET))
    cur.execute(sql)
    con_red.commit()
    cur.close()
    print(sql)

def upload_to_aws(bucket, str_prefix, dbcon, tgt_schema, tgt_tablename):
    s3 = boto3.client('s3', aws_access_key_id=KEY_ID,
                      aws_secret_access_key=KEY_SECRET)
    """Get a list of keys in an S3 bucket."""
    keys = []
    resp = s3.list_objects_v2(Bucket=bucket, Prefix=str_prefix)
    for obj in resp['Contents']:
        copy_s3_data_to_redshift(dbcon, tgt_schema, tgt_tablename, str(obj['Key']))
        keys.append(obj['Key'])
    return keys

if __name__ == '__main__':
    args = argument_parser()
    v_model = args.model
    string_key_direct = f"automation-tests/data/dynamic/{v_model}/"
    # last day updated
    v_date = upload_list_of_directories_aws(bucket_name, string_key_direct).strftime('%Y-%m-%d')
    # build key string
    string_key = f"automation-tests/data/dynamic/{v_model}/{v_date}/predict/last/"
    print(string_key)
    # truncate mrr table
    truncate_mrr_redshift(con_red, tgt_schema, tgt_tablename)
    print('data from mrr_dynamic_item_scores_tag truncated')
    # load mrr table
    upload_to_aws(bucket_name, string_key, con_red, tgt_schema, tgt_tablename)
    print('data loaded to mrr_dynamic_item_scores_tag')
