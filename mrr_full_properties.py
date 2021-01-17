import boto3
import configparser
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

## source: POSTGRESQL
postgres_database = config.get('postgres_a', 'DATABASE')
postgres_host = config.get('postgres_a', 'HOST')
postgres_port = config.get('postgres_a', 'PORT')
postgres_user = config.get('postgres_a', 'USER')
postgres_password = config.get('postgres_a', 'PASSWORD')

## target: REDSHIFT_ADMIN
username_red = config.get('REDSHIFT_ADMIN', 'User')
password_red = config.get('REDSHIFT_ADMIN', 'Pass')

## connections
connection = psycopg2.connect(database=postgres_database, user=postgres_user, password=postgres_password, host=postgres_host, port=postgres_port)
connection.autocommit = True
con_red = psycopg2.connect(dbname= 'dwh_dev', host='redshift-cluster-1.coczxpfcmk83.us-east-1.redshift.amazonaws.com',port= '5439', user= username_red, password= password_red)

## global params
src_table_name = 'properties'
src_schema_name = 'public'

bucket_name = 'worthy-etl'
s3_file_name = 'etl/mrr_full_properties_tag.csv'

file_path = r'/home/ec2-user/scripts/Talend_2_python/Step_1_Source_To_Mirror_Part_11/full/mrr_full_properties_tag.csv'
src_schema_table_name = src_schema_name+'.'+src_table_name

tgt_table_name = 'mrr_properties_tag'
tgt_schema_name = 'mrr'

## update log table start
def update_DWH_ETL_log_start(dbcon,tablename):
    crr = dbcon.cursor()
    sql = """insert into DWH_ETL_log
             (cycle_id,start_time,job_name,action_name,action_status,is_current_cycle,comment,error)
             VALUES
             ((select max(cycle_id) from dwh_etl_cycle_list),CURRENT_TIMESTAMP,'{}','load mrrtables','started running',1,'','');""".format(str(tablename))
    crr.execute(sql)
    dbcon.commit()
    crr.close()

## update log table finish
def update_DWH_ETL_log_finish(dbcon, tablename):
    crr = dbcon.cursor()
    sql = """update DWH_ETL_log 
             set end_time=CURRENT_TIMESTAMP , action_status='finished running' 
             WHERE job_name = '{}'
             AND   end_time is null AND is_current_cycle = 1;""".format(str(tablename))
    crr.execute(sql)
    dbcon.commit()
    crr.close()

def read_sql_max_rows(schema_table_name, conn):
    sql_max = "SELECT count(*) as rows_count FROM {} ".format(str(schema_table_name))
    max_row = pd.read_sql_query(sql_max, conn)
    rows_count = max_row["rows_count"].to_csv(None, header=False, encoding="utf-8", index=False)
    return int(rows_count)

def read_sql_chunked(schema_table_name, conn, nrows, chunksize, file):
    # args
    sql = "SELECT id, name ,option_type,created_at ,updated_at	,subset_type_id	,adds_subset ,system_name ,can_be_limited ,as_numeric FROM {} ".format(str(schema_table_name))
    start = 0
    dfs = []
    # clear file if exist
    try:
        os.remove(file)
    except OSError:
        pass
    # return the full table row number
    maxi = read_sql_max_rows(schema_table_name, conn)
    print(maxi)
    while start <= maxi:
        df = pd.read_sql("%s LIMIT %s OFFSET %s" % (sql, chunksize, start), conn)
        df.to_csv(file, mode='a', header=False, encoding="utf-8", index=False)
        start += chunksize
        print("Events added: %s to %s of %s" % (start-chunksize, start, nrows))
    return dfs

# load csv file to s3
def upload_to_aws(local_file, bucket, s3_file):
    s3 = boto3.client('s3', aws_access_key_id=KEY_ID,
                      aws_secret_access_key=KEY_SECRET)
    try:
        s3.delete_object(Bucket=bucket, Key=s3_file)
        print("delete s3 Successful")
        s3.upload_file(local_file, bucket, s3_file)
        print("Upload s3 Successful")
        return True
    except FileNotFoundError:
        print("The file was not found")
        return False
    except NoCredentialsError:
        print("Credentials not available")
        return False

## from s3 to redshift
def copy_s3_data_to_redshift(dbcon, tgt_schema, tgt_tablename, s3_file):
    tablename = tgt_schema+'.'+tgt_tablename
    cur = dbcon.cursor()
    pre_sql = ' truncate ' +tablename+ ' ;'
    cur.execute(pre_sql)
    con_red.commit()
    sql = """copy {}
             from 's3://worthy-etl/{}'
             access_key_id '{}'
             secret_access_key '{}'
             delimiter ','
             ignoreheader 0
             csv quote as '"'
             dateformat 'auto'
             timeformat 'auto'
             maxerror 0 
             truncatecolumns""".format(str(tablename), str(s3_file), str(KEY_ID), str(KEY_SECRET))
    cur.execute(sql)
    con_red.commit()
    cur.close()
    print('data from s3 copied')

if __name__ == '__main__':

    update_DWH_ETL_log_start(con_red, tgt_table_name)
    # read sql table to csv
    datetime_object = datetime.now()
    print("Start TimeStamp read sql table to csv")
    print("---------------")
    print(datetime_object)
    print("")
    read_sql_chunked(src_schema_table_name, connection, 1000000 * 4, 350000, file_path)
    datetime_object = datetime.now()
    print("End TimeStamp read sql table to csv")
    print("---------------")
    print(datetime_object)
    print("")

    # read csv local file to s3
    datetime_object = datetime.now()
    print("Start TimeStamp read csv local file to s3")
    print("---------------")
    print(datetime_object)
    print("")
    upload_to_aws(file_path, bucket_name, s3_file_name)
    datetime_object = datetime.now()
    print("End TimeStamp read csv local file to s3")
    print("---------------")
    print(datetime_object)
    print("")

    # read s3 to redshift
    datetime_object = datetime.now()
    print("Start TimeStamp read s3 to redshift")
    print("---------------")
    print(datetime_object)
    print("")
    copy_s3_data_to_redshift(con_red, tgt_schema_name, tgt_table_name, s3_file_name)
    # clear file if exist
    try:
        os.remove(file_path)
    except OSError:
        pass
    datetime_object = datetime.now()
    print("End TimeStamp read s3 to redshift")
    print("---------------")
    print(datetime_object)
    print("")
    update_DWH_ETL_log_finish(con_red, tgt_table_name)
