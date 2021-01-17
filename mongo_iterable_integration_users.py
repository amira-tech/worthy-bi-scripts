import boto3
from botocore.exceptions import NoCredentialsError
from shutil import copyfile
import configparser
import pandas as pd
import psycopg2
import os
import csv
import ast
import time
import sys
import datetime
from datetime import date
from sqlalchemy import create_engine
import pandas_redshift as pr

datetime_object = datetime.datetime.now()
print ("Start TimeStamp")
print ("---------------")
print(datetime_object)
print("")

# read values from ini
config = configparser.ConfigParser(interpolation=None)
config.read("//home//ec2-user//scripts//mongo//config_ini//file_config.ini")

# read values from a section
KEY_ID = config.get('AWS_CONFIG_FILE', 'AWS_ACCESS_KEY_ID')
KEY_SECRET = config.get('AWS_CONFIG_FILE', 'AWS_ACCESS_KEY_SECRET')
# read values from a section
## AWS_CONFIG_FILE
KEY_ID = config.get('AWS_CONFIG_FILE', 'AWS_ACCESS_KEY_ID')
KEY_SECRET = config.get('AWS_CONFIG_FILE', 'AWS_ACCESS_KEY_SECRET')
## REDSHIFT_ADMIN
username_red = config.get('REDSHIFT_ADMIN', 'User')
password_red = config.get('REDSHIFT_ADMIN', 'Pass')
host_red = config.get('REDSHIFT_ADMIN', 'Host')

# file from mongo
mongo_file = config.get('FILE_PATH', 'file_iterable')
mongo_file_bck =config.get('FILE_PATH', 'file_iterable_bck')

### copy file to bck directory
src = mongo_file
dst = mongo_file_bck
copyfile(src, dst)

# ------------------------------------------------------------------------------------------------------------------------
## basic params 2201215
local_file = mongo_file;
bucket_name = 'worthy-etl';
s3_file_name = 'mirros/mrr_iterable_integration_users.csv';

table_name = 'mrr_iterable_integration_users_tag';
statement_drop = 'drop table ' +table_name+ ' cascade';
statement_create = 'create table ' + table_name + ' (';

# ------------------------------------------------------------------------------------------------------------------------

#Obtaining the connection to RedShift
con=psycopg2.connect(dbname= 'dwh_dev', host='redshift-cluster-1.coczxpfcmk83.us-east-1.redshift.amazonaws.com',
port= '5439', user=username_red, password=password_red)
con_red=pr.connect_to_redshift(dbname='dwh_dev',
                               host='redshift-cluster-1.coczxpfcmk83.us-east-1.redshift.amazonaws.com',
                               port = '5439',
                               user=username_red,
                               password=password_red)


# read csv file and build table in redshift according to its structure
## open file get header and make create table statement
f = open(mongo_file, 'r')
reader = csv.reader(f, delimiter=';')
longest, headers, type_list = [], [], []

# ------------------------------------------------------------------------------------------------------------------------


## update log table start
def update_DWH_ETL_log_start(dbcon,tablename):
    crr = dbcon.cursor()
    sql = """insert into DWH_ETL_log
             (cycle_id,start_time,job_name,action_name,action_status,is_current_cycle,comment,error)
             VALUES
             ((select max(cycle_id) from dwh_etl_cycle_list),CURRENT_TIMESTAMP,'iterable_integration_users', 'load ods tables','started running',1,'','');"""
    crr.execute(sql)
    dbcon.commit()
    crr.close()

## type_list
def dataType(val, current_type):
    return 'varchar'

## headers & longest
for row in reader:
    if len(headers) == 0:
        headers = row
        for col in row:
            longest.append(0)
            type_list.append('')
    else:
        for i in range(len(row)):
            # NA is the csv null value
            if type_list[i] == 'varchar' or row[i] == 'NA':
                pass
            else:
                var_type = dataType(row[i], type_list[i])
                type_list[i] = var_type
            longest[i] = 1000
f.close()

for i in range(len(headers)):
        if headers[i].lower() == 'id' or 'experiments' in headers[i].lower():
            statement_create = (statement_create + '\n{} varchar({}),').format(headers[i].lower(), str(longest[i]))
statement_create = statement_create[:-1] + ');'

## replace table
### drop table only if exit
def checkTableExists(dbcon, tablename):
    dbcur = dbcon.cursor()
    dbcur.execute("""
        SELECT COUNT(*)
        FROM information_schema.tables
        WHERE table_name = '{0}'
        """.format(tablename.replace('\'', '\'\'')))
    if dbcur.fetchone()[0] == 1:
        dbcur.close()
        return True
    dbcur.close()
    return False

### create table
def createTable(dbcon, tablename):
    crs = dbcon.cursor()
    Ge = checkTableExists(dbcon, tablename)
    print(statement_create)
    print(Ge)
    if Ge is True:
       crs.execute(statement_drop)
       con.commit()
       crs.execute(statement_create)
       con.commit()
       crs.close()
       print('Table exist drop and create')
    else:
        crs.execute(statement_create)
        con.commit()
        crs.close()
        print('Table does not exist create only')

# upload new file to s3
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

# S3 to panda Panda to redshift
def s3_to_pandas_with_processing(bucket, key,header=None):
    # get key using boto3 client
    s3 = boto3.client('s3', aws_access_key_id=KEY_ID,
                      aws_secret_access_key=KEY_SECRET)
    obj = s3.get_object(Bucket=bucket, Key=key)
    df = pd.read_csv(obj['Body'],low_memory=False, sep=';')
    print("read s3 to df")
    return df

def pandas_df_to_redshist(bucket_name, s3_file_name):
    df = s3_to_pandas_with_processing(bucket_name, s3_file_name)
    # Write the DataFrame to S3 and then to redshift
    pr.connect_to_s3(aws_access_key_id = KEY_ID,
                    aws_secret_access_key = KEY_SECRET,
                    bucket = bucket_name,
                    subdirectory = s3_file_name)
    ## to redshift
    pr.pandas_to_redshift(data_frame = df,
                        redshift_table_name = table_name)

## copy_mrr_data_to_ods
def copy_mrr_data_to_ods(dbcon, tablename):
    cur = dbcon.cursor()
    sql_drop = 'truncate table ' + tablename + ';'
    sql_param = 'createdAt'
    sql_create_table_as_select = r"insert into public.ods_iterable_integration_user select distinct * from public.mrr_iterable_integration_users_tag where experiments_0__id != ''"
    print(sql_create_table_as_select)
    ## drop table and create as select     
    cur.execute(sql_drop)
    con.commit()
    cur.execute(sql_create_table_as_select)
    con.commit()
    print('data from mrr to ods')
    ## copy + distinct
    dbcon.commit()
    cur.close()

# pivoting
def call_sp_pivot(dbcon):
    crr = dbcon.cursor()
    truncsql = 'truncate table public.ref_iterable_integration_user'
    crr.execute(truncsql)
    sql = 'call public.sp_get_pivot()'
    crr.execute(sql)
    dbcon.commit()
    crr.close()
    
## update log table finish
def update_DWH_ETL_log_finish(dbcon, tablename):
    crr = dbcon.cursor()
    sql = """update DWH_ETL_log set end_time=CURRENT_TIMESTAMP , action_status='finished running' 
             WHERE job_name = 'iterable_integration_users' 
             AND   end_time is null 
             AND is_current_cycle = 1;"""
    crr.execute(sql)
    dbcon.commit()
    crr.close()



if __name__ == '__main__':
    update_DWH_ETL_log_start(con, table_name);
    ## create table according to csv
    createTable(con, table_name);
    ## S3
    uploaded = upload_to_aws(local_file, bucket_name, s3_file_name);
    pandas_df_to_redshist(bucket_name, s3_file_name)
    ## from mrr to ods
    table_name_tag = 'ods_iterable_integration_user'
    copy_mrr_data_to_ods(con, table_name_tag)
    # pivot 
    call_sp_pivot(con)
    update_DWH_ETL_log_finish(con, table_name)
  

