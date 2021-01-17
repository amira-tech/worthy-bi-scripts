import pandas as pd
from sqlalchemy import create_engine
import configparser
from datetime import datetime
import psycopg2
import s3fs
import os
import argparse

# read values from ini
config = configparser.ConfigParser(interpolation=None)
config.read("//home//ec2-user//scripts//Talend_2_python//config//mrr_config.ini")

## AWS_CONFIG_FILE
KEY_ID = config.get('AWS_CONFIG_FILE', 'AWS_ACCESS_KEY_ID')
KEY_SECRET = config.get('AWS_CONFIG_FILE', 'AWS_ACCESS_KEY_SECRET')

##  REDSHIFT_ADMIN
username_red = config.get('REDSHIFT_ADMIN', 'User')
password_red = config.get('REDSHIFT_ADMIN', 'Pass')

## source: POSTGRESQL
postgres_database = config.get('postgres_a', 'DATABASE')
postgres_host = config.get('postgres_a', 'HOST')
postgres_port = config.get('postgres_a', 'PORT')
postgres_user = config.get('postgres_a', 'USER')
postgres_password = config.get('postgres_a', 'PASSWORD')

# Global parameters
src_table_name = 'bundle_flags'
src_schema_name = 'public'

tgt_table_name = 'mrr_bundle_flags_tag'
tgt_schema_name = 'mrr'

dwh_tgt_incremental_column = 'updated_at'
dwh_tgt_table_name = 'ods_bundle_flags'
dwh_tgt_schema_name = 'public'

bucket_name = 'worthy-etl'
s3_file_name = '/etl/mrr_bundle_flags_tag.csv'

file_path = '//home//ec2-user//scripts//Talend_2_python//Step_1_Source_To_Mirror_Part_11//increment//mrr_bundle_flags_tag.csv'
src_schema_table_name = src_schema_name + '.' + src_table_name

# connections
connstr_post = 'postgresql+psycopg2://' + postgres_user + ':' + postgres_password + '@' + postgres_host + ':' + postgres_port + '/' + postgres_database
connstr_red = 'redshift+psycopg2://' + username_red + ':' + password_red + '@redshift-cluster-1.coczxpfcmk83.us-east-1.redshift.amazonaws.com:5439/dwh_dev'
con_red = psycopg2.connect(dbname='dwh_dev', host='redshift-cluster-1.coczxpfcmk83.us-east-1.redshift.amazonaws.com',
                           port='5439', user=username_red, password=password_red)

## create engines
red_engine = create_engine(connstr_red)
alchemyEngine = create_engine(connstr_post)
## connect to postgressql
dbConnection = alchemyEngine.connect()

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

## get is_full : is full run
def argument_parser():
    parser = argparse.ArgumentParser(description='get boolean full run')
    parser.add_argument("--is_full", help='0')
    args = parser.parse_args()
    return args

# read to max incremental column from the target (dwh redshift) if our dwh target table is empty take the incremental date 5 days back
def read_redshift_incremental_param(schema_name, table_name, incremental_column, v_engine):
    sql = 'SELECT max(' + incremental_column + ') as max_admin_creation_time FROM ' + schema_name + '.' + table_name + ' da '
    table_list = ['histories', 'item_properties', 'versions', 'user_actions_histories']
    data = pd.read_sql_query(sql, v_engine)
    incremental_date = data["max_admin_creation_time"].to_csv(None, header=False, encoding="utf-8", index=False)
    # check null
    if (len(
            incremental_date) < 10 or incremental_date is None or incremental_date == "" or incremental_date == '""') and (
            table_name in table_list):
        sql_none = "select  date_trunc('day',dateadd(day,-5,getdate())) as max_admin_creation_time"
        data = pd.read_sql_query(sql_none, v_engine)
        incremental_date = data["max_admin_creation_time"].to_csv(None, header=False, encoding="utf-8", index=False)
    if (len(
            incremental_date) < 10 or incremental_date is None or incremental_date == "" or incremental_date == '""') and (
            table_name not in table_list):
        incremental_date = '1900-01-01 00:00:00.01'
    print(incremental_date)
    return incremental_date


def read_sql_max_rows(schema_table_name, conn):
    sql_max = "SELECT count(*) as rows_count FROM {} ".format(str(schema_table_name))
    max_row = pd.read_sql_query(sql_max, conn)
    rows_count = max_row["rows_count"].to_csv(None, header=False, encoding="utf-8", index=False)
    return int(rows_count)


def read_sql_chunked(schema_table_name, conn, nrows, chunksize, file):
    # args
    sql = "SELECT * FROM {} ".format(str(schema_table_name))
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
        print("Events added: %s to %s of %s" % (start - chunksize, start, nrows))


# load table from postgresql to df according to source update date in front of incremental column
def df_load_src_data(schema_name, table_name, updated_at, db_Connection):
    sql_qur = """
       SELECT * 
       FROM {0}.{1} 
       WHERE updated_at > '{2}'""".format(schema_name.replace('\'', '\'\''), table_name.replace('\'', '\'\''),
                                          updated_at.replace('\'', '\'\''))
    df1 = pd.read_sql_query(sql_qur, db_Connection)
    return df1


# load df to s3 using df
def df_to_s3(v_bucket, v_s3_file, data_frame):
    data = data_frame
    bytes_to_write = data.to_csv(None, header=False, encoding="utf-8", index=False).encode()
    fs = s3fs.S3FileSystem(key=KEY_ID, secret=KEY_SECRET)
    with fs.open('s3://' + v_bucket + v_s3_file, 'wb') as f:
        f.write(bytes_to_write)
    print('data loaded from df to s3 as csv')


## from s3 to redshift
def copy_s3_data_to_redshift(dbcon, tgt_schema, tgt_tablename, s3_file):
    tablename = tgt_schema + '.' + tgt_tablename
    cur = dbcon.cursor()
    pre_sql = ' truncate ' + tablename + ' ;'
    cur.execute(pre_sql)
    con_red.commit()
    sql = """copy {}
             from 's3://worthy-etl{}'
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
    update_DWH_ETL_log_start(con_red,tgt_table_name)
    datetime_object = datetime.now()
    print("Start TimeStamp read ")
    print("---------------")
    print(datetime_object)
    print("")
    # get incremental date from dwh layer
    updated_at = read_redshift_incremental_param(dwh_tgt_schema_name, dwh_tgt_table_name, dwh_tgt_incremental_column, red_engine)
    print(updated_at)
    # get incremental data frame from postgres according to incremental date

    # Get is full run according to day of the week
    args = argument_parser()
    v_is_full = args.is_full
    if v_is_full is None or v_is_full == "":
       v_is_full = 0
    print(v_is_full)

    if updated_at != '1900-01-01 00:00:00.01' and v_is_full == '0':
        df = df_load_src_data(src_schema_name, src_table_name, updated_at, dbConnection)
        print(df.head(n=10))
        dbConnection.close()
        # load df to s3
        df_to_s3(bucket_name, s3_file_name, df)
        # load df to redshift
        copy_s3_data_to_redshift(con_red, tgt_schema_name, tgt_table_name, s3_file_name)
    if updated_at == '1900-01-01 00:00:00.01' or v_is_full == '1':
        # load data to excel
        read_sql_chunked(src_schema_table_name, dbConnection, 1000000 * 4, 350000, file_path)
        # load df to s3
        # upload_to_aws
        data = pd.read_csv(file_path)
        df_to_s3(bucket_name, s3_file_name, data)
        # copy_s3_data_to_redshift
        copy_s3_data_to_redshift(con_red, tgt_schema_name, tgt_table_name, s3_file_name)
        # clear file if exist
        try:
            os.remove(file_path)
        except OSError:
            pass

    datetime_object = datetime.now()
    print("End TimeStamp read ")
    print("---------------")
    print(datetime_object)
    print("")
    update_DWH_ETL_log_finish(con_red, tgt_table_name)
