import pandas as pd
from pymongo import MongoClient
import argparse 
import os
import json 
#from dotenv import load_dotenv
#load_dotenv()
from urllib.parse import urlparse
from pandas.io.json import json_normalize
from flatten_json import flatten
# import pdb

def read_mongo( uri, collection, query={}, projection={}, no_id=True, no_v=True):
    """ Read from Mongo and Store into DataFrame """
    database = urlparse(uri).path[1:] #remove first char /

    query = json.loads(query) #parse query variable from string to dict 
    # Connect to MongoDB
    db = MongoClient(uri, unicode_decode_error_handler='ignore')[database]

    # Make a query to the specific DB and Collection
    cursor = db[collection].find(query)

    # Expand the cursor and construct the DataFrame
    df =  pd.DataFrame(list(cursor))
    
    # Delete the _id
    if no_id and '_id' in df:
        del df['_id']
    if no_v and '__v' in df:
        del df['__v']
    return df

def argument_parser():
    parser = argparse.ArgumentParser(description='Load Data from MongoDB to CSV')
    parser.add_argument("--uri", default=os.environ.get('MONGO_URI', ''), help='connection string')
    parser.add_argument("--no_id", default=True, help='if false returns _id as well')
    parser.add_argument("--no_v", default=True, help='if false returns __v as well')
    parser.add_argument("--collection", help='choose collection')
    parser.add_argument("--query", default='{}', help='filter collection this is the argument inside find in mongodb query')
    parser.add_argument("--file", help='full file path to save')
    args = parser.parse_args()
    return args
    

if __name__ == '__main__':
    args = argument_parser()
    df = read_mongo(args.uri, args.collection, args.query, args.no_id,args.no_v)
    #print(df.to_dict('records'))
    dic_flattened = (flatten(d) for d in df.to_dict('records'))
    df = pd.DataFrame(dic_flattened)
    df.to_csv(args.file, index=False ,sep=';')
