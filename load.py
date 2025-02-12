from pymongo import MongoClient
import json
import os
import pandas as pd
from dotenv import load_dotenv
from transform import mongodb_data

load_dotenv()

def load_validated_and_cleaned_data_in_mongo(df):
    mongo_obj = mongodb_data()
    client = mongo_obj.client

    my_db = client["validated_and_cleaned_sales_data"]
    my_col = my_db["sales_data"]


    data = json.load(pd.to_json(df))
    print(data)
    # my_col.insert_many()
    # print(mongo_obj.client)


# load_validated_and_cleaned_data_in_mongo()