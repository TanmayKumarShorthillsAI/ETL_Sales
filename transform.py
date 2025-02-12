from pymongo import MongoClient
import mysql.connector
from dotenv import load_dotenv
from abc import ABC, abstractmethod
import pandas as pd
import os
import json
import numpy as np
# from load import load_validated_and_cleaned_data_in_mongo

load_dotenv()


class Data(ABC):
    def __init__(self):
        pass

    @abstractmethod
    def load_data_as_df(self):
        pass


class mongodb_data(Data):
    def __init__(self):
        self.mdb_password = os.getenv("mongodb_pass")
        self.uri = f"mongodb+srv://t2001kumar:{self.mdb_password}@cluster1.911mn.mongodb.net/?retryWrites=true&w=majority&appName=Cluster1"
        self.client = MongoClient(self.uri)

        try:
            self.client.admin.command("ping")
            print("You successfully connected to MongoDB!")
        except Exception as e:
            print(e)
            super().__init__()

    def get_client(self):
        return self.client

    def load_data_as_df(self):
        my_db = self.client["raw_sales_data"]
        my_col = my_db["sales_json"]

        all_data = my_col.find({}, {"_id": 0})

        raw_data = []
        for data in all_data:
            raw_data.append(data)

        try:
            df = pd.DataFrame(raw_data)
            print("MongoDB data loaded as dataframe")
        except Exception as e:
            print(e)
        # print(df.head())
        return df


class mysql_data(Data):
    def __init__(self):
        self.connection = mysql.connector.connect(
            host=os.getenv("host"),
            user=os.getenv("user"),
            password=os.getenv("password"),
            database=os.getenv("database"),
        )

        self.cursor = self.connection.cursor()
        super().__init__()

    def load_data_as_df(self):
        get_all_data_query = "SELECT * FROM sales_csv;"

        self.cursor.execute(get_all_data_query)
        raw_data = self.cursor.fetchall()
        column_names = raw_data[0]

        self.connection.commit()
        self.cursor.close()
        self.connection.close()

        try:
            df = pd.DataFrame(raw_data[1:], columns=column_names)
            print("MySQL data loaded as dataframe")
        except Exception as e:
            print(e)
        # print(df.head())

        return df


class tsv_data(Data):
    def __init__(self):
        super().__init__()

    def load_data_as_df(self):
        df = pd.read_csv("./input_files/sales_data.tsv", sep="\t")
        return df


class transform_sales_data:
    def __init__(self, df1, df2, df3):
        self.df1 = df1
        self.df2 = df2
        self.df3 = df3
        self.combined_df = pd.DataFrame([])

    def replace_with_nan(self, df):
        df = df.replace("", np.nan)

        return df

    def merge_and_drop_null_values(self):
        self.combined_df = pd.concat([self.df1, self.df2, self.df3])
        self.combined_df = self.replace_with_nan(self.combined_df)
        self.combined_df.dropna(inplace=True)

    def convert_data_types(self):
        self.combined_df["price"] = self.combined_df["price"].astype("float")
        self.combined_df["quantity"] = (
            self.combined_df["quantity"]
            .apply(lambda x: str(x).split(".")[0])
            .astype("int")
        )
        self.combined_df["customer_id"] = (
            self.combined_df["customer_id"]
            .apply(lambda x: str(x).split(".")[0])
            .astype("int")
        )

    def add_total_price_and_drop_duplicates(self):
        self.combined_df.drop_duplicates(inplace=True)
        self.combined_df["total_price"] = (
            self.combined_df["price"] * self.combined_df["quantity"]
        )

        # print(self.combined_df)
        return self.combined_df

    def payment_methods_info(self):
        payemnt_info_details = self.combined_df.groupby(
            by=["category", "payment_method"]
        ).agg(total_count=("customer_id", "count"))
        # print(payemnt_info_details)
        return payemnt_info_details


def load_validated_and_cleaned_data_in_mongo(df):
    mongo_obj = mongodb_data()
    client = mongo_obj.client

    df.reset_index(inplace=True)

    my_db = client["validated_and_cleaned_sales_data"]
    my_col = my_db["sales_data"]


    data = dict(df.to_json())

    my_col.insert_many(data)
    # print(data)

def main():
    get_mongo_db_data = mongodb_data()
    get_mysql_data = mysql_data()
    df1 = get_mysql_data.load_data_as_df()
    df2 = get_mongo_db_data.load_data_as_df()
    df3 = tsv_data().load_data_as_df()

    transform_sales_data_obj = transform_sales_data(df1, df2, df3)
    transform_sales_data_obj.merge_and_drop_null_values()
    transform_sales_data_obj.convert_data_types()
    cleaned_df = transform_sales_data_obj.add_total_price_and_drop_duplicates()
    print(cleaned_df)
    payment_info_df = transform_sales_data_obj.payment_methods_info()
    load_validated_and_cleaned_data_in_mongo(cleaned_df)


if __name__ == "__main__":
    main()
