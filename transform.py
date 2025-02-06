from pymongo import MongoClient
import mysql.connector
from dotenv import load_dotenv
from abc import ABC, abstractmethod
import pandas as pd
import os

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



class transform():
    def __init__(self):
        pass

def main():
    get_mongo_db_data = mongodb_data()
    get_mysql_data = mysql_data()
    df1 = get_mysql_data.load_data_as_df()
    df2 = get_mongo_db_data.load_data_as_df()


if __name__ == "__main__":
    main()
