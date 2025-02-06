from pymongo import MongoClient
import json
import os
import csv
from dotenv import load_dotenv
import mysql.connector

load_dotenv()


def insert_into_mongo():
    mdb_password = os.getenv("mongodb_pass")
    # print(mdb_password)
    uri = f"mongodb+srv://t2001kumar:{mdb_password}@cluster1.911mn.mongodb.net/?retryWrites=true&w=majority&appName=Cluster1"
    client = MongoClient(uri)

    try:
        client.admin.command("ping")
        print("You successfully connected to MongoDB!")
    except Exception as e:
        print(e)

    my_db = client["raw_sales_data"]
    my_col = my_db["sales_json"]

    with open("./input_files/sales_data.json", "r") as file:
        data = json.load(file)

    # print(type(data), data[:5])

    inserted_records = my_col.insert_many(data)

    print(inserted_records)


def insert_into_mysql():
    connection = mysql.connector.connect(
        host=os.getenv("host"),
        user=os.getenv("user"),
        password=os.getenv("password"),
        database=os.getenv("database"),
    )

    cursor = connection.cursor()

    create_table_query = """
        CREATE TABLE IF NOT EXISTS sales_csv (
            transaction_id VARCHAR(225),
            date VARCHAR(225),
            customer_id VARCHAR(225),
            product VARCHAR(225),
            category VARCHAR(225),
            quantity VARCHAR(225),
            price VARCHAR(225),
            payment_method VARCHAR(225)
        );     
    """

    cursor.execute(create_table_query)
    csv_path = "./input_files/sales_data.csv"

    dict_list = list()
    with open(csv_path, mode="r") as csv_reader:
        csv_reader = csv.reader(csv_reader)
        for rows in csv_reader:
            dict_list.append(
                {
                    "transaction_id": rows[0],
                    "date": rows[1],
                    "customer_id": rows[2],
                    "product": rows[3],
                    "category": rows[4],
                    "quantity": rows[5],
                    "price": rows[6],
                    "payment_method": rows[7],
                }
            )
    insert_records_query = """
            INSERT INTO sales_csv(transaction_id, date, customer_id, product, category, quantity, price, payment_method)
            VALUES (%s, %s, %s, %s, %s, %s,%s, %s)
            """

    for x in dict_list:
        values = (
            x["transaction_id"],
            x["date"],
            x["customer_id"],
            x["product"],
            x["category"],
            x["quantity"],
            x["price"],
            x["payment_method"],
        )

        try:
            cursor.execute(insert_records_query, values)
        except Exception as e:
            print(e)

    try:
        cursor.execute(create_table_query)
        connection.commit()
    except Exception as e:
        print(e)

    cursor.close()
    connection.close()


def main():
    insert_into_mongo()
    # insert_into_mysql()


if __name__ == "__main__":
    main()
