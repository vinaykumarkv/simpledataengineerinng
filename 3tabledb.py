import pandas as pd
import psycopg2
from psycopg2 import sql

def create_table_from_csv(cur, table_name, csv_file_path, columns):
    # Create table if it does not exist
    create_table_query = sql.SQL("CREATE TABLE IF NOT EXISTS {} ({})").format(
        sql.Identifier(table_name),
        sql.SQL(', ').join(sql.SQL("{} {}").format(sql.Identifier(col), sql.SQL(dtype)) for col, dtype in columns.items())
    )
    try:
        cur.execute(create_table_query)
        conn.commit()
    except psycopg2.Error as e:
        print(f"Unable to create table {table_name}")
        print(e)

    # Read data from CSV file
    data = pd.read_csv(csv_file_path)

    # Insert data into the table
    insert_query = sql.SQL("INSERT INTO {} ({}) VALUES ({})").format(
        sql.Identifier(table_name),
        sql.SQL(', ').join(map(sql.Identifier, columns.keys())),
        sql.SQL(', ').join(sql.Placeholder() * len(columns))
    )
    try:
        for index, row in data.iterrows():
            cur.execute(insert_query, tuple(row))
        conn.commit()
    except psycopg2.Error as e:
        print(f"Unable to insert rows into {table_name}")
        print(e)

    # Select data from the table
    try:
        cur.execute(sql.SQL("SELECT * FROM {}").format(sql.Identifier(table_name)))
        rows = cur.fetchall()
        for row in rows:
            print(row)
    except psycopg2.Error as e:
        print(f"Unable to select from table {table_name}")
        print(e)

try:
    conn = psycopg2.connect("host=127.0.0.1 dbname=postgres user=postgres password=1234")
    cur = conn.cursor()
    conn.set_session(autocommit=True)

    # Create a new database
    try:
        cur.execute("CREATE DATABASE threetabledb")
        print("threetabledb created")
    except psycopg2.Error as e:
        print("Error: could not create database")
        print(e)
    finally:
        cur.close()
        conn.close()

    # Connect to the new database
    conn = psycopg2.connect("host=127.0.0.1 dbname=threetabledb user=postgres password=1234")
    cur = conn.cursor()

    # Define table structures
    customer_demography_columns = {
        "CustomerId": "INT",
        "age": "INT",
        "job": "VARCHAR",
        "marital": "VARCHAR",
        "education": "VARCHAR",
        "default": "VARCHAR",
        "balance": "INT",
        "housing": "VARCHAR",
        "loan": "VARCHAR",
        "contact": "VARCHAR",
        "day": "INT",
        "month": "VARCHAR",
        "duration": "INT",
        "campaign": "INT",
        "pdays": "INT",
        "previous": "INT",
        "poutcome": "VARCHAR",
        "Target": "VARCHAR"
    }

    customer_profiles_columns = {
        "CustomerId": "INT",
        "Surname": "VARCHAR",
        "CreditScore": "INT",
        "Geography": "VARCHAR",
        "Gender": "VARCHAR",
        "Age": "INT",
        "Tenure": "INT",
        "Balance": "FLOAT",
        "NumOfProducts": "INT",
        "HasCrCard": "INT",
        "IsActiveMember": "INT",
        "EstimatedSalary": "FLOAT",
        "Exited": "INT"
    }

    customer_complaints_columns = {
        "CustomerId": "INT",
        "Complain": "INT",
        "SatisfactionScore": "INT",
        "CardType": "VARCHAR",
        "PointEarned": "INT"
    }

    # Create tables and insert data
    create_table_from_csv(cur, "customer_demography", 'Datasets/bank-full.csv', customer_demography_columns)
    create_table_from_csv(cur, "customer_profiles", 'Datasets/Churn_Modelling.csv', customer_profiles_columns)
    create_table_from_csv(cur, "customer_complaints", 'Datasets/Customer-Churn-Records.csv', customer_complaints_columns)

except psycopg2.Error as e:
    print("Error: could not make connection to the database")
    print(e)
finally:
    if cur:
        cur.close()
    if conn:
        conn.close()
