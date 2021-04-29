import os
import os.path
import csv
from datetime import datetime
import logging
import pandas as pd
import pyodbc
import sqlalchemy as sal
from sqlalchemy import create_engine
from sqlalchemy.engine import URL
import decimal as d
import time
import numpy as np
import email_service as es
import yaml
import configparser

# Logging configuration
logging.basicConfig(filename='logs/ingest.log', filemode='a', format='%(asctime)s - [%(levelname)s] - %(message)s',
                    level=logging.DEBUG, datefmt='%d-%b-%y %H:%M:%S')


# Global Variables
diff_count = 0


# Get the connection info for the Local DB
def get_local_database_info():
    config = configparser.ConfigParser()
    config.read('devconfig.ini')

    server_name = config['LocalDatabase']['Server']
    db_name = config['LocalDatabase']['Database']

    return server_name, db_name


# Get the connection info for the AWS db instance
def get_aws_db_info():
    config = configparser.ConfigParser()
    config.read('config.ini')

    server_name = config["AWS"]["Server"]
    db_name = config["AWS"]["Database"]
    username = config["AWS"]["Username"]
    password = config["AWS"]["Password"]

    return server_name, db_name, username, password


# Get the current time in order to to some time keeping in the main function
def get_time():
    return time.time()


# Check to make sure the file exists and return a bool based on that.
def check_for_file():
    current_year = datetime.today().strftime('%Y')
    today = datetime.today().strftime('%d-%b-%Y')

    global file_name
    file_name = f'C:/Users/Ricky/Desktop/LandingZone/01-Jan-{current_year}_to_{today}.csv'

    return os.path.exists(file_name)


# This function will grab the current state of the DB and return a data frame.
# Possibly add some validation that if the db returns no records the progrma throws a critical error.
def get_current_records():
    db_info = get_aws_db_info()
    # conn = pyodbc.connect(
    #     'Driver={SQL Server Native Client 11.0};'f'Server={db_info[0]};'f'Database={db_info[1]};''Trusted_Connection=yes;')
    conn = pyodbc.connect("Driver={ODBC Driver 17 for SQL Server};"
                          f"Server={db_info[0]};"
                          f"Database={db_info[1]};"
                          f"uid={db_info[2]};pwd={db_info[3]}")
    cursor = conn.cursor()

    query = "Select * FROM Purchase"

    cursor.execute(query)
    data = cursor.fetchall()

    # Make the dataframe from the sql results
    current_records_df = pd.read_sql(
        query, conn)
    cursor.close()
    # Clean the order date from the current_records_df to match is with the ingested df
    current_records_df = current_records_df.drop(columns=['OrderNumber'])

    return current_records_df


# Make a data frame with the Amazon CSV file.
def amazon_ingest_data_frame():
    data = pd.read_csv(file_name)

    # make a data frame with the csv file
    purchase_df = pd.DataFrame(
        data, columns=['Order Date', 'Order ID', 'Title', 'Category', 'Item Total'])

    # Remove the Dollar sign from the Item Total column so that the DB will accept the float
    purchase_df['Item Total'] = purchase_df['Item Total'].replace(
        {'\$': ''}, regex=True)

    # These are the category's that should be included in the data Frame
    purchase_df = purchase_df[(purchase_df.Category == 'OFFICE_PRODUCTS') |
                              (purchase_df.Category == 'RAW_MATERIALS') |
                              (purchase_df.Category == 'MARKING_PEN') |
                              (purchase_df.Category == 'JANITORIAL_SUPPLY')]
    return purchase_df


# Compare the amazon dat frame to the current data that is in the DB. Will return a dataframe that will be ready to be insterted into the DB.
def compare_records():
    # Bool to keep track if the df is populated
    is_empty = True
    # Get both data frames for comparison
    purchase_df = amazon_ingest_data_frame()
    current_db_state = get_current_records()

    # Lambda function to compare the two data frames
    outliers = set(current_db_state['A_OrderId']
                   ).symmetric_difference(purchase_df['Order ID'])
    outlier_items = purchase_df[purchase_df['Order ID'].isin(outliers)]
    # Count of the difference of items
    index = outlier_items.index
    diff_count = len(index)

    if diff_count == 0:
        is_empty = True
        return outlier_items, diff_count, is_empty
    else:
        # Return the iems that are not in the data base in a data frame.
        is_empty = False
        return outlier_items, diff_count, is_empty


def insert_data_frame(df):
    db_info = get_aws_db_info()
    connection_string = (
        'Driver={SQL Server Native Client 11.0};''Server=localhost\sqlexpress;''Database=EtsyBusinessMetricsDevelopment;''Trusted_Connection=yes;')
    AWS_connection_string = ("Driver={ODBC Driver 17 for SQL Server};"
                             f"Server={db_info[0]};"
                             f"Database={db_info[1]};"
                             f"uid={db_info[2]};pwd={db_info[3]}")

    # Insert statemnt
    insert_records = '''INSERT INTO Purchase(OrderDate,A_OrderId,Title,Category,ItemTotal) VALUES(?,?,?,?,?)'''

    conn = pyodbc.connect(AWS_connection_string)

    cursor = conn.cursor()

    for index, row in df.iterrows():
        cursor.execute(insert_records, row[0], row[1],
                       row.Title, row.Category, str(row[4]))

    conn.commit()
    cursor.close()

    # data = pd.read_sql("SELECT * FROM EtsySale", connection_string)


def main():
    # Get the amount of time the script takes to run
    start_time = get_time()
    # Check to see if the file exixts in the folder and assign it to a bool
    file_exists = check_for_file()

    if file_exists == True:
        # This will be the returned value of compare records [0] = outliers data frame | [1] = diff_count | [2] = is_empty bool
        record_data = compare_records()
        if record_data[2] == True:
            end_time = get_time()
            total_time = end_time - start_time
            logging.info(
                f'The total time taken for the script is {round(total_time, 3)} seconds. No new rows were added to the database.')
            pass
        else:
            insert_data_frame(record_data[0])

            end_time = get_time()
            total_time = end_time - start_time
            es.send_success_email(diff_count, round(total_time, 3))
            logging.info(
                f'The total time taken for the script is {round(total_time, 3)} seconds. There were {record_data[diff_count]} new Records added.')
    else:
        logging.critical(
            'No File was found in the Landig Zone. An email has been sent please upload the file and re run the script.')
        es.send_ingest_failure_email()


if __name__ == "__main__":
    main()
