# data engineering project
"""
        *** Automated CSV to MySQL Data Pipeline with Python ***
"""

import pandas as pd # data processing
import glob
from sqlalchemy import create_engine

# Handling the exception to connect the database
try:
    # Creating the connecting URL
    engine = create_engine("mysql+mysqlconnector://root:2130@localhost/school")
except Exception as e:
    print(f"Check the user and passwd for the database login: {e}")

# Reading the data from the source folder
file_addres = glob.glob('data_to_load_db/*.csv')

# Handling the exception for data upload ELT
try:
    for i in file_addres:
        # Getting the file name
        file_name = i.split("\\")[-1].replace(".csv", "")
        file_name = file_name.strip()  # removing the white sacpe form the file name

        print(f"Inserting data into table: {file_name}")

        # Reading the data from the CSV file
        data = pd.read_csv(i)
        data.columns = data.columns.str.strip()  # removing the white sacpe form the column label

        # Sending data to the database in the form of chunks
        data.to_sql(name=file_name, con=engine, index=False, if_exists='append',chunksize=1000)
        print(f"Data from {file_name} inserted successfully.")

except Exception as e:
    print(f"Error occurred during file processing: {e}")
