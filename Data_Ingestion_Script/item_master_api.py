import requests
import json
import pandas as pd
import numpy as np
import pymysql
import os
import datetime
import re
import openpyxl

# -------------------- Database Connection --------------------
def get_connection():
    conn = pymysql.connect(
        host="localhost",
        user="root",
    
        database="automatrix",
        charset="utf8mb4",
        cursorclass=pymysql.cursors.DictCursor
    )
    print("Database connection established.")
    return conn


# -------------------- API Fetch Function --------------------
def fetch_api_data():

    url = "https://www.tcsion.com/iONBizServices/iONWebService?servicekey=pSe9pdu8KGDGZ%2BhMIwZJ2A%3D%3D&s=GmssXBgd9hNmOhtxtJCayg%3D%3D&u=hOXTgMCOjQMWlmQ1tQBrR1PSB%2BULPZgrSTZpDFEqmtQ%3D"

    response = requests.get(url)

    if response.status_code == 200:
        data = response.json()
        print("API data fetched successfully.")
        return data 
    else:
        print("Failed to fetch data from ",response.status_code)
        return []
    
# -------------------- Rename Column Names --------------------
def rename_api_columns(df):
    column_mapping = {
        'Item_Code' : 'Item_Code',
        'Item_Description' : 'Item_Description',
        'Item_Name' : 'Item_Name',
        'Item_Category' : 'Item_Category',
        'Item_Group' : 'Item_Group',
        'Alternate_SW_Code' : 'Alternate_SW_Code',
        'Item_Type' : 'Item_Type',
        'Site' : 'Site',
        'Site_Description' : 'Site_Description',

    }

    df = df.rename(columns = column_mapping)
    return df

# -------------------- clean columns --------------------
def clean_column_names(df):
    clean_cols = []
    for col in df.columns:
        # Remove leading/trailing underscores and compress multiple underscores
        new_col = re.sub(r'_+', '_', col.strip('_'))
        clean_cols.append(new_col)
    df.columns = clean_cols
    return df

# -------------------- Save JSON to Excel --------------------
def save_json_to_excel(data):
    folder_path = os.path.join(os.getcwd(), "item_data")
    if not os.path.exists(folder_path):
        os.makedirs(folder_path) 

    # Generate file with current date
     
    current_date = datetime.datetime.now().strftime("%Y_%m_%d")
    file_name  = "Item_master_{current_date}.xlsx".format(current_date=current_date)
    file_path = os.path.join(folder_path, file_name)

    df = pd.DataFrame(data)

    df = rename_api_columns(df)

    # Replace Nan with None
    df.replace({np.nan: None}, inplace=True)

    # convert into excel file 
    df.to_excel(file_path, index=False)
    print("JSON data saved to Excel:", file_path)

    return file_path

# -------------------- Insert Data into MySQL --------------------
# def insert_data_to_db(file_path):
#     conn = get_connection()
#     cursor = conn.cursor()

#     df = pd.read_excel(file_path,engine="openpyxl")
    

    
#     # Clean column names again (just to be safe)
#     df = clean_column_names(df)

#     # Replace NaN with None before DB insert
#     df.replace({np.nan: None}, inplace=True)

#     table_name = "item_master_new_copy"

#     total_records = len(df)
#     inserted_records = 0
#     skipped_records = 0

#     for index, row in df.iterrows():
#     	try:
#     		check_sql = f"SELECT COUNT(*) AS cnt FROM {table_name} WHERE Item_Code=%s"
#     		cursor.execute(check_sql, (row.get('Item_Code'),))
#     		exists = cursor.fetchone()['cnt']
            
#             if exists > 0:
#             	skipped_records += 1
#             	continue

#             columns = ", ".join(f"`{col}`" for col in df.columns)
#         	placeholders = ", ".join(["%s"] * len(df.columns))
#         	insert_sql = f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders})"
#         	cursor.execute(insert_sql, tuple(row))
#         	inserted_records += 1

#         except Exception as e:

#             print("Duplicate data found {} This Data is alredy Present:".format(index), e)
    
#     conn.commit()
#     cursor.close()
#     conn.close()
#     print("Data inserted into MySQL successfully. Total: {}, Inserted: {}, Skipped: {}".format(
#         total_records, inserted_records, skipped_records))
def insert_data_to_db(file_path):
    conn = get_connection()
    cursor = conn.cursor()

    df = pd.read_excel(file_path, engine="openpyxl")

    # Clean column names again (just to be safe)
    df = clean_column_names(df)

    # Replace NaN with None before DB insert
    df.replace({np.nan: None}, inplace=True)

    table_name = "item_master_new_copy"

    total_records = len(df)
    inserted_records = 0
    skipped_records = 0

    for index, row in df.iterrows():
        try:
            # --------------- Duplication Check With Item_Code column  ------------------
            check_sql = f"SELECT COUNT(*) AS cnt FROM {table_name} WHERE Item_Code=%s AND Site=%s"
            cursor.execute(check_sql, (row.get('Item_Code'),row.get('Site')))
            exists = cursor.fetchone()['cnt']

            if exists > 0:
                skipped_records += 1
                continue

            # Insert new record
            columns = ", ".join(f"`{col}`" for col in df.columns)
            placeholders = ", ".join(["%s"] * len(df.columns))
            insert_sql = f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders})"

            cursor.execute(insert_sql, tuple(row))
            inserted_records += 1

        except Exception as e:
            print(f"Error inserting row {index}: {e}")

    conn.commit()
    cursor.close()
    conn.close()

    print(
        "Data inserted into MySQL successfully. Total: {}, Inserted: {}, Skipped: {}".format(
            total_records, inserted_records, skipped_records
        )
    )

    
# -------------------- Main Function --------------------
if __name__ == "__main__":
    data = fetch_api_data()
    if data:
        file_path = save_json_to_excel(data)
        insert_data_to_db(file_path)

















