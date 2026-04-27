import requests
import json
import pandas as pd
import numpy as np
import pymysql
import os
import datetime

import re

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

    url = "https://www4.tcsion.com/iONBizServices/iONWebService?servicekey=pSe9pdu8KGDGZ%2BhMIwZJ2A%3D%3D&s=Ki1CZzS40WkIARTjyfbphQ%3D%3D&u=hOXTgMCOjQMWlmQ1tQBrR1PSB%2BULPZgrSTZpDFEqmtQ%3D"

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
        "Site details - Site" : "Site",
        "Site Description" : "Site_Description",
        "Party Code" : "Party_Code",
        "Party Date(Date)" : "Party_Date",
        "GSTIN No" : "GSTIN_No",
        "Party Type" : "Party_Type",
        "Category" : "Category", 
        "Sales Region Code" : "Sales_Region_Code",
        "Sales Region Description" : "Sales_Region_Description",
        "PAN" : "PAN",
        "Party Group" : "Party_Group",
        "WF Status" : "WF_Status",
        "WF Status Change Date" : "Wf_Status_Change_Date"
        }
    
    df = df.rename(columns = column_mapping)
    return df

# -------------------- Clean Column Names --------------------
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
    folder_path = os.path.join(os.getcwd(), "party_data")
    if not os.path.exists(folder_path):
        os.makedirs(folder_path) 


    # Generate file with current date 
    current_date = datetime.datetime.now().strftime("%Y_%m_%d")
    file_name  = "Party_master_{current_date}.xlsx".format(current_date=current_date)
    file_path = os.path.join(folder_path, file_name)

    df = pd.DataFrame(data) 

    df = rename_api_columns(df)

    # Replace Nan with None
    df.replace({np.nan: None}, inplace=True)

    ## convert date columns 
    date_columns = [
        "Party_Date",
        "Wf_Status_Change_Date"
    ]

    for col in date_columns:
        if col in df.columns:

            df[col] = pd.to_datetime(df[col], errors='coerce', dayfirst=True).dt.strftime('%Y-%m-%d')
    
    # convert into excel file 
    df.to_excel(file_path, index=False)
    print("JSON data saved to Excel:", file_path)

    return file_path

# -------------------- Insert Data into MySQL --------------------
def insert_data_to_db(file_path):
    conn = get_connection()
    cursor = conn.cursor()

    df = pd.read_excel(file_path, engine='openpyxl')

    # Clean column names again (just to be safe)
    df = clean_column_names(df)

    # Replace NaN with None before DB insert
    df.replace({np.nan: None}, inplace=True)

    for date_col in ["Party_Date", "Wf_Status_Change_Date"]:
        if date_col in df.columns:
            df[date_col] = pd.to_datetime(df[date_col], errors='coerce').dt.strftime('%Y-%m-%d')

    table_name = "party_master_main"

    total_records = len(df)
    inserted_count = 0 
    skipped_count = 0 

    print("Total number of records {}".format(total_records))
    # print(df.)

    # for index,row in df.iterrows():
    #     print(row["Site"].strip(),row["Site_Description"].strip(),row["Party_Code"].strip())
    #     sql = "SELECT * FROM party_master_new_copy WHERE Site = %s AND Site_Description = %s AND Party_Code = %s"

        # pass
    for i, row in df.iterrows():
        try:
            # ---------Duplication check----------
            check_sql = "SELECT COUNT(*) FROM {} WHERE Site = %s AND Site_Description = %s AND Party_Code = %s".format(table_name)
            cursor.execute(check_sql, (row.get('Site'), row.get('Site_Description'), row.get('Party_Code')))
            exists = cursor.fetchone()['COUNT(*)']

            if exists > 0:
                skipped_count += 1
                #print(f"Skipping duplicate Party_Code: {row['Party_Code']}")
                print("Skipping duplicate Invoice_No: {}".format(row['Invoice_No']))

                continue

            # -------Inserting new record--------
            columns = ", ".join("`{}`".format(col) for col in df.columns)
            placeholders = ", ".join(["%s"] * len(df.columns))
            sql = "INSERT INTO {} ({}) VALUES ({})".format(table_name, columns, placeholders)
            cursor.execute(sql, tuple(row))
            inserted_count += 1

            
        except Exception as e:
            print("Data is present, Insert error on row {}:".format(i), e)

    conn.commit()
    cursor.close()
    conn.close()
    print("Data inserted into database successfully.")
    print("Total records processed: {}".format(total_records))
    print("Inserted records:{}".format(inserted_count))
    print("Skipped (duplicates):{}".format(skipped_count))

# -------------------- Main Execution --------------------
if __name__ == "__main__":
    data = fetch_api_data()
    if data:
        file_path = save_json_to_excel(data)
        insert_data_to_db(file_path)


        










    


