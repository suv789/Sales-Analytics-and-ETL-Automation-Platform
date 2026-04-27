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


# -------------------- API Fetch Data -------------------------
def fetch_api_data():
    url = "https://www4.tcsion.com/iONBizServices/iONWebService?servicekey=pSe9pdu8KGDGZ%2BhMIwZJ2A%3D%3D&s=w1hTYOdbpToRuxaaWRUe%2Fg%3D%3D&u=hOXTgMCOjQMWlmQ1tQBrR1PSB%2BULPZgrSTZpDFEqmtQ%3D"
    
    response = requests.get(url)

    if response.status_code == 200:
        print("API Data Fetch Successfully")
        return response.json()
    else:
        print("FAILED to fetch API Data:", response.status_code)
        return None


# -------------------- Rename Columns -------------------------
def rename_columns(df):
    column_mapping = {
        "Sales Return Number": "Sales_Return_Number",
        "Date(Date)": "Return_Date",
        "Site": "Site",
        "Customer Code": "Customer_Code",
        "Customer Name": "Customer_Name",
        "Customer Category Description": "Customer_Category_Description",
        "Status": "Status",
        "Original Invoice Number": "Original_Invoice_Number",
        "Original Invoice Date": "Original_Invoice_Date",
        "Customer Invoice No": "Customer_Invoice_No",
        "Customer Invoice Date": "Customer_Invoice_Date",
        "Item Code": "Item_Code",
        "Item Description": "Item_Description",
        "Return Qty": "Return_Qty",
        "Assessable Value": "Assessable_Value",
        "Item Net Amount": "Item_Net_Amount"
    }
    return df.rename(columns=column_mapping)


# -------------------- Clean Column Names ----------------------
def clean_column_names(df):
    df.columns = [col.replace(" ", "_") for col in df.columns]
    return df


# -------------------- Fix Date Format -------------------------
def fix_date(value):
    if pd.isna(value):
        return None
    try:
        return pd.to_datetime(value).strftime("%Y-%m-%d")
    except:
        return None


# -------------------- Save API JSON to Excel ------------------
def save_json_to_excel(data):
    folder = os.path.join(os.getcwd(), "sales_return_dump")
    os.makedirs(folder, exist_ok=True)

    today = datetime.datetime.now().strftime("%Y-%m-%d")
    file_path = os.path.join(folder, f"sales_return_{today}.xlsx")

    df = pd.DataFrame(data)
    df = rename_columns(df)
    df.replace({np.nan: None}, inplace=True)

    df.to_excel(file_path, index=False, engine="openpyxl")
    print(f"Saved API data to Excel → {file_path}")

    return folder  # return folder, NOT file path


# -------------------- Insert Excel Files to DB ----------------
def insert_into_db(folder_path):

    conn = get_connection()
    cursor = conn.cursor()

    table_name = "sales_return_main"
    excel_files = [f for f in os.listdir(folder_path) if f.endswith(".xlsx")]

    print(f"Found {len(excel_files)} Excel files.")

    for file in excel_files:

        file_path = os.path.join(folder_path, file)
        print(f"\nProcessing: {file}")

        df = pd.read_excel(file_path, engine="openpyxl")
        df = rename_columns(df)
        df = clean_column_names(df)
        df.replace({np.nan: None}, inplace=True)

        # Fix date columns
        for col in ["Return_Date", "Original_Invoice_Date", "Customer_Invoice_Date"]:
            if col in df.columns:
                df[col] = df[col].apply(fix_date)

        valid_columns = [
            "Sales_Return_Number", "Return_Date", "Site", "Customer_Code",
            "Customer_Name", "Customer_Category_Description", "Status",
            "Original_Invoice_Number", "Original_Invoice_Date",
            "Customer_Invoice_No", "Customer_Invoice_Date",
            "Item_Code", "Item_Description", "Return_Qty",
            "Assessable_Value", "Item_Net_Amount"
        ]

        inserted = skipped = 0

        for _, row in df.iterrows():

            # Auto-Fill Missing Original Invoice Number
            if not row["Original_Invoice_Number"]:
                row["Original_Invoice_Number"] = row["Customer_Invoice_No"]

            # Duplicate check
            cursor.execute(
                f"SELECT COUNT(*) AS c FROM {table_name} WHERE Original_Invoice_Number=%s AND Item_Code=%s",
                (row["Original_Invoice_Number"], row["Item_Code"])
            )
            if cursor.fetchone()['c'] > 0:
                skipped += 1
                continue

            row_data = [row.get(col) for col in valid_columns]
            placeholders = ", ".join(["%s"] * len(valid_columns))

            insert_sql = f"""
                INSERT INTO {table_name} ({", ".join(valid_columns)})
                VALUES ({placeholders})
            """
            cursor.execute(insert_sql, row_data)
            inserted += 1

        print(f"Inserted: {inserted}, Skipped: {skipped}")

    conn.commit()
    cursor.close()
    conn.close()
    print("\nAll Excel files inserted successfully!")


# -------------------- MAIN --------------------
if __name__ == "__main__":
    data = fetch_api_data()

    if data:
        folder_path = save_json_to_excel(data)   # returns folder
        insert_into_db(folder_path)
