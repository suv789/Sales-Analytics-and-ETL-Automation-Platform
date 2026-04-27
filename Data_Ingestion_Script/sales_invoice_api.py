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
    url = "https://www4.tcsion.com/iONBizServices/iONWebService?servicekey=X6zX5sVCvLAohIFSHSMW4g%3D%3D&s=4eDkV4f%2B0ZWud0nSvn9T5A%3D%3D&u=hOXTgMCOjQMWlmQ1tQBrR1PSB%2BULPZgrSTZpDFEqmtQ%3D"
    response = requests.get(url)

    if response.status_code == 200:
        data = response.json()
        print("API data fetched successfully.")
        return data
    else:
        print(" Failed to fetch data from API:", response.status_code)
        return []


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
    folder_path = os.path.join(os.getcwd(), "Invoice_data")
    if not os.path.exists(folder_path):
        os.makedirs(folder_path)

    # Generate file with current date
    current_date = datetime.datetime.now().strftime("%Y_%m_%d")
    file_name = "Invoice_{current_date}.xlsx".format(current_date=current_date)
    file_path = os.path.join(folder_path, file_name)

    df = pd.DataFrame(data)

    # Clean column names
    df = clean_column_names(df)

    # Replace NaN with None (for DB compatibility)
    df.replace({np.nan: None}, inplace=True)

    # Convert date columns safely
    date_columns = [
        "Customer_PO_Date",
        "Invoice_Date",
        "Removal_Date"
    ]

    for col in date_columns:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors='coerce', dayfirst=True).dt.strftime('%Y-%m-%d')

    df.to_excel(file_path, index=False)
    print("JSON data saved to Excel:", file_path)

    return file_path




def insert_data_to_db(file_path):
    conn = get_connection()
    cursor = conn.cursor()

    df = pd.read_excel(file_path, engine='openpyxl')
    df = clean_column_names(df)

    df.columns = df.columns.str.strip()

    df.rename(columns={
                "SUM(Item Amount)": "Without_GST_Amount",
                "SUM(Item_Amount)": "Without_GST_Amount"
            },
            inplace=True
        )

    # Handle NaN values safely
    df.replace({np.nan: None}, inplace=True)

    # Convert date columns for MySQL
    for date_col in ["Customer_PO_Date", "Invoice_Date", "Removal_Date"]:
        if date_col in df.columns:
            df[date_col] = pd.to_datetime(df[date_col], errors='coerce').dt.strftime('%Y-%m-%d')

    table_name = "sales_invoice_main"

    total_records = len(df)
    inserted_count = 0
    skipped_count = 0

    for i, row in df.iterrows():
        try:

            values = [None if pd.isna(x) else x for x in row]
            check_sql = f"SELECT COUNT(*) AS cnt FROM {table_name} WHERE Invoice_No = %s AND Item_Code = %s AND SUM_Bill_Qty = %s"
            cursor.execute(check_sql, (row['Invoice_No'], row['Item_Code'],row['SUM_Bill_Qty']))
            exists = cursor.fetchone()['cnt']

            if exists > 0:
                skipped_count += 1
                print(f"Skipping duplicate Invoice_No={row['Invoice_No']} | Item_Code={row['Item_Code']} | SUM_Bill_Qty={row['SUM_Bill_Qty']}")
                continue

            # Insert new record
            columns = ", ".join(f"`{col}`" for col in df.columns)
            placeholders = ", ".join(["%s"] * len(df.columns))
            sql = f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders})"
            #cursor.execute(sql, tuple(row))
            cursor.execute(sql,values)
            inserted_count += 1
            columns = ", ".join(f"`{col}`" for col in df.columns)
            print(f"Inserted: Invoice_No={row['Invoice_No']} | Item_Code={row['Item_Code']} | SUM_Bill_Qty={row['SUM_Bill_Qty']}")

        except Exception as e:
            print(f"Insert error on row {i}: {e}")

    conn.commit()
    cursor.close()
    conn.close()

    print("\nSummary:")
    print(f"Total records processed: {total_records}")
    print(f"Inserted: {inserted_count}")
    print(f"Skipped (duplicates): {skipped_count}")


# -------------------- Main Execution --------------------
if __name__ == "__main__":
    data = fetch_api_data()
    if data:
        excel_path = save_json_to_excel(data)
        insert_data_to_db(excel_path)
