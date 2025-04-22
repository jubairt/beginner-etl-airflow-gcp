import pandas as pd
import sqlite3



input_file = "/home/airflow/gcs/data/User_details.csv"
output_file = "/home/airflow/gcs/data/Cleaned_user_details.csv"
sqlite_db = "/home/airflow/gcs/data/Cleaned_user_details_sqlite.db"


df = pd.read_csv(input_file) # Loading the data



df = df.dropna() # Droping the rows where the columns have null values


df.to_csv(output_file, index=False) # Creating new csv file with cleaned data


conn = sqlite3.connect(sqlite_db) # Connecting to sqlite
df.to_sql('cleaned_data', conn, if_exists='replace', index=False) # Converting data into sqlite format
conn.close() # Closing the connection

print("ETL complete: data loaded to SQLite.")
