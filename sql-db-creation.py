import pandas as pd
import psycopg2
import os

# Folder containing the CSV files
csv_folder = r'E:\GitHub Repos\immaculate-grid\lahman-data'

# Map pandas dtypes to PostgreSQL types
dtype_mapping = {
    'int64': 'INTEGER',
    'float64': 'REAL',
    'object': 'TEXT',
    'bool': 'BOOLEAN',
    'datetime64[ns]': 'TIMESTAMP'
}

# Connect to PostgreSQL
conn = psycopg2.connect(
    dbname="lahman-baseball",
    user="postgres",
    password="password",
    host="localhost",
    port="5432"
)
cur = conn.cursor()

# Iterate through all CSV files in the folder
for file_name in os.listdir(csv_folder):
    if file_name.endswith('.csv'):
        csv_file = os.path.join(csv_folder, file_name)
        table_name = os.path.splitext(file_name)[0]  # Use file name (without extension) as table name

        # Load the CSV file
        df = pd.read_csv(csv_file, encoding='latin1', delimiter=',', low_memory=False)

        # Infer column types
        columns = ', '.join([f'"{col}" {dtype_mapping[str(dtype)]}' for col, dtype in df.dtypes.items()])
        create_table_query = f'CREATE TABLE IF NOT EXISTS {table_name} ({columns});'

        # Create the table
        cur.execute(create_table_query)
        conn.commit()

        # Insert the data
        for _, row in df.iterrows():
            placeholders = ', '.join(['%s'] * len(row))
            insert_query = f'INSERT INTO {table_name} VALUES ({placeholders})'
            cur.execute(insert_query, tuple(row))

        conn.commit()
        print(f"Table '{table_name}' created and data from '{file_name}' imported successfully.")

cur.close()
conn.close()