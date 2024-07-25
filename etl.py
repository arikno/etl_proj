# etl.py
import pandas as pd
from sqlalchemy import create_engine

# Source DB
source_engine = create_engine('mysql+mysqlconnector://root:root@localhost:3306/source_db')

# Target DB
target_engine = create_engine('postgresql+psycopg2://postgres:password@localhost:5432/target_db')

def extract():
    # Extract data from MySQL
    df = pd.read_sql('SELECT * FROM source_table', source_engine)
    df.to_csv('/tmp/source_data.csv', index=False)

def load():
    # Load data into PostgreSQL
    df = pd.read_csv('/tmp/source_data.csv')
    df.to_sql('raw_table', target_engine, if_exists='replace', index=False)

def run_etl():
    extract()
    load()
