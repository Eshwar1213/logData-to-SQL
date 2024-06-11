import gzip
import pandas as pd
from sqlalchemy import create_engine, MetaData, Table, Column, Integer, DateTime, VARCHAR, TEXT
from sqlalchemy.ext.declarative import declarative_base
import os
from dotenv import load_dotenv


load_dotenv()

columnData = ['cp_id', 'real_time', 'received_time', 'message_id', 'message_flow', 'call_action', 'call_payLoad']


def read_gz_file(file_path):
    try:
        with gzip.open(file_path, 'rt') as f:
            file_content = f.read()
        return file_content
    except FileNotFoundError:
        print("File not found. Please check the path and try again.")
        return None


def transfermysql(df):
    dataframe = pd.DataFrame(df, columns=columnData)

    db_config = {
        'user': os.getenv('USERNAME'),
        'password': os.getenv('PASSWORD'),
        'host': os.getenv('HOST'),
        'database': os.getenv('DATABASE')
    }


    engine = create_engine(f'mysql+pymysql://{db_config["user"]}:{db_config["password"]}@{db_config["host"]}/{db_config["database"]}')

    metadata = MetaData()

    table_name = 'log_datas'  
    
    table = Table(
        table_name, metadata,
        Column('cp_id', Integer, primary_key=True),
        Column('real_time', DateTime),
        Column('received_time', DateTime),
        Column('msg_ig', VARCHAR(255)),
        Column('msg_flow', VARCHAR(255)),
        Column('call_action', VARCHAR(255)),
        Column('call_payload', TEXT)
    )

    metadata.create_all(engine)

    with engine.connect() as conn:
        dataframe.to_sql(table_name, conn, if_exists='replace', index=False, method='multi')
        print(f"Data inserted to {db_config['database']} database successfully")


    engine.dispose()


def transferpostgresql(df):
    
    db_config = {
        'user': os.getenv('USERNAME'),
        'password': os.getenv('PASSWORD'),
        'host': os.getenv('HOST'),
        'database': os.getenv('DATABASE'),
        'port': os.getenv('PORT')
    }


    engine = create_engine(f'postgresql://{db_config["user"]}:{db_config["password"]}@{db_config["host"]}:{db_config["port"]}/{db_config["database"]}')
    
    metadata = MetaData()

    table_name = 'log_datas'
    table = Table(
        table_name, metadata,
        Column('cp_id', Integer, primary_key=True),
        Column('real_time', DateTime),
        Column('received_time', DateTime),
        Column('msg_ig', VARCHAR(255)),
        Column('msg_flow', VARCHAR(255)),
        Column('call_action', VARCHAR(255)),
        Column('call_payload', TEXT)
    )


    metadata.create_all(engine)

    with engine.connect() as connection:
        dataframe.to_sql(name=table_name, con=connection, if_exists='replace', index=False)
        print(f"Data inserted to {db_config['database']} database successfully")

    engine.dispose()


def transfercsv(filename, df):

    df.to_csv(filename, index=False)
    print(f"Data written to {filename} successfully.")


def transferexcel(filename, df):

    df.to_excel(filename, index=False)
    print(f"Data written to {filename} successfully.")

def tranderjson(filename, df):

    df.to_json(filename, orient='records', indent=4)
    print(f"Data written to {filename} successfully.")