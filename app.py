import os
from dotenv import load_dotenv
from clean import clean_log_data
from transform import transform_log
from load import read_gz_file, tranderjson, transfercsv, transferexcel, transfermysql, transferpostgresql



load_dotenv()

os.environ['FILEPATH'] = input('Enter the file path: ')

file = os.getenv('FILEPATH')

log_data = read_gz_file(file)
print('data read completed')

cleaned_data = clean_log_data(log_data)
print('Cleaning Process Completed')

data = transform_log(cleaned_data)



file_format = input("Enter the file format (csv, json, excel, database): ").strip().lower()


if file_format == 'csv':
    file_name = input('Enter File name: ')
    transfercsv(file_name,data)
elif file_format == 'json':
    file_name = input('Enter File name: ')
    tranderjson(file_name,dataframe)
elif file_format == 'excel':
    file_name = input('Enter File name: ')
    transferexcel(file_name,dataframe)
elif file_format == 'database':
    db = input('Enter the SQL Database (MySQL, PostgreSQL): ').strip().lower()

    if db == 'mysql':
        os.environ['USERNAME'] = input('Enter the username: ')
        os.environ['PASSWORD'] = input('Enter the password: ')
        os.environ['HOST'] = input('Enter the host: ')
        os.environ['DATABASE'] = input('Enter the database: ')
        transfermysql(dataframe)
    elif db == 'postgresql':
        os.environ['USERNAME'] = input('Enter the username: ')
        os.environ['PASSWORD'] = input('Enter the password: ')
        os.environ['HOST'] = input('Enter the host: ')
        os.environ['PORT'] = input('Enter the Port')
        os.environ['DATABASE'] = input('Enter the database: ')
        transferpostgresql(dataframe)
    else:
        print('Enter the Valid Database (MySQL, PostgreSQL)')

else:
    print("Unsupported file format")






