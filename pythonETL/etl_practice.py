import glob 
import pandas as pd
from datetime import datetime
log_file = "log_file.txt"
target_file = "transformed_data.csv"

def extract_from_csv(file_to_process):
    dataframe = pd.read_csv(file_to_process)
    return dataframe

def extract_from_json(file_to_process):
    dataframe = pd.read_json(file_to_process,lines=True)
    return dataframe

def extract_from_xml(file_to_process):
    dataframe = pd.read_xml(file_to_process)
    return dataframe

def extract():
    extracted_file = pd.DataFrame(columns=['car_model','year_of_manufacture','price','fuel'])
    extracted_file.index.name="car_id"
    for csv_file in glob.glob('*.csv'):
        extracted_file = pd.concat([extracted_file,extract_from_csv(csv_file)],ignore_index=True)
    
    for json_file in glob.glob('*.json'):
        extracted_file = pd.concat([extracted_file,extract_from_json(json_file)],ignore_index=True)

    for xml_file in glob.glob('*.xml'):
        extracted_file = pd.concat([extracted_file,extract_from_xml(xml_file)],ignore_index=True)
    
    return extracted_file

def transform(data):
    data['price'] = round(data.price,2)
    return data

def load(target_file,tranformed_file):
    tranformed_file.to_csv(target_file)

def log_progress(message):
    timestamp_format = '%Y-%h-%d-%H:%M:%S'
    now = datetime.now()
    timestamp = now.strftime(timestamp_format)
    with open(log_file,"a") as f:
        f.write(f"{timestamp},{message}\n")


log_progress("ETL process started")

log_progress("Extract process started")
extracted_data = extract()
extracted_data.index.name = 'car_id'
log_progress("Extract process started")

log_progress("Transform process started")
transformed_data = transform(extracted_data)
log_progress("Transform process ended")

log_progress("Load process started")
load(target_file,transformed_data)
log_progress("Load process ended")
log_progress("ETL Job Ended") 
