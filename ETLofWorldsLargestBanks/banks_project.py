import requests
import pandas as pd
from bs4 import BeautifulSoup
from datetime import datetime
import sqlite3
log_file = './code_log.txt'
csv_file = './Largest_banks_data.csv'
def log_progress(message):
    timestamp_format = '%Y-%h-%d-%H:%M:%S'
    now = datetime.now()
    timestamp = now.strftime(timestamp_format)
    with open(log_file,"a") as f:
        f.write(f'${timestamp},{message} \n')

def extract(url):
    log_progress("extract progress started")
    try:
        html_page = requests.get(url).text
        data = BeautifulSoup(html_page,'html.parser')
        table = data.find_all('table')
        tbody = table[0].find_all('tbody')
        rows = tbody[0].find_all('tr')
        world_bank_data = []
        for row in rows:
            col = row.find_all('td')
            if len(col) >=3:
                data_dict = {
                    "rank": col[0].text.strip(),
                    "name": col[1].text.strip(),
                    "MC_USD_Billion":col[2].text.strip()
                }
                world_bank_data.append(data_dict)

        df = pd.DataFrame(world_bank_data)
        log_progress('Extract progress Ended')
        return df              
    except requests.exceptions.RequestException as e:
        log_progress(f'Extract progrees error: {e}')
        return pd.DataFrame()  

def transform(df,csv_file):
    log_progress('Transform progress started')
    exchangeData = pd.read_csv(csv_file)
    
    df['MC_USD_Billion'] = df['MC_USD_Billion'].str.replace(',','')
    df['MC_USD_Billion'] = pd.to_numeric(df['MC_USD_Billion'])

    df['MC_GBP_Billion'] = (df['MC_USD_Billion'] *
                             exchangeData.loc[exchangeData['Currency'] == 'GBP', 'Rate'].iloc[0]).round(2)
    df['MC_EUR_Billion'] = (df['MC_USD_Billion'] * 
                            exchangeData.loc[exchangeData['Currency'] == 'EUR', 'Rate'].iloc[0]).round(2)
    df['MC_INR_Billion'] = (df['MC_USD_Billion'] * 
                            exchangeData.loc[exchangeData['Currency'] == 'INR', 'Rate'].iloc[0]).round(2)
    log_progress('Transform progress ended')
    print(f"market capitalization of the 5th largest bank in billion EUR is  {df['MC_EUR_Billion'][4]}")
    return df
    
def load_to_csv(df,target_file):
    log_progress('Load to csv file progress started')
    df.index.name = "id"
    df.to_csv(target_file)
    log_progress('Load to csv file progress ended')

def load_to_db(df,conn,table_name):
    log_progress("load to database started")
    db =  df.to_sql(table_name,conn,if_exists="replace", index=True)
    log_progress("load to database ended")

def run_query(query_statment,conn):
     print(pd.read_sql(query_statment,conn))


extracted_data = extract('https://web.archive.org/web/20230908091635%20/https://en.wikipedia.org/wiki/List_of_largest_banks')
print(extracted_data)
transformed_data = transform(extracted_data,'exchange_rate.csv')
table_name = "Largest_banks"
db_name = "Banks.db"
conn = sqlite3.connect(db_name)
load_to_csv(transformed_data,csv_file)
load_to_db(transformed_data,conn,table_name)
run_query(f"select * from {table_name}",conn)
run_query(f"select AVG(MC_GBP_Billion) from {table_name}",conn)
run_query(f"select NAME from {table_name} LIMIT 5;",conn)

