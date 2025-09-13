import pandas as pd
from bs4 import BeautifulSoup
import requests
import sqlite3
from datetime import datetime 

traget_file_path = './Countries_by_GDP.csv'
db_name = 'World_Economies.db '
table_name = 'Countries_by_GDP'
conn = sqlite3.connect(db_name)
log_file = './etl_project_log.txt'

def log_progess(message):
    timestamp_format = '%Y-%h-%d-%H:%M:%S'
    now = datetime.now()
    timestamp = now.strftime(timestamp_format)
    with open(log_file,"a") as f:
        f.write(f"{timestamp},{message}\n")

def extract(url):
    log_progess("Extract process stared")
    html_page = requests.get(url).text
    data= BeautifulSoup(html_page,'html.parser')
    table = data.find_all('table',{'class':'wikitable'})
    rows =table[0].find_all('tr')
    gdp_data =[]

    for row in rows[1:]:
        col = row.find_all('td')
        if len(col) >=8:
            data_dict={
                    "Country/Territory": col[0].text.strip(),
                    "UN region": col[1].text.strip(),
                    "IMF Estimate": col[2].text.strip(),
                    "IMF Year": col[3].text.strip(),
                    "World Bank Estimate": col[4].text.strip(),
                    "World Bank Year": col[5].text.strip(),
                    "United Nations Estimate": col[6].text.strip(),
                    "United Nations Year": col[7].text.strip()
            }
            gdp_data.append(data_dict)
    df = pd.DataFrame(gdp_data)
    log_progess("Extract process ended")
    return df
            
def transform(df):
    log_progess("Transform process started")
    if df.empty:
        return df
    for col in ['IMF Estimate', 'World Bank Estimate', 'United Nations Estimate']:
        df[col] = df[col].astype(str).str.replace(',','')
        df[col] = pd.to_numeric(df[col],errors='coerce')/1000
        df[col] = df[col].round(2)
        df.index.name='country_id'
    log_progess("Extract process ended")
    return df

def load_to_csv(df,traget_path):
    log_progess("Load to csv file process started")
    df.to_csv(traget_path)
    log_progess("Load to csv file process ended")

def load_to_db(df,conn,table_name):
    log_progess('load to database started')
    df.to_sql(table_name,conn,if_exists="replace", index=True)
    log_progess('load to database ended')

extracted_data = extract('https://web.archive.org/web/20230902185326/https://en.wikipedia.org/wiki/List_of_countries_by_GDP_%28nominal%29')
print(extracted_data)
transformed_data =(transform(extracted_data))   
load_to_csv(transformed_data,traget_file_path)
load_to_db(transformed_data,conn,table_name)

query = f'select * from {table_name} where country_id <=100'
print(pd.read_sql(query,conn))

