# import libraries

import pandas as pd
from sqlalchemy import create_engine

# load data from csv file

df_alerts = pd.read_csv('/app/data/alerts_dataset.csv')

# Conncet to SQL Database:

engine = create_engine('sqlite:////app/data/alerts.db', echo=False)

# Insert Data into SQL Databse:

df_alerts.to_sql('alerts', con=engine, if_exists='replace', index=False)

print('Data Inserted into SQL Database Successfully')

