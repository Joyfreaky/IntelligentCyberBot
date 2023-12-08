# Read the top 10 rwos from the sql database alerts.db saved in /app/data/ and display it


# Import libraries
import pandas as pd
from sqlalchemy import create_engine

# Read data from the sql database
engine = create_engine('sqlite:////app/data/database/passive_dns.db', echo=False)

# Read the top 10 rows from the sql database
#df_alerts = pd.read_sql_query('SELECT DISTINCT NodeName, Category, NodeType, * FROM alerts GROUP BY NodeName, Category, NodeType ORDER BY DetectTime ASC LIMIT 1000', engine)

# Define the node, category and nodetype
"""node = "cz.cesnet.hugo.haas_dionaea"
category = "Recon.Scanning"
nodetype = "Connection, Protocol, Honeypot"


query = "SELECT * FROM alerts"
conditions = []
if node is not None:
    conditions.append(f"NodeName = '{node}'")
if category is not None:
    conditions.append(f"Category = '{category}'")
if nodetype is not None:
    conditions.append(f"NodeType = '{nodetype}'")

if conditions:
    query += " WHERE " + " AND ".join(conditions) """


query = "SELECT * FROM passiveDNS_data LIMIT 1000"

# fetch the data 
df_alerts = pd.read_sql_query(query, engine)


# if the column names are repeated then keep only the first occurence of the column name and drop the rest of the columns with the same name 
df_alerts = df_alerts.loc[:,~df_alerts.columns.duplicated()]


""" df_alerts['DetectTime'] = pd.to_datetime(df_alerts['DetectTime'], format='ISO8601', utc=True)
df_alerts['EventTime'] = pd.to_datetime(df_alerts['EventTime'], format='ISO8601', utc=True)
df_alerts['WinStartTime'] = pd.to_datetime(df_alerts['WinStartTime'], format='ISO8601', utc=True)
df_alerts['WinEndTime'] = pd.to_datetime(df_alerts['WinEndTime'], format='ISO8601', utc=True) """

""" 
# Add a column anme AlertID and set it to 1 for all rows 
df_alerts['AlertID'] = 1

df_grouped = df_alerts.groupby(['DetectTime', 'Category']).agg({
        'ConnCount': 'sum',
        'FlowCount': 'sum',
        'SourceIP': pd.Series.nunique,  # Count unique source IPs
        'TargetIP': pd.Series.nunique,    # Count unique destination IPs
        'AlertID': 'sum'              # Count total number of alerts
    }).reset_index()

df_grouped = df_grouped.assign(Total_attempts=df_grouped['ConnCount'] + df_grouped['FlowCount'])
 """

# Print the length of the dataframe
print(len(df_alerts))

# Print the column names
print(df_alerts.columns)


# Display the top 10 rows
print(df_alerts.head(10))

# Print the type of each column in the dataframe
print(df_alerts.dtypes)

# print all the rows and columns from the df_alerts dataframe
#print(df_alerts.to_string())
