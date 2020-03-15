import pyodbc
import pandas as pd
from sqlalchemy import create_engine
from flask import Flask, request
import urllib

########################################
# Dane Mariott   &   Shelby Fullbright #
#         MIS 5400 Summer 2019         #
# Project: Mega Millions Data Analysis #
########################################

# --------- DATA ACQUISITION ----------#

''' NOTE #1:
We intend to scrape and parse the data from the Colorado Lottery website, but we are reading the existing .xlsx
file into a pandas dataframe via python to express that we can and are aware of its existence first. This .xlsx 
file could then be written into a new file for further manipulation (which we are skipping for brevity).
'''
# dls = "https://www.coloradolottery.com/en/player-tools/winning-history/?game=megamillions&timeframe=sincestart&submit=&xlsx="
# df = pd.read_excel(dls)
# print(df)  # Comment in print to verify that the excel sheet was imported to a pandas dataframe.

''' NOTE #2:
Here we actually scrape and parse the html table data from our target site into a pandas dataframe. The following 
print statements will return the number of columns and rows, the dimensions, and data types of our dataframe respectively.
By default, strings and characters will be returned as "objects", which we could individually correct with the "astype()"
function. Because there are 41 columns in our dataset, we will not correct data types until unnecessary columns have
been deleted and/or unless we are certain it is needed. 
'''
url = 'https://www.coloradolottery.com/en/player-tools/winning-history/?game=megamillions&timeframe=sincestart&submit='
dfs = pd.read_html(url)
for df in dfs:
    print(df.shape)      # Returns 945 rows and 41 columns at time of writing.
    print(df.ndim)       # Returns 2 dimensions ( columns & rows).
    print(df.dtypes)     # Returns "objects" and "float64".

# print(df)  # Comment in print to verify that the data was imported and dataframe created.

''' Note #3:
Creation of the aforementioned dataframe produces no accessible file; however, a combination of debugging and running 
the script in console should populate the pycharm "special variables" window with a "df" element. The "df" element can
be right clicked and "view as dataframe" selected to evaluate the data from the SciView window. That said, we are also 
including the necessary code to write the dataframe to an accessible .csv file in the following. 
'''
# df.to_csv('mega_millions.csv')  # Comment in to write newly created dataframe to a .csv file.

''' NOTE #4:
Concerns about the data are minimal. We do not believe there are any null values in the columns we will need and we
will otherwise erase or populate them as necessary. This leaves only deciding on a few relevant columns for analysis 
from the 41 columns provided with use of any number of column isolating functions. 
'''

# --------- DATA PERSISTENCE --------- #

''' NOTE #1. DB explanations:
We have chosen an SQL database hosted by Azure because these appear to be both strong and common industry 
standards for data management. While we will only be working with one data table at a time, we will be using 
a traditional sql relational database table schema for its ease of access, spreadsheet structure, and because it 
already mirrors the shape of our data. 
'''
params = urllib.parse.quote_plus(r'Driver={ODBC Driver 13 for SQL Server};Server=tcp:summer2019-5400mis.database.windows.net,1433;Database=megaData;Uid=millionsDataUser@summer2019-5400mis;Pwd=$nowballsCh@nce;Encrypt=yes;TrustServerCertificate=no;Connection Timeout=30;')
conn_str = 'mssql+pyodbc:///?odbc_connect={}'.format(params)
engine_azure = create_engine(conn_str, echo=False)
print('Connection established!')

''' NOTE #2. Connection:
At this point, it is imperative to explain that default versions of "pyodbc" all had to be entirely removed and 
replaced with .whl files for our respective system requirements. Additionally, the drivers had to be individually 
examined in order to correctly state the DRIVER version in the connection string. Then Python's "venv" virtual 
environment interpreter also had to be switched for a direct path to the user's python installation. We also 
found that unicode characters in the string threw an error and had to be bypassed by "urllib.parse.quote_plus" 
and "r" in the connection string. We have also employed SQLalchemy for the connection engine rather than pyodbc 
for simplicity and structure. 
'''
url = 'https://www.coloradolottery.com/en/player-tools/winning-history/?game=megamillions&timeframe=sincestart&submit='
dfs = pd.read_html(url)
for df in dfs:
    df.to_sql('test_table', con=engine_azure, if_exists='replace')
    print(df)  # Comment in print to affirm the import of data to pandas dataframe.

''' NOTE #3. The Data:
Now we have imported the data table from Colorado Mega Millions as a pandas dataframe and immediately persisted 
it to our SQL Azure database in one fell swoop. We have currently persisted the the entirety of our data to a 
"test_table" for the purpose of this exercise and while we explore our analysis options.
'''
dfs = pd.read_sql_table("test_table", engine_azure, columns=['Draw date', 'Winning Numbers', 'Megaball', 'Jackpot Winners'])
print(dfs.head())

''' NOTE #4. Read Data from DB:
Here we print the first 5 rows of what we are tentatively considering our four most valuable columns from 
the database table. This "dfs" variable can be used to further analyze our data and write results to the original
(currently empty) table (MegaMillions) in our database. Lastly, this print statement output should serve as proof of 
data persistence to, and retrieval from, our database.  
'''

# -------- EXPORT DATA TO API -------- #

# Globals # This is system and driver specific connection string and engine as pyodbc proved repeatedly problematic. #
params = urllib.parse.quote_plus(r'Driver={ODBC Driver 13 for SQL Server};Server=tcp:summer2019-5400mis.database.windows.net,1433;Database=megaData;Uid=millionsDataUser@summer2019-5400mis;Pwd=$nowballsCh@nce;Encrypt=yes;TrustServerCertificate=no;Connection Timeout=30;')
conn_str = 'mssql+pyodbc:///?odbc_connect={}'.format(params)
engine_azure = create_engine(conn_str, echo=False)
print('Connection established!')

# Setup Flask # Instantiating, configuring, and simultaneously debugging the API.
app = Flask(__name__)
app.config.from_object(__name__)
app.config["DEBUG"] = True

# GET All DATA # Returning the entirety of our  Mega/lotto dataset from the Azure sql database to web page endpoint.
@app.route('/api/v1/test_table', methods=['GET'])
def get_all_data():
    dfs = pd.read_sql_table("test_table", engine_azure)
    return dfs.to_json(orient='records')


# GET Single DATA # Returning one row of our Mega/lotto dataset by id from the Azure sql database to web page endpoint.
@app.route('/api/v1/test_table/<int:id>', methods=['GET'])
def get_single_data(id):
    dfs = pd.read_sql('select * from "test_table" where "index" = ?', engine_azure, params=[id])
    return dfs.to_json(orient='records')


# Calling and running the API from local to endpoint.
if __name__ == '__main__':
    app.run()

# ------- SEE JUPYTER NOTEBOOK ------- #
