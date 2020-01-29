import pyodbc


SERVER = 'HOSTNAME'
DATABASE = 'DB'
UNAME = 'USR'
PWD ='PWD'

SQL_CONN_STR = 'Driver={ODBC Driver 17 for SQL Server};Server='+SERVER+';Database='+DATABASE+';Trusted_Connection=no;UID='+UNAME+';PWD='+PWD+';'


#try:
conn = pyodbc.connect(SQL_CONN_STR)
cursor = conn.cursor()
#except:
#    print("Error connecting to DB")

#try:
cursor.execute("SELECT @@version;") 
row = cursor.fetchone() 
while row: 
    print(row[0])
    row = cursor.fetchone()
#except:
#    print("Error executing query")
cursor.close()
conn.close()