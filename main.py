import sqlite3 as sql
from tabulate import tabulate
from datetime import datetime
from py_scripts import create_tables as ct

connection = sql.connect('bank_fraud.db')
cursor = connection.cursor()
now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
print(f'\nВремя на момент вызова: {now} \n')

ct.CreateTables(connection, './sql_scripts/create_main_data_tables.sql', cursor)
## ct.LoadData(connection, './sql_scripts/load_initial_data.sql', cursor)

def CreateTables (connection, path, cursor):
	with open (path, 'r', encoding='utf-8') as sql_file:
		sql_script = sql_file.read()
	cursor.executescript(sql_script)
	connection.commit()

def LoadData (connection, path, cursor):
	with open (path, 'r', encoding='utf-8') as sql_file:
		sql_script = sql_file.read()
	cursor.executescript(sql_script)
	connection.commit()

	
def showTable(tableName):
	cursor.execute(f'SELECT * From {tableName}') # Через вопрос не прокатит 
	print('_-'*10)
	print(tableName)
	print('_-'*10)
	for row in cursor.fetchall():
		print(', '.join([str(item) for item in row]))
	print('_-'*10 + '\n')	

showTable('DWH_DIM_CARDS')
showTable('DWH_DIM_ACCOUNTS')
showTable('DWH_DIM_CLIENTS')