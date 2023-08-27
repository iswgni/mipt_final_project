import sqlite3 as sql
from tabulate import tabulate
from datetime import datetime
from py_scripts import run_sql_scripts as rs
from py_scripts import load_daily_data as ldd


#Код для создания и подключения к БД:
con = sql.connect('bank_fraud.db')
cur = con.cursor()
now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
print(f'\nВремя на момент вызова: {now} \n')


# При первом запуске, после создания БД, код ниже создаст таблицы и загрузит данные,
# которые шли в файле ddl_dml.sql. (Модифицированный файл с данными для проекта называется load_initial_data.sql):
rs.RunScript(con, './sql_scripts/create_main_data_tables.sql', cur)
try:
	rs.RunScript(con, './sql_scripts/load_initial_data.sql', cur)
except:
	None


# Для удобства тестирования из консоли:
date = input('Введите дату файла: ')


# Для загрузки данных по транзакциям. Необходимо вместо date указать дату формирования файла. Например 01032021:
try:
	ldd.LoadTransactions(con, date, cur)
except FileNotFoundError:
	print('Такого csv/txt файла по транзакциям нет.')


# Для загрузки данных по паспортам. Необходимо вместо date указать дату формирования файла. Например 01032021:
try:
	ldd.LoadBlackPassports(con, date, cur)
except FileNotFoundError:
	print('Такого excel файла по заблокированным паспортам нет.')



# Для отладки:
def showTable(tableName):
	cur.execute(f'SELECT * From {tableName}') # Через вопрос не прокатит 
	print('_-'*10)
	print(tableName)
	print('_-'*10)
	for row in cur.fetchall():
		print(', '.join([str(item) for item in row]))
	print('_-'*10 + '\n')	


# showTable('DWH_FACT_TRANSACTIONS')