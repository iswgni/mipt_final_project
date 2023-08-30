import sqlite3 as sql
from tabulate import tabulate
from datetime import datetime
from py_scripts import run_sql_scripts as rs
from py_scripts import load_daily_data as ldd
from py_scripts import fraud_detection as fd
from numpy import array 
import re




#Код для создания и подключения к БД:
con = sql.connect('bank_fraud.db')
cur = con.cursor()
now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
print(f'\nВремя на момент вызова: {now} \n')


# Для просмотра таблиц:
def show_table(table_name):
	cur.execute(f'SELECT * FROM {table_name}')
	columns = list(map(lambda x: x[0], cur.description))
	rows = array(cur.fetchall())
	print('\n'+table_name+'\n')
	print(tabulate(rows, headers = columns, tablefmt ='fancy_grid'))


# Для удобства проверки работы программы из консоли. Например 01032021:
date = input('Введите дату файла: ')


# Форматирование даты для передачи в функции:
date_format = re.sub(r"(\d\d)(\d\d)(\d{4})", r'\3-\2-\1', date)


# При первом запуске, после создания БД, код ниже создаст таблицы и загрузит данные,
# которые шли в файле ddl_dml.sql. (Модифицированный файл с данными для проекта называется load_initial_data.sql):
rs.RunScript(con, './sql_scripts/create_main_data_tables.sql', cur)
try:
	rs.RunScript(con, './sql_scripts/load_initial_data.sql', cur)
except:
	None


# Для загрузки данных по транзакциям:
try:
	ldd.load_transactions(con, date, cur)
except FileNotFoundError:
	print('\nСsv/txt файла на дату '+date+' по транзакциям нет.\n')


# Для загрузки данных по паспортам:
try:
	ldd.load_black_passports(con, date, cur)
except FileNotFoundError:
	print('\nExcel файла на дату '+date+' по заблокированным паспортам нет.\n')


# Для загрузки данных по терминалам:
try:
	ldd.load_terminals(con, date, cur)
except FileNotFoundError:
	print('\nExcel файла на дату '+date+' по терминалам нет.\n')


# Вызовы функций для заполнения таблицы REP_FRAUD на выбранную дату:
fd.overdue_or_blocked_passports(con, date_format, cur)
fd.overdue_account(con, date_format, cur)
fd.different_city_in_hour(con, date_format, cur)
fd.sum_guessing(con, date_format, cur)

show_table('REP_FRAUD')



