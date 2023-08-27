import pandas as pd
import os

# Функция для чтения данных из файла csv или txt и загрузки в таблицу:
def CsvLoadToSql (con, filePath, tableName):
	df = pd.read_csv(filePath, sep=';')
	df.to_sql(tableName, con=con, if_exists="append", index=False)


# Функция для чтения данных из файла excel и загрузки в таблицу:
def ExcelLoadToSql (con, filePath, tableName):
	df = pd.read_excel(filePath)
	df.to_sql(tableName, con=con, if_exists='append', index=False)


# Функция для загрузки данных по транзакциям в STG таблицу, а затем в FACT таблицу. После загрузки файл перемещается в archive:
def LoadTransactions (con, date, cur):
	filePath = 'data/transactions_'+date+'.txt'
	CsvLoadToSql(con, filePath, 'STG_TRANSACTIONS')
	cur.execute('''
	INSERT INTO DWH_FACT_TRANSACTIONS (
            trans_id, trans_date, amt, card_num, oper_type, oper_result,
            terminal
        ) select
            transaction_id, transaction_date, amount, card_num, oper_type,
            oper_result, terminal from STG_TRANSACTIONS;
		''')
	con.commit()
	os.rename(filePath, os.path.join('archive', 'transactions_'+date+'.txt.backup'))


# Функция для загрузки данных по паспортам в STG таблицу, а затем в FACT таблицу. После загрузки файл перемещается в archive:
def LoadBlackPassports (con, date, cur):
	filePath = 'data/passport_blacklist_'+date+'.xlsx'
	ExcelLoadToSql(con, filePath, 'STG_PASSPORT_BLACKLIST')
	cur.execute('''
		INSERT INTO DWH_FACT_PASSPORT_BLACKLIST (
            passport_num, entry_dt
        ) select
            passport, date from STG_PASSPORT_BLACKLIST
		''')
	con.commit()
	os.rename(filePath, os.path.join('archive', 'passport_blacklist_'+date+'.xlsx.backup'))
