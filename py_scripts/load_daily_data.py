import pandas as pd
import os




# Функция для чтения данных из файла csv или txt и загрузки в таблицу:
def csv_load_to_sql (con, filePath, tableName):
	df = pd.read_csv(filePath, sep=';')
	df.to_sql(tableName, con=con, if_exists="append", index=False)


# Функция для чтения данных из файла excel и загрузки в таблицу:
def excel_load_to_sql (con, filePath, tableName):

	df = pd.read_excel(filePath)

	df.to_sql(tableName, con=con, if_exists='append', index=False)


# Функция для загрузки данных по транзакциям в STG таблицу, а затем в FACT таблицу. После загрузки файл перемещается в archive:
def load_transactions (con, date, cur):

	filePath = 'data/transactions_'+date+'.txt'

	cur.execute('''
		CREATE TABLE if not exists STG_TRANSACTIONS (
			transaction_id varchar(32),
			transaction_date date,
			amount decimal(10,2),
			card_num varchar(32),
			oper_type varchar(32),
			oper_result varchar(32),
			terminal varchar(32)
			);
		''')

	csv_load_to_sql(con, filePath, 'STG_TRANSACTIONS')

	cur.execute('''
	INSERT INTO DWH_FACT_TRANSACTIONS (
            trans_id, trans_date, amt, card_num, oper_type, oper_result,
            terminal
        ) select
            transaction_id, transaction_date, amount, card_num, oper_type,
            oper_result, terminal from STG_TRANSACTIONS;
		''')

	cur.execute('DROP TABLE if exists STG_TRANSACTIONS')

	con.commit()

	os.rename(filePath, os.path.join('archive', 'transactions_'+date+'.txt.backup'))


# Функция для загрузки данных по паспортам в STG таблицу, а затем в FACT таблицу. После загрузки файл перемещается в archive:
def load_black_passports (con, date, cur):

	filePath = 'data/passport_blacklist_'+date+'.xlsx'

	cur.execute('''
		CREATE TABLE if not exists STG_PASSPORT_BLACKLIST (
			passport varchar(32),
			date date
			);
		''')

	excel_load_to_sql(con, filePath, 'STG_PASSPORT_BLACKLIST')

	cur.execute('''
		INSERT INTO DWH_FACT_PASSPORT_BLACKLIST (
            passport_num, entry_dt
        ) select
            passport, 
            date 
            from STG_PASSPORT_BLACKLIST
            where passport not in (select passport_num from DWH_FACT_PASSPORT_BLACKLIST) -- Сделано, чтобы избежать дублей паспортов.
		''')

	cur.execute('DROP TABLE if exists STG_PASSPORT_BLACKLIST')

	con.commit()

	os.rename(filePath, os.path.join('archive', 'passport_blacklist_'+date+'.xlsx.backup'))


# Функция для загрузки данных по терминалам в STG таблицы, выделения инкрементов, а затем загрузки в HIST таблицу. После загрузки файл перемещается в archive:
def load_terminals (con, date, cur):

	filePath = 'data/terminals_'+date+'.xlsx'

	cur.execute('''
		CREATE TABLE if not exists STG_TERMINALS (
			terminal_id varchar(32),
			terminal_type varchar(10),
			terminal_city varchar(64),
			terminal_address varchar(128)
			);
		''')

	cur.execute('''
		CREATE VIEW if not exists STG_TERMINALS_ACTUAL_VIEW as
		    select
		        id, terminal_id, terminal_type, terminal_city, terminal_address
		    from DWH_DIM_TERMINALS_HIST
		    where current_timestamp between effective_from and effective_to
		    and deleted_flg = 0;
		''')

	excel_load_to_sql(con, filePath, 'STG_TERMINALS') 

	# Для создания таблицы, в которой будут новые записи:
	cur.execute('''
		CREATE TABLE if not exists STG_TERMINALS_NEW as 
			SELECT 
				*
			from STG_TERMINALS
			where terminal_id not in (select terminal_id from STG_TERMINALS_ACTUAL_VIEW)
		''')

	# Для создания таблицы, в которой будут удаленные записи:
	cur.execute('''
		CREATE TABLE if not exists STG_TERMINALS_DELETED as 
			SELECT 
				*
			from STG_TERMINALS_ACTUAL_VIEW
			where terminal_id not in (select terminal_id from STG_TERMINALS)
		''')

	# Для создания таблицы, в которой будут измененные записи:
	cur.execute('''
		CREATE TABLE if not exists STG_TERMINALS_CHANGED as 
			SELECT 
				t1.*
			from STG_TERMINALS t1
			inner join STG_TERMINALS_ACTUAL_VIEW t2
			on t1.terminal_id = t2.terminal_id
			where 
				t1.terminal_type <> t2.terminal_type
				or t1.terminal_city <> t2.terminal_city
				or t1.terminal_address <> t2.terminal_address
		''')

	# Добавление новых записей:
	cur.execute('''
		INSERT INTO DWH_DIM_TERMINALS_HIST (
			terminal_id,
			terminal_type,
			terminal_city,
			terminal_address
			) select
			terminal_id,
			terminal_type,
			terminal_city,
			terminal_address
			from STG_TERMINALS_NEW
		''')

	# Добавление удаленных записей:
	cur.execute('''
		UPDATE DWH_DIM_TERMINALS_HIST
			set effective_to = datetime('now', '-1 second')
			where effective_to = '2999-12-31 23:59:59' and terminal_id in (select terminal_id from STG_TERMINALS_DELETED)
	''')

	cur.execute('''
		INSERT INTO DWH_DIM_TERMINALS_HIST (
			terminal_id,
			terminal_type,
			terminal_city,
			terminal_address,
			deleted_flg
			) select
			terminal_id,
			terminal_type,
			terminal_city,
			terminal_address,
			1
			from STG_TERMINALS_DELETED
		''')

	# Добавление измененных записей:
	cur.execute('''
		UPDATE DWH_DIM_TERMINALS_HIST
			set effective_to = datetime('now', '-1 second')
			where effective_to = '2999-12-31 23:59:59' and terminal_id in (select terminal_id from STG_TERMINALS_CHANGED)
	''')

	cur.execute('''
		INSERT INTO DWH_DIM_TERMINALS_HIST (
			terminal_id,
			terminal_type,
			terminal_city,
			terminal_address
			) select
			terminal_id,
			terminal_type,
			terminal_city,
			terminal_address
			from STG_TERMINALS_CHANGED
		''')

	cur.execute('DROP TABLE if exists STG_TERMINALS')

	cur.execute('DROP TABLE if exists STG_TERMINALS_NEW')

	cur.execute('DROP TABLE if exists STG_TERMINALS_DELETED')

	cur.execute('DROP TABLE if exists STG_TERMINALS_CHANGED')

	con.commit()

	os.rename(filePath, os.path.join('archive', 'terminals_'+date+'.xlsx.backup'))
	
	

