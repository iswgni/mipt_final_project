

# Функция для поиска успешных операций, совершенных при просроченном или заблокированном паспорте:
def overdue_or_blocked_passports(con, date, cur):

	# Выборка всех просроченных заблокированных паспортов:
	cur.execute('''
		CREATE TABLE if not exists STG_OVERDUE_OR_BLOCKED_PASSPORTS as 
			SELECT
				client_id,
				passport_num,
				last_name || ' ' || first_name || ' ' || patronymic as fio,
				phone,
				'overdue_passport' as event_type
			from DWH_DIM_CLIENTS
			where passport_valid_to is not null 
			and ? > passport_valid_to
			UNION
			SELECT
				client_id,
				passport_num,
				last_name || ' ' || first_name || ' ' || patronymic as fio,
				phone,
				'blocked_passport' as event_type
			from DWH_DIM_CLIENTS
			where passport_num in (select passport_num from DWH_FACT_PASSPORT_BLACKLIST)
		''', [date])

	# Выборка совершенных транзакций при заблокированном/просроченном паспорте:
	cur.execute('''
		CREATE TABLE if not exists STG_OVERDUE_OR_BLOCKED_PASSPORTS_FINAL as 
			SELECT
				t4.trans_date,
				t1.*
			from STG_OVERDUE_OR_BLOCKED_PASSPORTS t1
			join DWH_DIM_ACCOUNTS t2 on t1.client_id = t2.client
			join DWH_DIM_CARDS t3 on t2.account_num = t3.account_num
			join DWH_FACT_TRANSACTIONS t4 on t3.card_num = t4.card_num
			where ? = date(t4.trans_date)
			and t4.oper_result = 'SUCCESS'
		''', [date])

	# Загрузка из стеджинга в таблицу отчетов:
	cur.execute('''
		INSERT INTO REP_FRAUD (
			event_dt,
			passport,
			fio,
			phone,
			event_type
			) select 
				trans_date,
				passport_num,
				fio,
				phone,
				event_type
			from STG_OVERDUE_OR_BLOCKED_PASSPORTS_FINAL
		''')

	cur.execute('DROP TABLE if exists STG_OVERDUE_OR_BLOCKED_PASSPORTS')

	cur.execute('DROP TABLE if exists STG_OVERDUE_OR_BLOCKED_PASSPORTS_FINAL')

	con.commit()


# Функция для поиска успешных операций, совершенных при недействующем договоре:
def overdue_account(con, date, cur):

	# Выборка успешных операций при недействующем договоре:
	cur.execute('''
		CREATE TABLE if not exists SGT_OVERDUE_ACCOUNT_FINAL as 
			SELECT
				t4.trans_date,
				t1.passport_num,
				t1.last_name || ' ' || t1.first_name || ' ' || t1.patronymic as fio,
				t1.phone,
				'overdue_account' as event_type
			from DWH_DIM_CLIENTS t1
			join DWH_DIM_ACCOUNTS t2 on t1.client_id = t2.client
			join DWH_DIM_CARDS t3 on t2.account_num = t3.account_num
			join DWH_FACT_TRANSACTIONS t4 on t3.card_num = t4.card_num
			where ? > t2.valid_to 
			and t4.oper_result = 'SUCCESS'
			and ? = date(t4.trans_date)
		''', [date,date])

	# Загрузка из стейджинга в таблицу отчетов:
	cur.execute('''
		INSERT INTO REP_FRAUD (
			event_dt,
			passport,
			fio,
			phone,
			event_type
			) select 
				trans_date,
				passport_num,
				fio,
				phone,
				event_type
			from SGT_OVERDUE_ACCOUNT_FINAL
		''')

	cur.execute('DROP TABLE if exists SGT_OVERDUE_ACCOUNT_FINAL')

	con.commit()


# Функция для поиска успешных операций, совершенных в разных городах в течение одного часа:
def different_city_in_hour(con, date, cur):

	# Выборка успешных операций, совершенных в разных городах в течение часа:
	cur.execute('''
		CREATE TABLE if not exists STG_DIF_CITY as
			SELECT
				t3.card_num,
				t3.terminal_city,
				t3.oper_result,
				t3.trans_date,
				t3.prev_trans_date,
				t3.prev_city,
				t6.passport_num,
				t6.last_name || ' ' || t6.first_name || ' ' || t6.patronymic as fio,
				t6.phone,
				'diff_city' as event_type,
				julianday(t3.trans_date)*24*60 - julianday(t3.prev_trans_date)*24*60 as dif
			from(
				SELECT
					t1.card_num,
					t2.terminal_city,
					t1.oper_result,
					t1.trans_date,
					lag(t1.trans_date) over(partition by t1.card_num order by t1.trans_date) as prev_trans_date,
					lag(t2.terminal_city) over(partition by t1.card_num order by t1.trans_date) as prev_city
				from DWH_FACT_TRANSACTIONS t1
				join DWH_DIM_TERMINALS_HIST t2 on t1.terminal = t2.terminal_id
				where ? = date(t1.trans_date)
				and t1.oper_result = 'SUCCESS') t3
			join DWH_DIM_CARDS t4 on t3.card_num = t4.card_num
			join DWH_DIM_ACCOUNTS t5 on t4.account_num = t5.account_num
			join DWH_DIM_CLIENTS t6 on t5.client = t6.client_id
			where terminal_city <> prev_city
			and julianday(trans_date)*24*60 - julianday(prev_trans_date)*24*60 < 60
		''', [date])

	# Загрузка из стейджинга в таблицу отчетов:
	cur.execute('''
		INSERT INTO REP_FRAUD (
			event_dt,
			passport,
			fio,
			phone,
			event_type
			) select 
				trans_date,
				passport_num,
				fio,
				phone,
				event_type
			from STG_DIF_CITY
		''')

	cur.execute('DROP TABLE if exists STG_DIF_CITY')

	con.commit()


# Функция для поиска успешных операций при подборе суммы:
def sum_guessing(con, date, cur):

	# Выборка успешных операций, совершенных с третьей попытки подбором суммы:
	cur.execute('''
		CREATE TABLE if not exists STG_SUM_GUESSING as
			SELECT 
				card_num,
				amt,
				oper_result,
				trans_date,
				card_num,
				prev_oper_result_1,
				prev_amt_1,
				prev_oper_result_2,
				prev_amt_2,
				prev_oper_result_3,
				prev_amt_3,
				prev_trans_date_3,
				julianday(trans_date)*24*60 - julianday(prev_trans_date_3)*24*60 as t,
				'sum_guessing' as event_type
			from(SELECT 
					card_num,
					amt,
					oper_result,
					trans_date,
					card_num,
					lag(oper_result) over(partition by card_num order by trans_date) as prev_oper_result_1,
					lag(amt) over(partition by card_num order by trans_date) as prev_amt_1,
					lag(oper_result,2) over(partition by card_num order by trans_date) as prev_oper_result_2,
					lag(amt,2) over(partition by card_num order by trans_date) as prev_amt_2,
					lag(oper_result,3) over(partition by card_num order by trans_date) as prev_oper_result_3,
					lag(amt,3) over(partition by card_num order by trans_date) as prev_amt_3,
					lag(trans_date,3) over(partition by card_num order by trans_date) as prev_trans_date_3
				from DWH_FACT_TRANSACTIONS
				where ? = date(trans_date)) 
			where oper_result = 'SUCCESS'
			and prev_oper_result_1 = 'REJECT'
			and prev_oper_result_2 = 'REJECT'
			and prev_oper_result_3 = 'REJECT'
			and cast(amt as int) < cast(prev_amt_1 as int)
			and cast(prev_amt_1 as int) < cast(prev_amt_2 as int)
			and cast(prev_amt_2 as int)  < cast(prev_amt_3 as int)
			and julianday(trans_date)*24*60 - julianday(prev_trans_date_3)*24*60 < 20
		''', [date])

	# Сбор оставшихся данных для отчетной таблицы:
	cur.execute('''
		CREATE TABLE if not exists STG_SUM_GUESSING_FINAL as
			SELECT 
				t1.trans_date,
				t1.event_type,
				t4.last_name || ' ' || t4.first_name || ' ' || t4.patronymic as fio,
				t4.passport_num,
				t4.phone
			from STG_SUM_GUESSING t1
			join DWH_DIM_CARDS t2 on t1.card_num = t2.card_num
			join DWH_DIM_ACCOUNTS t3 on t2.account_num = t3.account_num
			join DWH_DIM_CLIENTS t4 on t3.client = t4.client_id
		''')

	# Загрузка из стейджинга в таблицу отчетов:
	cur.execute('''
		INSERT INTO REP_FRAUD (
			event_dt,
			passport,
			fio,
			phone,
			event_type
			) select 
				trans_date,
				passport_num,
				fio,
				phone,
				event_type
			from STG_SUM_GUESSING_FINAL
		''')

	cur.execute('DROP TABLE if exists STG_SUM_GUESSING')

	cur.execute('DROP TABLE if exists STG_SUM_GUESSING_FINAL')

	con.commit()