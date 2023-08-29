import re


# Функция для поиска успешных операций, совершенных при просроченном или заблокированном паспорте:
def overdue_or_blocked_passports(con, date, cur):


	date_format = re.sub(r"(\d\d)(\d\d)(\d{4})", r'\3-\2-\1', date)


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
		''', [date_format])


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
		''', [date_format])


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


	date_format = re.sub(r"(\d\d)(\d\d)(\d{4})", r'\3-\2-\1', date)


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
		''', [date_format,date_format])


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


	date_format = re.sub(r"(\d\d)(\d\d)(\d{4})", r'\3-\2-\1', date)


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
		''', [date_format])


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