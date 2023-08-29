import re


# Функция для поиска успешных операций, соврешенных при просроченном или заблокированном паспорте:
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


# Функция для поиска успешных операций, соврешенных при недействующем договоре:
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


# Функция для поиска успешных операций, соверешенных в разных городах в течение одного часа:
def overdue_account(con, date, cur):
	date_format = re.sub(r"(\d\d)(\d\d)(\d{4})", r'\3-\2-\1', date)