# Запуск скриптов. Указать коннект, путь до скрипта, sql курсор:

def RunScript (con, path, cur):
	with open (path, 'r', encoding='utf-8') as sql_file:
		sql_script = sql_file.read()
	cur.executescript(sql_script)
	con.commit()

