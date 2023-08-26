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