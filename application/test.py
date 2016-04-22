import db_connect

conn = db_connect.connect()
cur = conn.cursor()

test = {'PC':1,
'Mobile':2,
'Microsoft':3,
'Sony': 4,
'Nintendo':5,
'Sega':6,
'Other': 7}


query = '''UPDATE platformgroup SET groupid = 7 WHERE platformgroup = 'Other'
'''

cur.execute(query)

conn.commit()
conn.close()
