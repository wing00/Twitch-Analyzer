import db_connect

conn = db_connect.connect()
cur = conn.cursor()



query = '''SELECT * FROM stream WHERE url = 'https://www.twitch.tv/clintstevens'
'''

cur.execute(query)
fetch = cur.fetchall()

for item in fetch:
    print item

conn.commit()
conn.close()
