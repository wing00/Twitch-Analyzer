import db_connect
import pandas

conn = db_connect.connect()
cur = conn.cursor()

query = '''SELECT * FROM snapshot
           ORDER BY STAMP DESC
'''

cur.execute(query)
fetch = cur.fetchall()
test = pandas.DataFrame(fetch)


print test
conn.close()

