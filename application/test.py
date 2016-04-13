import db_connect
from multiprocessing import Pool
import requests
from functools import partial


conn = db_connect.connect()
cur = conn.cursor()

cur.execute('''SELECT * FROM stream''')
fetch = cur.fetchall()
print fetch

# conn.commit()
conn.close()


print (u'test' + u'\xf6')

