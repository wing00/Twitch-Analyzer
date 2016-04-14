import db_connect
from multiprocessing import Pool
import requests
from functools import partial

import plotly

plotly.tools.set_credentials_file(username='styoung', api_key='vaa6619ibf')
conn = db_connect.connect()
cur = conn.cursor()

cur.execute('''SELECT * FROM publisher WHERE publisherid = 2189
''')
fetch = cur.fetchall()
print fetch

# conn.commit()
conn.close()

print fetch[0]