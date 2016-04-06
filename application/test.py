import db_connect

conn = db_connect.connect()
cur = conn.cursor()

cur.execute('''DROP TABLE team''')

cur.execute('''CREATE TABLE team
    (TEAMID            BIGINT               NOT NULL,
     CHANNELID         BIGINT               NOT NULL,
     TEAMNAME          TEXT                  NULL,
     TIMESTAMP         TIMESTAMP             NOT NULL
    );
''')



conn.commit()
conn.close()