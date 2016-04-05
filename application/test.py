import db_connect

conn = db_connect.connect()
cur = conn.cursor()


cur.execute('''DROP TABLE team

''')

cur.execute('''CREATE TABLE team
    (TEAMID            BIGINT  PRIMARY KEY  NOT NULL,
     CHANNELID         BIGINT               NOT NULL,
     TEAMNAME          TEXT                  NULL
    );
''')

conn.commit()
conn.close()