import db_connect

conn = db_connect.connect()
cur = conn.cursor()

cur.execute('''DROP TABLE stream''')

cur.execute('''CREATE TABLE stream
    (ID               SERIAL PRIMARY KEY      NOT NULL,
     STREAMID         BIGINT                  NOT NULL,
     CHANNELID        INT                     NOT NULL,
     URL              TEXT                    NOT NULL,
     LANGUAGE         SERIAL                  NOT NULL,
     SCHEDULED        BOOLEAN                 NOT NULL,
     FEATURED         BOOLEAN                 NOT NULL,
     MATURE           BOOLEAN                 NOT NULL,
     PARTNER          BOOLEAN                 NOT NULL,
     SPONSORED        BOOLEAN                 NOT NULL,
     GAME             TEXT                    NOT NULL,
     VIEWERS          INT                     NOT NULL,
     FOLLOWERS        INT                     NOT NULL,
     TOTALVIEWS       INT                     NOT NULL,
     VIDEOS           INT                     NOT NULL,
     TEAMS            INT                     NULL,
     STAMP           TIMESTAMP                NOT NULL
     );
     ''')


cur.execute('''SELECT * from snapshot
    ;
''')
fetch = cur.fetchall()

for row in fetch:
    print row
conn.commit()
conn.close()
