import db_connect


def drop_tables():
    """KILL ALL THE TABLES"""
    conn = db_connect.connect()
    cur = conn.cursor()

    cur.execute("DROP TABLE snapshot")
    cur.execute("DROP TABLE game_name")
    cur.execute("DROP TABLE ratingbomb")
    cur.execute("DROP TABLE rating")
    cur.execute("DROP TABLE franchisebomb")
    cur.execute("DROP TABLE franchise")
    cur.execute("DROP TABLE publisherbomb")
    cur.execute("DROP TABLE publisher")
    cur.execute("DROP TABLE platformbomb")
    cur.execute("DROP TABLE platform")
    cur.execute("DROP TABLE genrebomb")
    cur.execute("DROP TABLE genre")
    cur.execute("DROP TABLE themebomb")
    cur.execute("DROP TABLE theme")
    cur.execute("DROP TABLE mismatch")
    cur.execute("DROP TABLE giantbomb")

    conn.commit()
    conn.close()

def make_tables():
    """MAKE ALL THE TABLES"""
    conn = db_connect.connect()
    cur = conn.cursor()

    # adding new tables
    cur.execute('''CREATE TABLE snapshot
           (ID              SERIAL   PRIMARY KEY    NOT NULL,
            NAME            TEXT                    NOT NULL,
            GIANTBOMBID     INT                     NOT NULL,
            TRIALID         SERIAL                  NOT NULL,
            RANK            INT                     NOT NULL,
            VIEWERS         INT                     NOT NULL,
            CHANNELS        INT                     NOT NULL,
            STAMP           TIMESTAMP               NOT NULL
            );''')

    cur.execute('''CREATE TABLE game_name
            (ID              SERIAL   PRIMARY KEY    NOT NULL,
             NAME            TEXT                    NOT NULL,
             GIANTBOMBID     INT                     NOT NULL,
             VIEWER_TOTAL    INT                     NOT NULL,
             CHANNEL_TOTAL   INT                     NOT NULL,
             RANK_TOTAL      INT                     NOT NULL,
             TRIALS          INT                     NOT NULL
             );''')

    cur.execute('''CREATE TABLE ratingbomb
        (RATINGBOMBID        SERIAL    PRIMARY KEY  NOT NULL,
         RATINGID            INT                    NOT NULL,
         GIANTBOMBID         INT                    NOT NULL
        );
    ''')

    cur.execute('''CREATE TABLE rating
        (RATINGID            INT       PRIMARY KEY  NOT NULL,
         RATING              TEXT                   NOT NULL
        );
    ''')

    cur.execute('''CREATE TABLE franchisebomb
        (FRANCHISEBOMBID        SERIAL    PRIMARY KEY  NOT NULL,
         FRANCHISEID            INT                    NOT NULL,
         GIANTBOMBID            INT                    NOT NULL
        );
    ''')

    cur.execute('''CREATE TABLE franchise
        (FRANCHISEID            INT       PRIMARY KEY  NOT NULL,
         FRANCHISE              TEXT                   NOT NULL
        );
    ''')

    cur.execute('''CREATE TABLE publisherbomb
        (PUBLISHERBOMBID        SERIAL    PRIMARY KEY  NOT NULL,
         PUBLISHERID            INT                    NOT NULL,
         GIANTBOMBID            INT                    NOT NULL
        );
    ''')

    cur.execute('''CREATE TABLE publisher
        (PUBLISHERID            INT       PRIMARY KEY  NOT NULL,
         PUBLISHER              TEXT                   NOT NULL
        );
    ''')

    cur.execute('''CREATE TABLE platformbomb
        (PLATFORMID          SERIAL    PRIMARY KEY  NOT NULL,
         PLATFORM            INT                    NOT NULL,
         GIANTBOMBID         INT                    NOT NULL
        );
    ''')

    cur.execute('''CREATE TABLE platform
        (PLATFORMID            INT       PRIMARY KEY  NOT NULL,
         PLATFORM              TEXT                   NOT NULL
        );
    ''')

    cur.execute('''CREATE TABLE genrebomb
        (GENREBOMBID        SERIAL    PRIMARY KEY  NOT NULL,
         GENREID            INT                    NOT NULL,
         GIANTBOMBID        INT                    NOT NULL
        );
    ''')
    cur.execute('''CREATE TABLE genre
        (GENREID            INT       PRIMARY KEY  NOT NULL,
         GENRE              TEXT                   NOT NULL
        );
    ''')

    cur.execute('''CREATE TABLE themebomb
        (THEMEBOMBID       SERIAL    PRIMARY KEY  NOT NULL,
         THEMEID           INT                    NOT NULL,
         GIANTBOMBID       INT                    NOT NULL
        );
    ''')

    cur.execute('''CREATE TABLE theme
        (THEMEID            INT       PRIMARY KEY  NOT NULL,
         THEME              TEXT                   NOT NULL
        );
    ''')

    cur.execute('''CREATE TABLE mismatch
        (MISMATCHID         SERIAL  PRIMARY KEY  NOT NULL,
         NAME               TEXT                 NOT NULL,
         GIANTBOMBID        INT                  NOT NULL
        );
    ''')

    cur.execute('''CREATE TABLE giantbomb
           (GIANTBOMBID     INT      PRIMARY KEY    NOT NULL,
            NAME            TEXT                    NOT NULL,
            ALIAS           TEXT                    NULL,
            API             TEXT                    NULL,
            RELEASE         TIMESTAMP               NULL,
            DECK            TEXT                    NULL
            );
    ''')

    cur.execute('''CREATE TABLE mismatch
        (MISMATCHID         SERIAL  PRIMARY KEY  NOT NULL,
         NAME               TEXT                 NOT NULL,
         GIANTBOMBID        INT                  NOT NULL
        );
    ''')

    cur.execute('''CREATE TABLE stream
        (DEFAULT          SERIAL PRIMARY KEY      NOT NULL,
         STREAMID         BIGINT                  NOT NULL,
         CHANNELID        INT                     NOT NULL,
         URL              TEXT                    NOT NULL,
         LANGUAGE         VARCHAR(2)              NOT NULL,
         SCHEDULED        BOOLEAN                 NOT NULL,
         FEATURED         BOOLEAN                 NOT NULL,
         MATURE           BOOLEAN                 NULL,
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

    cur.execute('''CREATE TABLE team
        (TEAMID            BIGINT               NOT NULL,
         CHANNELID         BIGINT               NOT NULL,
         TEAMNAME          TEXT                  NULL,
         STAMP             TIMESTAMP             NOT NULL
        );
    ''')

    conn.commit()
    conn.close()

if __name__ == '__main__':
    # BREAK IN CASE OF EMERGENCY # drop_tables()
    make_tables()

