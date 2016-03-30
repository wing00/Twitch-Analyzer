import psycopg2
from psycopg2.extensions import AsIs
import re
import requests
from application import app


def connect():
    """  db connection for psql

    :return: (object) psycopg2 connect
    """

    conn = psycopg2.connect(
        database=app.config['DB_NAME'],
        user=app.config['DB_USER'],
        password=app.config['DB_PASS'],
        host=app.config['DB_HOST'],
        port=app.config['DB_PORT']
    )
    return conn


def get_trialid(cur):
    """ fetches highest trial id number

    :return: (int) trial id
    """

    cur.execute('''SELECT trialid FROM snapshot ORDER BY trialid DESC LIMIT 1''')
    trialid = cur.fetchone()
    if trialid:
        trialid = trialid[0]
    else:
        trialid = 1

    return trialid


def update_table(row, trialid, giantbomb, cur):
    """ checking and updating db with game info from giantbomb

    :param row: (dict) dict of twitch json object
    :param trialid: (int) trial id number
    :param giantbomb: (object) giantbomb class object
    :return: None
    :rtype: object
    """

    giantbomb.check_db(row['name'], row['giantbombid'])
    row['trialid'] = trialid + 1

    # adding values into snapshot table
    query = '''
              INSERT INTO snapshot VALUES (
                  DEFAULT,
                  %(name)s,
                  %(giantbombid)s,
                  %(trialid)s,
                  %(rank)s,
                  %(viewers)s,
                  %(channels)s,
                  current_timestamp
                )
            '''
    cur.execute(query, row)

    # updating game_name table: if no entry found add entry
    query = '''
                DO
                $do$
                BEGIN
                    IF EXISTS(SELECT * FROM game_name WHERE name = %(name)s) THEN
                        UPDATE game_name
                            SET viewer_total = viewer_total + %(viewers)s,
                                channel_total = channel_total + %(channels)s,
                                rank_total = rank_total + %(rank)s,
                                trials = trials + 1
                            WHERE name = %(name)s;
                    ELSE
                        INSERT INTO game_name VALUES (
                            DEFAULT,
                            %(name)s,
                            %(giantbombid)s,
                            %(viewers)s,
                            %(channels)s,
                            %(rank)s,
                            1
                            );
                    END IF;
                END
                $do$
            '''
    cur.execute(query, row)


class Twitch:
    """twitch API"""

    def __init__(self):
        self.token = app.config['TWITCH_API']
        self.fields = self.set_fields
        self.streams = self.set_streams

    @property
    def set_fields(self):
        """ api call to twitch to get data

        :rtype: object
        :return: (list) list of dicts of json
        """

        headers = {'client-id': self.token}

        data = requests.get('https://api.twitch.tv/kraken/games/top', headers=headers).json()
        total = data['_total']
        offset = 0
        fields = []

        while offset < total:
            data = requests.get('https://api.twitch.tv/kraken/games/top',
                                params=dict(limit=100, offset=offset),
                                headers=headers).json()
            total = data['_total']

            for index, field in enumerate(data['top']):
                row = dict(
                    name=field['game']['name'],
                    giantbombid=field['game']['giantbomb_id'],
                    viewers=field['viewers'],
                    channels=field['channels'],
                    rank=index + 1
                )
                fields.append(row)
            offset += 100

        return fields

    @property
    def set_streams(self):

        headers = {'client-id': self.token}
        data = requests.get('https://api.twitch.tv/kraken/streams/featured', params=dict(limit=1), headers=headers).json()

        fields = []

        for index, field in enumerate(data['featured']):
            stream = field['stream']
            channel = stream['channel']

            row = dict(sponsored=field['sponsored'],
                       scheduled=field['scheduled'],

                       game=stream['game'],
                       viewers=stream['viewers'],
                       stream_id=stream['_id'],

                       mature=channel['mature'],
                       language=channel['broadcaster_language'],
                       channel_id=channel['_id'],
                       partner=channel['partner'],
                       url=channel['url'],
                       total_views=channel['views'],
                       followers=channel['followers'],
                       )

            fields.append(row)

        return fields


class Giantbomb:
    """giantbomb API"""

    def __init__(self):
        self.token = app.config['GIANTBOMB_API']  # api token
        self.header = {'user-agent': app.config['GIANTBOMB_NAME']}
        self.tablenames = [('original_game_rating', 'rating'),
                           ('platforms', 'platform'),
                           ('franchises', 'franchise'),
                           ('publishers', 'publisher'),
                           ('genres', 'genre'),
                           ('themes', 'theme')]

        # populating db_ids and mismatch_ids from db
        self.db_ids = []
        self.mismatch_ids = []
        self.set_ids()

    def search_web(self, name, giantbombid):
        """ accesses giantbomb api for search query and returns api if giantbombid matches

        :param name: (str) name of game
        :param giantbombid: (int) id number of game
        :return: (str): url for api link
        """

        if giantbombid == 0:
            print('no giantbombid')
            return
        else:
            url = 'http://giantbomb.com/api/search/'

            headers = self.header
            param = dict(
                format='json',
                resources='game',
                api_key=self.token,
                query=name
            )

            data = requests.get(url, params=param, headers=headers).json()

            for row in data['results']:
                if giantbombid == row['id']:
                    return row['api_detail_url']
            print('no match found')
            return

    def search_mismatch(self, name):
        """ checks mismatch table for name and returns giantbombid for that name

        :param name: (str) name of game
        :return: (int) giantbombid
        """

        data = [item[0] for item in self.mismatch_ids if item[1] == name]
        if data:
            return data[0]
        else:
            print('no match found, adding mismatch')
            fetch = self.add_mismatch(name)
            return fetch

    def search_name(self, name):
        """ accesses api and returns first match or returns None if none found

        :param name: (str) name of game
        :return: (str) api url
        """

        url = 'http://giantbomb.com/api/search/'
        headers = self.header

        param = dict(
            format='json',
            resources='game',
            api_key=self.token,
            query=name
        )
        fields = requests.get(url, params=param, headers=headers).json()

        if fields['results']:
            for row in fields['results']:
                if re.search(name, row['name']):
                    return row['id']
        return None

    def add_mismatch(self, name):
        """adding mismatch to table

        :param name: (int) name of game
        :return: giantbombid (int)
        """

        giantbombid = self.search_name(name)

        conn = connect()
        cur = conn.cursor()

        if not giantbombid:
            print('no match found')

            query = '''SELECT giantbombid FROM giantbomb ORDER BY giantbombid DESC'''
            cur.execute(query)
            fetch = cur.fetchone()

            if fetch[0] < 1000000:
                giantbombid = 1000000  # setting id to high value to avoid mismatch
            else:
                giantbombid = fetch[0] + 1  # setting from highest value

        query = '''INSERT INTO mismatch VALUES (
            DEFAULT,
            %(name)s,
            %(giantbombid)s
            )
        '''
        cur.execute(query, dict(name=name, giantbombid=giantbombid))
        conn.commit()
        conn.close()

        return giantbombid

    def set_ids(self):
        """ populates db_ids with giantbombids for searching

        :return:
        """

        conn = connect()
        cur = conn.cursor()
        query = '''SELECT giantbombid FROM giantbomb'''
        cur.execute(query)
        fetch = cur.fetchall()
        fetch = [item[0] for item in fetch]
        self.db_ids = fetch


        query = '''SELECT giantbombid, name FROM mismatch'''
        cur.execute(query)
        fetch = cur.fetchall()
        self.mismatch_ids = fetch

        conn.close()

    def check_db(self, name, giantbombid):
        """ checks db_ids for giantbombid
        if giantbombid doesn't exist search mismatch table for giantbombid

        :param name: (str) name of game
        :param giantbombid: (int) id of game
        :return: None
        """

        if giantbombid == 0:
            giantbombid = self.search_mismatch(name)

        if giantbombid not in self.db_ids:
            api = self.search_web(name, giantbombid)
            print(giantbombid, name)

            if api:
                self.add_db(api, name)
                self.db_ids.append(giantbombid)
            else:
                self.add_db_no_api(name, giantbombid)
            print('added ' + name + ' to giantbomb table')

    @staticmethod
    def add_db_no_api(name, giantbombid):
        """db call for no api

        :param name: (str) name of game
        :param giantbombid: (int) id of game
        :return: None
        """
        conn = connect()
        cur = conn.cursor()

        query = '''INSERT INTO giantbomb VALUES (%(giantbombid)s, %(name)s) '''
        cur.execute(query, dict(giantbombid=giantbombid, name=name))

        conn.commit()
        conn.close()

    def add_db(self, api, name):
        """ db add with api call

        :param api: (str) api url
        :param name: (str) name of game
        :return: None
        """

        headers = self.header
        fields = requests.get(api, params=dict(format='json', api_key=self.token), headers=headers).json()
        row = fields['results']

        data = dict(
            name=name,
            giantbombid=row['id'],
            alias=row['aliases'],
            api=row['api_detail_url'],
            release=row['original_release_date'],
            deck=row['deck']
        )

        conn = connect()
        cur = conn.cursor()

        # looping through tablenames to populate tables
        for tablename in self.tablenames:
            self.add_resource(data['giantbombid'], tablename, row, cur)

        # final query to giantbomb table
        query = '''
              INSERT INTO giantbomb VALUES (
                  %(giantbombid)s,
                  %(name)s,
                  %(alias)s,
                  %(api)s,
                  %(release)s,
                  %(deck)s
                  )
            '''

        cur.execute(query, data)
        conn.commit()
        conn.close()

    @staticmethod
    def add_resource(giantbombid, table_name, row, cur):
        """adds info to each table

        :param giantbombid: (int) id of game
        :param table_name: (str) name of table
        :param row: (json) json of api call
        :param cur: (object) cursor for sql connection
        :return: None
        :rtype: object
        """

        # check if resource exists
        if row.get(table_name[0]) is None:
            return

        elif row[table_name[0]] is None:
            return

        else:
            data = dict(giantbombid=AsIs(giantbombid),
                        tablename=AsIs(table_name[1]),
                        tablebombname=AsIs(table_name[1] + 'bomb')
                        )

            for item in row[table_name[0]]:
                data['name'] = item['name']
                data['id'] = item['id']

                # adding to resourcebomb table
                query = '''
                  INSERT INTO %(tablebombname)s VALUES(DEFAULT, %(id)s, %(giantbombid)s)
                '''
                cur.execute(query, data)

                # adding to resource table, checking for uniqueness
                query = '''
                  DO
                  $do$
                  BEGIN
                      IF NOT EXISTS(SELECT * FROM %(tablename)s WHERE %(tablename)s = %(name)s) THEN
                         INSERT INTO %(tablename)s VALUES(%(id)s, %(name)s);
                      END IF;
                  END
                  $do$
                '''
                cur.execute(query, data)

            return

    @staticmethod
    def lookup_giantbombid(cur, giantbombid):
        """ looks up tables with giantbombid

        :param cur: (object) cursor object for sql
        :param giantbombid: (int) giantbomb id
        :return:
        """

        query = '''SELECT * FROM game_name WHERE giantbombid = %s''' % giantbombid
        cur.execute(query)
        fetch = cur.fetchall()
        print(fetch)

