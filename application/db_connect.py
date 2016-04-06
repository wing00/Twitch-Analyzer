import psycopg2
from psycopg2.extensions import AsIs
import re
import requests
from application import app

HTTP_RE = re.compile(r'http://')

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


def get_trialid():
    """ fetches highest trial id number

    :return: (int) trial id
    """

    conn = connect()
    cur = conn.cursor()

    cur.execute('''SELECT trialid FROM snapshot ORDER BY trialid DESC LIMIT 1''')
    trialid = cur.fetchone()
    if trialid:
        trialid = trialid[0]
    else:
        trialid = 1

    conn.close()

    return trialid


def update_table(row):
    """ checking and updating db with game info from giantbomb

    :param row: (dict) dict of twitch json object
    :param trialid: (int) trial id number
    :param giantbomb: (object) giantbomb class object
    :return: None
    :rtype: object
    """

    conn = connect()
    cur = conn.cursor()

    trialid = get_trialid()
    Giantbomb.check(row['name'], row['giantbombid'])
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
    conn.commit()
    conn.close()


def update_stream_table(row):
    conn = connect()
    cur = conn.cursor()

    query = '''
        INSERT INTO stream VALUES (
            %(stream_id)s,
            %(channel_id)s,
            %(url)s,
            %(language)s,
            %(scheduled)s,
            %(featured)s,
            %(mature)s,
            %(partner)s,
            %(sponsored)s,
            %(game)s,
            %(viewers)s,
            %(followers)s,
            %(total_views)s,
            %(video_count)s,
            %(team_count)s,
            current_timestamp
          )'''
    cur.execute(query, row)
    conn.commit()
    conn.close()


def update_team_table(row):
    if not row:
        return

    conn = connect()
    cur = conn.cursor()
    query = '''SELECT channelid, teamid FROM team
               WHERE channelid = %(channel_id)s AND teamid = %(team_id)s'''
    cur.execute(query, row)
    fetch = cur.fetchone()

    if fetch is None:
        query = '''
            INSERT INTO team VALUES (
                %(channel_id)s,
                %(team_id)s,
                %(team_name)s,
                current_timestamp
              )'''
        cur.execute(query, row)
        conn.commit()

    conn.close()


def update_video_table(row):
    if not row:
        return

    conn = connect()
    cur = conn.cursor()
    query = '''
            INSERT INTO featured VALUES (
                DEFAULT,
                 %(channel_id)s,
                 %(video_id)s,
                 %(video_title)s,
                 %(video_game)s,
                 %(video_status)s,
                 %(video_type)s,
                 %(video_views)s,
                 %(video_url)s,
                 %(video_res)s,
                 %(video_length)s,
                 %(video_desc)s,
                current_timestamp
              )'''
    cur.execute(query, row)
    cur.commit()
    conn.close()

class Twitch:
    """twitch API"""

    def __init__(self):
        self.token = app.config['TWITCH_API']
        self.fields = None
        self.streams = None
        self.teams = None
        self.videos = None
        self.featured = None
        self.featured_teams = None

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

        self.fields = fields

    @classmethod
    def run_fields(cls):
        test = cls()
        test.set_fields
        return test.fields

    # def get_videos(self):
    #     offset = 0
    #     video_rows = []
    #
    #     if videos_count:
    #         while offset < videos_count:
    #             videos = requests.get(channel['_links']['videos'], params=dict(limit=100, offset=offset),
    #                                   headers=headers).json()
    #             for video in videos['videos']:
    #                 video_row = dict(channel_id=channel['_id'],
    #                                  video_id=video['_id'],
    #                                  video_type=video['broadcast_type'],
    #                                  video_title=video['title'],
    #                                  video_game=video['game'],
    #                                  video_desc=video['description'],
    #                                  video_status=video['status'],
    #                                  video_views=video['views'],
    #                                  video_url=video['url'],
    #                                  video_res=video['resolutions'],
    #                                  video_length=video['length']
    #                                  )
    #
    #                 video_rows.append(video_row)

    @property
    def set_streams(self):
        headers = {'client-id': self.token}
        data = requests.get('https://api.twitch.tv/kraken/streams', headers=headers).json()
        streams_total = data['_total']

        offset = 0

        fields = []
        team_fields = []
        while offset < streams_total:
            print 'getting offset:', offset
            data = requests.get('https://api.twitch.tv/kraken/streams', params=dict(limit=100, offset=offset), headers=headers).json()
            if not data['streams']:
                break

            for index, field in enumerate(data['streams']):
                channel = field['channel']
                row = dict(sponsored=False,
                           scheduled=False,
                           featured=False,

                           game=field['game'],
                           viewers=field['viewers'],
                           stream_id=field['_id'],

                           mature=channel['mature'],
                           language=channel['broadcaster_language'],
                           channel_id=channel['_id'],
                           partner=channel['partner'],
                           url=channel['url'],
                           total_views=channel['views'],
                           followers=channel['followers'],
                           )
                team_url = HTTP_RE.sub(r'https://', channel['_links']['teams'])
                teams = requests.get(team_url, params=dict(limit=100), headers=headers).json()

                team_count = 0
                if teams['teams']:
                    for team in teams['teams']:
                        team_row = dict(channel_id=channel['_id'],
                                        team_id=team['_id'],
                                        team_name=team['display_name'])
                        team_fields.append(team_row)
                    team_count = len(teams['teams'])

                video_url = HTTP_RE.sub(r'https://', channel['_links']['videos'])

                videos = requests.get(video_url, headers=headers).json()
                video_count = videos['_total'] if videos['_total'] else 0

                row.update(dict(video_count=video_count,
                                team_count=team_count))

                fields.append(row)
            offset += 100

        self.streams = fields
        self.teams = team_fields

    @classmethod
    def run_streams(cls):
        test = cls()
        test.set_streams
        return test.streams, test.teams


    @property
    def set_featured(self):

        headers = {'client-id': self.token}
        data = requests.get('https://api.twitch.tv/kraken/streams/featured', params=dict(limit=100), headers=headers).json()

        fields = []
        team_fields = []

        for index, field in enumerate(data['featured']):
            stream = field['stream']
            channel = stream['channel']

            row = dict(sponsored=field['sponsored'],
                       scheduled=field['scheduled'],
                       featured=True,

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

            teams = requests.get(channel['_links']['teams'], params=dict(limit=100), headers=headers).json()
            team_count = 0

            if teams['teams']:
                for team in teams['teams']:
                    team_row = dict(channel_id=channel['_id'],
                                    team_id=team['_id'],
                                    team_name=team['display_name'])
                    team_fields.append(team_row)
                team_count = len(teams['teams'])

            videos = requests.get(channel['_links']['videos'], headers=headers).json()
            video_count = videos['_total'] if videos['_total'] else 0

            row.update(dict(video_count=video_count,
                            team_count=team_count))

            fields.append(row)

        self.featured = fields
        self.featured_teams = team_fields

    @classmethod
    def run_featured(cls):
        test = cls()
        test.set_featured
        return test.featured, test.featured_teams


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
        self.set_ids

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
        param = dict(format='json',
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

    @property
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
        conn.close()

        conn = connect()
        cur = conn.cursor()
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
            else:
                self.add_db_no_api(name, giantbombid)
            print('added ' + name + ' to giantbomb table')

    @classmethod
    def check(cls, name, giantbombid):
        checker = cls()
        checker.check_db(name, giantbombid)

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

if __name__ == '__main__':
    # Twitch.run_fields()
    # Twitch.run_featured()
    data = Twitch.run_streams()
    print len(data)


