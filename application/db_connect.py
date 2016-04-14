import psycopg2
from psycopg2.extensions import AsIs
from application import app
from argparse import ArgumentParser
from multiprocessing import Pool
import re
import requests

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
                    IF EXISTS(SELECT * FROM game_name
                                  WHERE name = %(name)s)
                    THEN
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
    """ Updates stream db
    :param row: row to insert to stream db
    """
    conn = connect()
    cur = conn.cursor()

    query = '''
        INSERT INTO stream VALUES (
            DEFAULT,
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
    """ Check if row exists. If not, insert to team table
    :param row: row to insert
    """
    if not row:
        return

    conn = connect()
    cur = conn.cursor()
    query = '''SELECT channelid, teamid FROM team
               WHERE channelid = %(channel_id)s
                  AND teamid = %(team_id)s'''

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
    """ Inserts row to video table
    :param row: row to insert
    """
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


def games_wrap(offset):
    """wrapper for parallel requests
    :param offset: off set of page
    :return: requests query
    """

    return requests.get('https://api.twitch.tv/kraken/games/top',
                        params=dict(
                            limit=100,
                            offset=offset
                            ),
                        headers=app.config['TWITCH_API']
                        ).json()





def video_wrap(params):
    """wrapper for parallel requests
    :param params: off set of page
    :return: requests query
    """
    url, offset = params
    return requests.get(url,
                        params=dict(
                            limit=100,
                            offset=offset
                            ),
                        headers=app.config['TWITCH_API']
                        ).json()


def get_team_row(url, channel_id):
    """ accesses team api link of channel and updates the table row with the information

    :param url: api link of channel's team
    :param channel_id: channel id number
    :return: number of teams
    """
    team_url = HTTP_RE.sub(r'https://', url)
    teams = requests.get(team_url,
                         params=dict(limit=100),
                         headers=app.config['TWITCH_API']
                         ).json()

    if teams['teams']:
        for team in teams['teams']:
            team_row = dict(
                channel_id=channel_id,
                team_id=team['_id'],
                team_name=team['display_name']
                )
            update_team_table(team_row)
        return len(teams['teams'])

    return 0


def get_stream_row(offset):
    """extracts information from json into a dict

    :param field: json object for channel information
    :return: dict of relevant information from json
    """

    datas = requests.get('https://api.twitch.tv/kraken/streams',
                 params=dict(
                     limit=100,
                     offset=offset
                 ),
                 headers=app.config['TWITCH_API']
                 ).json()['streams']

    for data in datas:
        for field in data:
            channel = field['channel']
            row = dict(
                sponsored=False,
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
                followers=channel['followers']
            )

            team_count = get_team_row(channel['_links']['teams'], channel['_id'])
            video_url = HTTP_RE.sub(r'https://', channel['_links']['videos'])
            videos = requests.get(video_url, headers=app.config['TWITCH_API']).json()
            video_count = videos['_total'] if videos['_total'] else 0

            row.update(dict(
                video_count=video_count,
                team_count=team_count
                )
            )
            update_stream_table(row)


def get_featured_row(field):
    """extracts information from json into a dict for featured streams
       extra level of information needs processing

    :param field: json object for channel information
    :return: dict of relevant information from json
        """
    stream = field['stream']
    channel = stream['channel']

    row = dict(
        sponsored=field['sponsored'],
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
        followers=channel['followers']
        )

    team_count = get_team_row(channel['_links']['teams'], channel['_id'])

    video_url = HTTP_RE.sub(r'https://', channel['_links']['videos'])
    videos = requests.get(video_url,
                          headers=app.config['TWITCH_API']
                          ).json()
    video_count = videos['_total'] if videos['_total'] else 0

    row.update(dict(
        video_count=video_count,
        team_count=team_count
        )
    )
    return row


def get_video_row(params):
    """extracts information from json into a dict

    :param field: json object for channel information
    :return: dict of relevant information from json
    """
    video, channel_id = params
    video_row = dict(
        channel_id=channel_id,
        video_id=video['_id'],
        video_type=video['broadcast_type'],
        video_title=video['title'],
        video_game=video['game'],
        video_desc=video['description'],
        video_status=video['status'],
        video_views=video['views'],
        video_url=video['url'],
        video_res=video['resolutions'],
        video_length=video['length']
        )
    return video_row

class Twitch:
    """twitch API"""

    def __init__(self):
        self.headers = app.config['TWITCH_API']

    @staticmethod
    def set_fields():
        """ api call to twitch to get data

        :rtype: object
        :return: (list) list of dicts of json
        """

        data = requests.get('https://api.twitch.tv/kraken/games/top',
                            headers=app.config['TWITCH_API']
                            ).json()

        total = data['_total']
        pool = Pool()
        datas = pool.map(games_wrap, range(0, total + 100, 100))

        count = 0
        for data in datas:
            for field in data['top']:
                row = dict(
                    name=field['game']['name'],
                    giantbombid=field['game']['giantbomb_id'],
                    viewers=field['viewers'],
                    channels=field['channels'],
                    rank=count + 1
                )
                count += 1
                update_table(row)

    @classmethod
    def run_fields(cls):
        """Class Method to run fields
        """
        test = cls()
        test.set_fields()

    def get_videos(self, channel):
        video_url = HTTP_RE.sub(r'https://', channel['_links']['videos'])
        videos = requests.get(video_url,
                              headers=app.config['TWITCH_API']
                              ).json()

        video_count = videos['_total'] if videos['_total'] else 0

        if video_count:
            pool = Pool()
            rows = pool.map(video_wrap, [(video_url, i) for i in xrange(0, video_count + 100, 100)])
            rows = pool.map(get_video_row, [(row['videos'], channel['_id']) for row in rows])
            pool.map(update_video_table, rows)

        return video_count

    @staticmethod
    def set_streams():
        """ Gets the stream data from twitch

        """
        data = requests.get('https://api.twitch.tv/kraken/streams',
                            headers=app.config['TWITCH_API']
                            ).json()
        streams_total = data['_total']

        pool = Pool()
        pool.map(get_stream_row, range(0, streams_total + 100, 100))


    @classmethod
    def run_streams(cls):
        """Class Method to run streams

        """
        test = cls()
        test.set_streams()

    @staticmethod
    def set_featured():
        """getting featured data and updating database


        """
        data = requests.get('https://api.twitch.tv/kraken/streams/featured',
                            params=dict(limit=100),
                            headers=app.config['TWITCH_API']
                            ).json()

        pool = Pool()
        rows = pool.map(get_featured_row, [field for field in data['featured']])
        pool.map(update_stream_table, rows)

    @classmethod
    def run_featured(cls):
        """Class Method to run fields

        """
        test = cls()
        test.set_featured()


class Giantbomb:
    """giantbomb API"""

    def __init__(self):
        self.token = app.config['GIANTBOMB_API']  # api token
        self.headers = app.config['GIANTBOMB_NAME']
        self.tablenames = [('original_game_rating', 'rating'),
                           ('platforms', 'platform'),
                           ('franchises', 'franchise'),
                           ('publishers', 'publisher'),
                           ('genres', 'genre'),
                           ('themes', 'theme')
                           ]

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
            data = requests.get('http://giantbomb.com/api/search/',
                                params=dict(
                                    format='json',
                                    resources='game',
                                    api_key=self.token,
                                    query=name
                                ),
                                headers=self.headers
                                ).json()

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

        fields = requests.get('http://giantbomb.com/api/search/',
                              params=dict(
                                  format='json',
                                  resources='game',
                                  api_key=self.token,
                                  query=name
                              ),
                              headers=self.headers
                              ).json()

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

            query = '''SELECT giantbombid FROM giantbomb
                          ORDER BY giantbombid DESC'''
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
            print(u'added ' + name + u' to giantbomb table')

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

        query = '''INSERT INTO giantbomb VALUES (
                      %(giantbombid)s,
                      %(name)s
                      ) '''
        cur.execute(query, dict(giantbombid=giantbombid, name=name))

        conn.commit()
        conn.close()

    def add_db(self, api, name):
        """ db add with api call

        :param api: (str) api url
        :param name: (str) name of game
        :return: None
        """

        fields = requests.get(api,
                              params=dict(
                                  format='json',
                                  api_key=self.token
                              ),
                              headers=self.headers
                              ).json()
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
            data = dict(
                giantbombid=AsIs(giantbombid),
                tablename=AsIs(table_name[1]),
                tablebombname=AsIs(table_name[1] + 'bomb')
            )

            for item in row[table_name[0]]:
                data['name'] = item['name']
                data['id'] = item['id']

                # adding to resourcebomb table
                query = '''
                  INSERT INTO %(tablebombname)s VALUES(
                                                  DEFAULT,
                                                  %(id)s,
                                                  %(giantbombid)s
                                                  )
                '''
                cur.execute(query, data)

                # adding to resource table, checking for uniqueness
                query = '''
                  DO
                  $do$
                  BEGIN
                      IF NOT EXISTS(SELECT * FROM %(tablename)s
                                        WHERE %(tablename)s = %(name)s
                                        )
                      THEN
                         INSERT INTO %(tablename)s VALUES(
                                                      %(id)s,
                                                      %(name)s
                                                      );
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

        query = '''SELECT * FROM game_name
                              WHERE giantbombid = %s''' % giantbombid
        cur.execute(query)
        fetch = cur.fetchall()
        print(fetch)


def parse_options():
    """ Flags to control which functions to run
    """
    parser = ArgumentParser()

    parser.add_argument('-t', '--stream', default=False, action='store_true')
    parser.add_argument('-f', '--featured', default=False, action='store_true')
    parser.add_argument('-g', '--games', default=False, action='store_true')

    return parser.parse_args()

if __name__ == '__main__':
    options = parse_options()

    if options.games:
        Twitch.run_fields()
    if options.stream:
        Twitch.run_streams()
    if options.featured:
        Twitch.run_featured()

