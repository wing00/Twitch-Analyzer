from application import app
from server import connect
from psycopg2.extensions import AsIs
import requests
import re


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
            print(u'added ' + re.sub(r'[^\x00-\x7F]', '', name) + u' to giantbomb table')

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
