from application import app
import psycopg2
import requests
import re

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
