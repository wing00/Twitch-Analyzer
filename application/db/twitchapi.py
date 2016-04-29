from application import app
from multiprocessing import Pool
from server import games_wrap, video_wrap, HTTP_RE
import update
import get
import requests


class Twitch:
    """twitch API"""

    def __init__(self):
        self.headers = app.config['TWITCH_API']

    @staticmethod
    def get_live():
        data = requests.get('https://api.twitch.tv/kraken/games/top',
                            params=dict(limit=100),
                            headers=app.config['TWITCH_API']
                            ).json()
        test = []

        for field in data['top']:
            row = dict(
                name=field['game']['name'],
                giantbombid=field['game']['giantbomb_id'],
                viewers=field['viewers'],
                channels=field['channels'],
            )
            test.append(row)
        return test

    @classmethod
    def run_live(cls):
        live = cls()
        return live.get_live()

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
                update.table(row)

    @classmethod
    def run_fields(cls):
        """Class Method to run fields
        """
        test = cls()
        test.set_fields()

    @staticmethod
    def get_videos(channel):
        """getting videos for channel and updates video table

        :param channel: channel json object
        :return video_count: number of videos found
        """
        video_url = HTTP_RE.sub(r'https://', channel['_links']['videos'])
        videos = requests.get(video_url,
                              headers=app.config['TWITCH_API']
                              ).json()

        video_count = videos['_total'] if videos['_total'] else 0

        if video_count:
            pool = Pool()
            rows = pool.map(video_wrap, [(video_url, i) for i in xrange(0, video_count + 100, 100)])
            rows = pool.map(get.video_row, [(row['videos'], channel['_id']) for row in rows])
            pool.map(update.video_table, rows)

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
        pool.map(get.stream_row, range(0, streams_total + 100, 100))

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
        rows = pool.map(get.featured_row, [field for field in data['featured']])
        pool.map(update.stream_table, rows)

    @classmethod
    def run_featured(cls):
        """Class Method to run fields

        """
        test = cls()
        test.set_featured()
