from transform import StreamTransformer, FFTTransformer
from application.db import server
from multiprocessing import Pool
import get
import json
import re
import dill

CLEANNAME = re.compile(r'[\s:\']')


def train_full_model():
    """Gets all data and formats them for fitting in the full model
    """
    conn = server.connect()
    cur = conn.cursor()

    query = '''
    SELECT stream.channelid, language.sk_language, stream.scheduled, stream.featured, stream.mature, stream.partner, stream.sponsored, game_name.giantbombid, genres, ratings, platforms, stream.followers, stream.videos, stream.teams, stream.stamp, stream.viewers FROM stream

                       LEFT JOIN game_name
                       ON game_name.name = stream.game

                       LEFT JOIN (
                          SELECT array_agg(genrebomb.genreid) AS genres, giantbombid
                            FROM genrebomb
                            GROUP BY giantbombid
                       ) A
                       ON A.giantbombid = game_name.giantbombid

                       LEFT JOIN (
                          SELECT array_agg(ratingbomb.ratingid) AS ratings, giantbombid
                            FROM ratingbomb
                            GROUP BY giantbombid
                       ) B
                       ON B.giantbombid = game_name.giantbombid

                       LEFT JOIN (
                          SELECT array_agg(DISTINCT (platformgroup.groupid)) AS platforms, giantbombid
                            FROM platformbomb
                            INNER JOIN platformgroup
                              ON platformgroup.platformid = platformbomb.platformid
                            GROUP BY giantbombid
                       ) C
                       ON C.giantbombid = game_name.giantbombid

                       LEFT JOIN language
                       ON language.iso6391code = stream.language

                       WHERE stamp >= '2016-04-07'
                       ORDER BY stamp ASC
        '''
    cur.execute(query)
    fetch = cur.fetchall()
    conn.close()

    print 'data get'

    starscream = StreamTransformer()
    data, viewers = starscream.process(fetch)
    starscream.fit(data, viewers)
    dill.dump(starscream, open('./models/full.dill', mode='wb+'))


def train_time_model(name):
    """Gets the time data for game and trains a FFT model to it

    """
    conn = server.connect()
    cur = conn.cursor()
    row = {'name': name}

    query = '''SELECT stamp, viewers FROM snapshot
               WHERE stamp >= '2016-04-07'
               AND name = %(name)s
               ORDER BY stamp ASC
        '''

    cur.execute(query, row)
    fetch = cur.fetchall()
    conn.close()

    data = dict(zip(['times', 'viewers'], zip(*fetch)))

    starscream = FFTTransformer()
    starscream.fit(data, data['viewers'])
    dill.dump(starscream, open('./models/' + CLEANNAME.sub('', name) + '.dill', mode='wb+'))


def make_json(gamelist):
    data = [dict(game=item) for item in gamelist]
    with open('./static/games.json', mode='wb+') as f:
        f.write(json.dumps(data, indent=4, separators=(',', ': ')))


def run_time_model(num=10):
    gamelist = get.game_list(num)
    make_json(gamelist)
    pool = Pool()
    pool.map(train_time_model, gamelist)
    pool.close()

if __name__ == '__main__':
    train_full_model()
    pass


