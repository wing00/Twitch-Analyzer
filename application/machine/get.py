from application.db import server
import dill
import datetime
import json
import numpy


def game_list(num=10):
    """Polls database for top num of games

    :param num: number of games to fetch
    :return: list of game names
    """
    conn = server.connect()
    cur = conn.cursor()

    query = ''' SELECT name FROM game_name
                  ORDER BY viewer_total DESC
                  LIMIT %(num)s
                '''

    cur.execute(query, dict(num=num))
    gamelist = cur.fetchall()
    conn.close()

    return [item[0] for item in gamelist]


def range():
    """calculates all possible combinations of parameters and writes to json file
    """
    conn = server.connect()
    cur = conn.cursor()

    query = '''
            SELECT array_agg(DISTINCT stream.channelid), array_agg(DISTINCT language.sk_language), array_agg(DISTINCT stream.scheduled),
                   array_agg(DISTINCT stream.featured), array_agg(DISTINCT stream.mature), array_agg(DISTINCT stream.partner), array_agg(DISTINCT stream.sponsored),
                   array_agg(DISTINCT game_name.giantbombid), array_agg(DISTINCT genres), array_agg(DISTINCT ratings), array_agg(DISTINCT platforms), MAX(stream.followers), MAX(stream.videos), MAX(stream.teams), 1, MAX(stream.viewers) FROM stream

                               LEFT JOIN game_name
                               ON game_name.name = stream.game

                               LEFT JOIN (
                                  SELECT genrebomb.genreid AS genres, giantbombid
                                    FROM genrebomb

                               ) A
                               ON A.giantbombid = game_name.giantbombid

                               LEFT JOIN (
                                  SELECT ratingbomb.ratingid AS ratings, giantbombid
                                    FROM ratingbomb

                               ) B
                               ON B.giantbombid = game_name.giantbombid

                               LEFT JOIN (
                                  SELECT platformgroup.groupid AS platforms, giantbombid
                                    FROM platformbomb
                                    INNER JOIN platformgroup
                                      ON platformgroup.platformid = platformbomb.platformid

                               ) C
                               ON C.giantbombid = game_name.giantbombid

                               LEFT JOIN language
                               ON language.iso6391code = stream.language

                               WHERE stamp >= '2016-04-07'


                '''

    cur.execute(query)
    fetch = cur.fetchall()

    conn.close()

    starscream = dill.load(open('application/models/full.dill', mode='rb+'))
    data, viewers = starscream.process(fetch)
    data['followers'] = [range(data['followers'][0] + 1)]
    data['videos'] = [range(data['videos'][0] + 1)]
    data['teams'] = [range(data['teams'][0] + 1)]
    data['times'] = [[datetime.datetime.date()]]
    data['viewers'] = [[data['viewers'][0]]]

    with open('application/models/range.json', mode='wb+') as f:
        f.write(json.dumps(data, indent=4, separators=(',', ': ')))

