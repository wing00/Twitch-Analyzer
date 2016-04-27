import dill
import numpy
import db_connect
import datetime


def look():
    conn = db_connect.connect()
    cur = conn.cursor()

    query = '''
    SELECT stream.channelid, stream.language, stream.scheduled, stream.featured, stream.mature, stream.partner, stream.sponsored, game_name.giantbombid, genres, ratings, platforms, stream.followers, stream.videos, stream.teams, stream.stamp, stream.viewers FROM stream

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

                       WHERE stamp >= '2016-04-07'
                       AND language = 'ru'

                       ORDER BY stamp ASC
                       LIMIT 1
        '''

    cur.execute(query)
    fetch = cur.fetchall()

    data = dict(zip(
        ['channelid', 'language', 'scheduled', 'featured', 'mature', 'partner', 'sponsored', 'games', 'genres',
         'ratings', 'platforms', 'followers', 'videos', 'teams', 'times', 'viewers'], zip(*fetch)))

    print fetch
    conn.close()

def test():
    channelids = range(1, 122808824, 10000)
    languages = ['', 'ar', 'bg', 'cs', 'da', 'de', 'en', 'es', 'fi', 'fr', 'hu', 'it', 'ja', 'ko', 'nl', 'no', 'other', 'pl', 'pt',
      'ru', 'sk', 'sv', 'th', 'tr', 'vi', 'zh', None]
    scheduleds = [True, False]
    featureds = [True, False]
    matures = [True, False]
    partners = [True, False]
    sponsoreds = [True, False]
    games = range(1, 53504)
    genres = range(1, 51)
    ratings = [1, 9, 16, 23, 26, 29]
    platforms = range(1, 7)
    followers = range(0, 11) + range(11, 101, 10) + range(101, 2208667, 100)
    videos = range(0, 11) + range(11, 101, 10) + range(101, 25828, 100)
    teams = range(0, 14)
    times = [datetime.datetime.now()]

    starscream = dill.load(open('./models/full.dill', mode='rb+'))

    viewers = []
    for channelid in channelids:
        for language in languages:
            for scheduled in scheduleds:
                for featured in featureds:
                    for mature in matures:
                        for partner in partners:
                            for sponsored in sponsoreds:
                                for genre in genres:
                                    for game in games:
                                        for rating in ratings:
                                            for platform in platforms:
                                                for follower in followers:
                                                    for video in videos:
                                                        for team in teams:
                                                            for time in times:

                                                                data = [(channelid, language, scheduled,
                                                                        featured, mature, partner,
                                                                        sponsored, game,
                                                                        [genre], [rating], [platform],
                                                                        follower, video, team, time, 0)]

                                                                viewer = starscream.predict(data)
                                                                viewers.append([viewer, data])

    print sorted(viewers, key=lambda x: -x[0])


test()
