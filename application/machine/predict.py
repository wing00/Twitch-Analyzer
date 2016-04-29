from application import app
from application.db import server
from train import CLEANNAME
from multiprocessing import Pool
from scipy import fftpack
import json
import numpy
import dill
import plotly


def predict_time_model(name):
    """function to generate predicted time values

    :param name: name of the game
    :return times, viewers, machine, name: list of times, viewer count, predicted viewers, name of the game

    """
    conn = server.connect()
    cur = conn.cursor()

    query = '''SELECT stamp, viewers FROM snapshot
                   WHERE stamp >= '2016-04-07'
                   AND name = %(name)s
                   ORDER BY stamp ASC
            '''

    cur.execute(query, dict(name=name))
    fetch = cur.fetchall()
    conn.close()

    data = dict(zip(['times', 'viewers'], zip(*fetch)))
    starscream = dill.load(open('application/models/' + CLEANNAME.sub('', name) + '.dill', mode='rb+'))

    machine = starscream.predict(data)
    return data['times'], data['viewers'], machine, name


def predict_model():
    """full model prediction: all features versus actual
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

    starscream = dill.load(open('models/full.dill', mode='rb+'))

    data, viewers = starscream.process(fetch)
    machine = starscream.predict(data, viewers)
    print starscream.get_max(data, fetch, 10)
    print starscream.score(data, viewers)

    return data['times'], data['viewers'], machine, 'full'


def plot_predict(param):
    """plotly graph of actual versus machine fit data

    :param param: tuple
        times: list of times
        viewers: list of viewer counts
        machine: list of predicted viewers
        name: name of the game
    """
    times, viewers, machine, name = param
    data = [
        dict(
            x=times,
            y=viewers,
            name='Real Data',
            mode='markers',
            marker=dict(
                color='#FF7F0E'
            )
        ),
        dict(
            x=times,
            y=machine,
            name='Machine',
            marker=dict(
                color='#1F77B4',
                opacity='0.5'
            )
        ),
    ]
    layout = dict(
        title=name,
        titlefont=dict(
            size=24,
        ),
        showlegend=True
    )
    fig = dict(data=data, layout=layout)
    plotly.plotly.image.save_as(fig, filename='static/img/model/' + CLEANNAME.sub('', name), format='png')


def plot_fft(name='Dota 2'):
    """creates plot for fourier transform

    :param name: name of the game
    """

    conn = server.connect()
    cur = conn.cursor()

    query = '''SELECT stamp, viewers FROM snapshot
               WHERE stamp >= '2016-04-07'
               AND name = %(name)s
               ORDER BY stamp ASC
        '''

    cur.execute(query, dict(name=name))
    fetch = cur.fetchall()
    conn.close()

    times, viewers = zip(*fetch)
    time_scale = numpy.array([(time - times[0]).total_seconds() for time in times])

    viewers = numpy.array(viewers)
    y_fft = abs(fftpack.fft(viewers - viewers.mean()))

    print sorted(zip(y_fft, time_scale), reverse=True)

    data = [
        dict(
            x=time_scale[:len(time_scale) / 2],
            y=y_fft[:len(time_scale) / 2]
        )
    ]
    layout = dict(
        title='FFT',
        titlefont=dict(
            size=48,
        ),
        showlegend=False
    )
    fig = dict(data=data,
               layout=layout
               )

    plotly.plotly.image.save_as(fig, filename='static/img/plots/fft', format='png')


def run_predict_time():
    """opens games json and plots machine predictions for time series
    """
    with open('./static/games.json', mode='r') as f:
        games = json.loads(f.read())

    pool = Pool()
    fetch = pool.map(predict_time_model, [item['game'] for item in games])
    map(plot_predict, fetch)


if __name__ == '__main__':
    pass