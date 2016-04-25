from sklearn.pipeline import Pipeline
from sklearn.linear_model import Ridge, LinearRegression
from sklearn.grid_search import GridSearchCV
from sklearn.cross_validation import KFold
from sklearn.preprocessing import OneHotEncoder
from sklearn.externals import joblib
from sklearn.base import TransformerMixin, BaseEstimator
from scipy import fftpack
import numpy
import scipy
import pandas
import re
import math
import db_connect
from matplotlib import pyplot
import plotly
from application import app
from multiprocessing import Pool

CLEANNAME = re.compile(r'[\s:\']')

class FFTTransformer(TransformerMixin, BaseEstimator):
    def __init__(self):
        self.model = LinearRegression()
        self.y_mean = None

    def fit(self, X, y=None):
        X_train = self.make_waves(X)
        y_train = numpy.array(y)
        self.y_mean = y_train.mean()
        self.model.fit(X_train, y_train - self.y_mean)
        return self

    def make_waves(self, X):
        time_scale = numpy.array([(time - X[0]).total_seconds() for time in X]).reshape(-1, 1)

        X_train = [[math.sin(math.pi * 2.0 / (24 * 60 * 60) * delta),
                    math.cos(math.pi * 2.0 / (24 * 60 * 60) * delta),
                    math.sin(math.pi * 2.0 / (12 * 60 * 60) * delta),
                    math.cos(math.pi * 2.0 / (12 * 60 * 60) * delta),
                    math.sin(math.pi * 2.0 / (6 * 60 * 60) * delta),
                    math.cos(math.pi * 2.0 / (6 * 60 * 60) * delta),

                    math.sin(math.pi * 2.0 / (7 * 24 * 60 * 60) * delta),
                    math.cos(math.pi * 2.0 / (7 * 24 * 60 * 60) * delta),
                    math.sin(math.pi * 2.0 / (7.0 / 2 * 24 * 60 * 60) * delta),
                    math.cos(math.pi * 2.0 / (7.0 / 2 * 24 * 60 * 60) * delta),
                    math.sin(math.pi * 2.0 / (7.0 / 3 * 24 * 60 * 60) * delta),
                    math.cos(math.pi * 2.0 / (7.0 / 3 * 24 * 60 * 60) * delta),

                    math.sin(math.pi * 2.0 / (1380500.0) * delta),
                    math.cos(math.pi * 2.0 / (1380500.0) * delta),
                    math.sin(math.pi * 2.0 / (1380500.0 / 2) * delta),
                    math.cos(math.pi * 2.0 / (1380500.0 / 2) * delta),
                    math.sin(math.pi * 2.0 / (1380500.0 / 3) * delta),
                    math.cos(math.pi * 2.0 / (1380500.0 / 3) * delta),

                    ] for delta in time_scale]
        return X_train

    def predict(self, X):
        X_test = self.make_waves(X)
        return self.model.predict(X_test) + self.y_mean

    def transform(self, X, y=None):
        return self.predict(X)


class TestTransformer(TransformerMixin, BaseEstimator):
    def __init__(self):
        self.model = LinearRegression()
        self.encoder = OneHotEncoder()
        self.y_mean = None

    def fit(self, X, y=None):
        X_train, y_train = self.shape_data(X)
        self.model.fit(X_train, y_train)
        return self

    def shape_data(self, X):
        games, channels, genres, ratings, platforms, times, viewers = zip(*X)

        games = self.encoder.fit_transform(numpy.array(games).reshape(-1, 1))
        channels = scipy.sparse.lil_matrix(numpy.array(channels).reshape(-1, 1)).tocsr()

        genres = self.encoder.fit_transform(self.fill_rows(ratings))
        ratings = self.encoder.fit_transform(self.fill_rows(ratings))
        platforms = self.encoder.fit_transform(self.fill_rows(platforms))
        times = scipy.sparse.lil_matrix(self.make_waves(times)).tocsr()

        X_train = scipy.sparse.hstack([games, genres, channels, ratings, platforms, times])

        y_train = numpy.array(viewers)
        self.y_mean = y_train.mean()
        y_train = y_train - self.y_mean
        return X_train, y_train

    def fill_rows(self, X):
        length = len(sorted(X, key=lambda x: 0 if x is None else len(x), reverse=True)[0])

        return numpy.array([numpy.array(([0] if item is None else item) +
                                        [0] * (length - (1 if item is None else len(item))))
                            for item in X])

    def make_waves(self, X):
        time_scale = numpy.array([(time - X[0]).total_seconds() for time in X]).reshape(-1, 1)
        X_train = numpy.array([numpy.array([math.sin(math.pi * 2.0 / (24 * 60 * 60) * delta),
                    math.cos(math.pi * 2.0 / (24 * 60 * 60) * delta),
                    math.sin(math.pi * 2.0 / (12 * 60 * 60) * delta),
                    math.cos(math.pi * 2.0 / (12 * 60 * 60) * delta),
                    math.sin(math.pi * 2.0 / (6 * 60 * 60) * delta),
                    math.cos(math.pi * 2.0 / (6 * 60 * 60) * delta),

                    math.sin(math.pi * 2.0 / (7 * 24 * 60 * 60) * delta),
                    math.cos(math.pi * 2.0 / (7 * 24 * 60 * 60) * delta),
                    math.sin(math.pi * 2.0 / (7.0 / 2 * 24 * 60 * 60) * delta),
                    math.cos(math.pi * 2.0 / (7.0 / 2 * 24 * 60 * 60) * delta),
                    math.sin(math.pi * 2.0 / (7.0 / 3 * 24 * 60 * 60) * delta),
                    math.cos(math.pi * 2.0 / (7.0 / 3 * 24 * 60 * 60) * delta),

                    math.sin(math.pi * 2.0 / (1380500.0) * delta),
                    math.cos(math.pi * 2.0 / (1380500.0) * delta),
                    math.sin(math.pi * 2.0 / (1380500.0 / 2) * delta),
                    math.cos(math.pi * 2.0 / (1380500.0 / 2) * delta),
                    math.sin(math.pi * 2.0 / (1380500.0 / 3) * delta),
                    math.cos(math.pi * 2.0 / (1380500.0 / 3) * delta),

                    ]) for delta in time_scale])

        return X_train

    def predict(self, X):
        X_test, y_test = self.shape_data(X)

        return self.model.predict(X_test) + self.y_mean

    def transform(self, X, y=None):
        return self.predict(X)


def get_game_list(num=10):
    conn = db_connect.connect()
    cur = conn.cursor()
    row = {'num': num}

    query = ''' SELECT name FROM game_name
                  ORDER BY viewer_total DESC
                  LIMIT %(num)s
                '''

    cur.execute(query, row)
    gamelist = cur.fetchall()
    conn.close()

    return [item[0] for item in gamelist]


def master_data():
    conn = db_connect.connect()
    cur = conn.cursor()

    query = '''SELECT snapshot.giantbombid, snapshot.channels, genres, ratings, platforms, snapshot.stamp, snapshot.viewers FROM snapshot

                   LEFT JOIN (
                      SELECT array_agg(genrebomb.genreid) as genres, giantbombid
                        FROM genrebomb
                        GROUP BY giantbombid
                   ) A
                   ON A.giantbombid = snapshot.giantbombid

                   LEFT JOIN (
                      SELECT array_agg(ratingbomb.ratingid) as ratings, giantbombid
                        FROM ratingbomb
                        GROUP BY giantbombid
                   ) B
                   ON B.giantbombid = snapshot.giantbombid

                   LEFT JOIN (
                      SELECT array_agg(DISTINCT (platformgroup.groupid)) as platforms, giantbombid
                        FROM platformbomb
                        INNER JOIN platformgroup
                          ON platformgroup.platformid = platformbomb.platformid
                        GROUP BY giantbombid
                   ) C
                   ON C.giantbombid = snapshot.giantbombid

                   WHERE stamp >= '2016-04-07'
                   ORDER BY stamp ASC
    '''

    cur.execute(query)
    fetch = cur.fetchall()
    conn.close()
    name = 'test'
    games, channels, genres, ratings, platforms, test_times, viewers = zip(*fetch)

    starscream = TestTransformer()
    starscream.fit(fetch)

    machine = starscream.predict(fetch)

    joblib.dump(starscream, './models/' + CLEANNAME.sub('', name) + '.dill')

    print machine

    return test_times, viewers, machine, name


def get_time_data(name):
    conn = db_connect.connect()
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

    times, viewers = zip(*fetch)

    starscream = FFTTransformer()
    starscream.fit(times, viewers)
    machine = starscream.predict(times)

    joblib.dump(starscream, './models/' + CLEANNAME.sub('', name) + '.dill')

    return times, viewers, machine, name


def plot_predict(param):
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


def plot_fft():
    name = 'Dota 2'
    conn = db_connect.connect()
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

    times, viewers = zip(*fetch)
    time_scale = numpy.array([(time-times[0]).total_seconds() for time in times])

    viewers = numpy.array(viewers)
    y_fft = abs(fftpack.fft(viewers - viewers.mean()))

    print sorted(zip(y_fft, time_scale), reverse=True)

    data = [
        dict(
            x=time_scale[:len(time_scale)/2],
            y=y_fft[:len(time_scale)/2]
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


if __name__ == '__main__':
    plotly.tools.set_credentials_file(username=app.config['PLOTLY_NAME'], api_key=app.config['PLOTLY_API'])
    plotly.tools.set_credentials_file(username='sherington', api_key='s5qlad4ga3')

    gamelist = get_game_list(20)

    pool = Pool()
    #fetch = pool.map(get_time_data, gamelist)
    #map(plot_predict, fetch)
    fetch = master_data()
    plot_predict(fetch)
    # plot_fft()
