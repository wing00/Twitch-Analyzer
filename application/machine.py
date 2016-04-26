from sklearn.pipeline import Pipeline
from sklearn.linear_model import Ridge, LinearRegression
from sklearn.grid_search import GridSearchCV
from sklearn.cross_validation import KFold
from sklearn.preprocessing import OneHotEncoder, LabelEncoder
import dill
from sklearn.base import TransformerMixin, BaseEstimator
from sklearn.pipeline import FeatureUnion
from scipy import fftpack
import numpy
import re

import db_connect
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
        X = X['times']
        time_scale = numpy.array([(time - X[0]).total_seconds() for time in X]).reshape(-1, 1)
        X_train = [
            numpy.concatenate((
                numpy.pi * 2.0 / (24 * 60 * 60) * delta,
                numpy.pi * 2.0 / (12 * 60 * 60) * delta,
                numpy.pi * 2.0 / (6 * 60 * 60) * delta,

                numpy.pi * 2.0 / (7 * 24 * 60 * 60) * delta,
                numpy.pi * 2.0 / (7.0 / 2 * 24 * 60 * 60) * delta,
                numpy.pi * 2.0 / (7.0 / 3 * 24 * 60 * 60) * delta,

                numpy.pi * 2.0 / (1380500.0) * delta,
                numpy.pi * 2.0 / (1380500.0 / 2) * delta,
                numpy.pi * 2.0 / (1380500.0 / 3) * delta), axis=0)
            for delta in time_scale]

        X_train = numpy.concatenate((numpy.sin(X_train), numpy.cos(X_train)), axis=1)
        return X_train

    def predict(self, X):
        X_test = self.make_waves(X)
        return self.model.predict(X_test) + self.y_mean

    def transform(self, X, y=None):
        return [[item if item > 0 else 0] for item in self.predict(X)]


class FillTransformer(TransformerMixin, BaseEstimator):
    def __init__(self, name):
        self.encoder = OneHotEncoder(handle_unknown='ignore')
        self.model = LinearRegression()
        self.name = name
        self.length = None

    def fit(self, X, y=None):
        X = X[self.name]
        self.length = len(sorted(X, key=lambda x: 0 if x is None else len(x), reverse=True)[0])

        X = numpy.array([numpy.array(([0] if item is None else item) +
                                     [0] * (self.length - (1 if item is None else len(item))))
                         for item in X])

        X_train = self.encoder.fit_transform(X)
        self.model.fit(X_train, y)
        return self

    def predict(self, X):
        X = X[self.name]
        X = numpy.array([numpy.array(([0] if item is None else item) +
                                     [0] * (self.length - (1 if item is None else len(item))))
                         for item in X])
        X_test = self.encoder.transform(X)

        return self.model.predict(X_test)

    def transform(self, X, y=None):
        return [[item if item > 0 else 0] for item in self.predict(X)]


class EncodeTransformer(TransformerMixin, BaseEstimator):
    def __init__(self, name):
        self.name = name
        self.encoder = OneHotEncoder(handle_unknown='ignore')
        self.model = LinearRegression()

    def fit(self, X, y=None):
        X = X[self.name]
        X = [[item if item else 0] for item in X]
        X_train = self.encoder.fit_transform(X)
        self.model.fit(X_train, y)
        return self

    def predict(self, X):
        X = X[self.name]
        X = [[item if item else 0] for item in X]
        X_test = self.encoder.transform(X)

        return self.model.predict(X_test)

    def transform(self, X, y=None):
        return [[item if item > 0 else 0] for item in self.predict(X)]


class LabelTransformer(TransformerMixin, BaseEstimator):
    def __init__(self, name):
        self.name = name
        self.encoder = LabelEncoder()
        self.model = LinearRegression()


    def fit(self, X, y=None):
        X = X[self.name]
        X = [[item if item else 0] for item in X]

        X_train = self.encoder.fit_transform(X).reshape(-1, 1)

        self.model.fit(X_train, y)
        return self


    def predict(self, X):
        X = X[self.name]
        X = [[item if item else 0] for item in X]

        X_test = self.encoder.transform(X).reshape(-1, 1)
        return self.model.predict(X_test)

    def transform(self, X, y=None):
        return [[item if item > 0 else 0] for item in self.predict(X)]


class LineTransformer(TransformerMixin, BaseEstimator):
    def __init__(self, name):
        self.name = name
        self.model = LinearRegression()


    def fit(self, X, y=None):
        X = X[self.name]
        X_train = [[item if item else 0] for item in X]

        self.model.fit(X_train, y)
        return self

    def predict(self, X):
        X = X[self.name]
        X_test = [[item if item else 0] for item in X]
        return self.model.predict(X_test)

    def transform(self, X, y=None):
        return [[item if item > 0 else 0] for item in self.predict(X)]


class BinaryTransformer(TransformerMixin, BaseEstimator):
    def __init__(self, name):
        self.name = name
        self.model = LinearRegression()

    def process(self, X):
        X_train = X[self.name]
        X_train = [[1 if item else 0] for item in X_train]

        return X_train

    def fit(self, X, y=None):
        X_train = self.process(X)
        self.model.fit(X_train, y)
        return self

    def predict(self, X):
        X_test = self.process(X)
        return self.model.predict(X_test)

    def transform(self, X, y=None):
        return [[item if item > 0 else 0] for item in self.predict(X)]


class TestTransformer(TransformerMixin, BaseEstimator):
    def __init__(self):
        self.master = Pipeline([('union', FeatureUnion([('games', EncodeTransformer('games')),
                                                        ('channels', LineTransformer('channels')),

                                                        ('genres', FillTransformer('genres')),
                                                        ('ratings', FillTransformer('ratings')),
                                                        ('platforms', FillTransformer('platforms')),
                                                        ('times', FFTTransformer()),
                                                        ])),

                                ('line', LinearRegression())
                                ])
    def process(self, X):
        return dict(zip(['games', 'channels', 'genres', 'ratings', 'platforms', 'times', 'viewers'], zip(*X)))

    def fit(self, X, y=None):
        data = self.process(X)
        self.master.fit(data, data['viewers'])
        return self

    def predict(self, X):
        data = self.process(X)
        return self.master.predict(data)

    def transform(self, X, y=None):
        return self.predict(X)


class StreamTransformer(TransformerMixin, BaseEstimator):
    def __init__(self):
        self.master = Pipeline([
            ('union', FeatureUnion([
                ('games', EncodeTransformer('games')),
                ('channelid', EncodeTransformer('channelid')),

                ('language', LabelTransformer('language')),

                ('scheduled', BinaryTransformer('scheduled')),
                ('featured', BinaryTransformer('featured')),
                ('mature', BinaryTransformer('mature')),
                ('partner', BinaryTransformer('partner')),
                ('sponsored', BinaryTransformer('sponsored')),

                ('followers', LineTransformer('followers')),
                ('videos', LineTransformer('videos')),
                ('teams', LineTransformer('teams')),

                ('genres', FillTransformer('genres')),
                ('ratings', FillTransformer('ratings')),
                ('platforms', FillTransformer('platforms')),

                ('times', FFTTransformer()),
                ])
            ),

            ('line', LinearRegression())
            ])

    def process(self, X):
        return dict(zip(
            ['channelid', 'language', 'scheduled', 'featured', 'mature', 'partner', 'sponsored', 'games', 'genres',
             'ratings', 'platforms', 'followers', 'videos', 'teams', 'times', 'viewers'], zip(*X)))

    def fit(self, X, y=None):
        data = self.process(X)
        self.master.fit(data, data['viewers'])
        return self

    def get_max(self, X, num=1):
        return sorted(zip(self.predict(X), X), key=lambda x: -x[0])[:num]

    def score(self, X, y):
        return self.predict(X) - numpy.array(y).T

    def predict(self, X):
        data = self.process(X)
        return numpy.array([numpy.array([item if item > 0 else 0]) for item in self.master.predict(data)])

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


def streams_data():
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
                       ORDER BY stamp ASC
        '''

    cur.execute(query)
    fetch = cur.fetchall()

    data = dict(zip(
        ['channelid', 'language', 'scheduled', 'featured', 'mature', 'partner', 'sponsored', 'games', 'genres',
         'ratings', 'platforms', 'followers', 'videos', 'teams', 'times', 'viewers'], zip(*fetch)))

    starscream = StreamTransformer()
    starscream.fit(fetch)

    dill.dump(starscream, open('./models/full.dill', mode='wb+'))

    machine = starscream.predict(fetch)
    print starscream.get_max(fetch, 10)
    print starscream.score(fetch, data['viewers'])

    return data['times'], data['viewers'], machine, 'full'


def master_data():
    conn = db_connect.connect()
    cur = conn.cursor()


    query = '''SELECT snapshot.giantbombid, snapshot.channels, genres, ratings, platforms, snapshot.stamp, snapshot.viewers FROM snapshot
                   LEFT JOIN (
                      SELECT array_agg(genrebomb.genreid) AS genres, giantbombid
                        FROM genrebomb
                        GROUP BY giantbombid
                   ) A
                   ON A.giantbombid = snapshot.giantbombid

                   LEFT JOIN (
                      SELECT array_agg(ratingbomb.ratingid) AS ratings, giantbombid
                        FROM ratingbomb
                        GROUP BY giantbombid
                   ) B
                   ON B.giantbombid = snapshot.giantbombid

                   LEFT JOIN (
                      SELECT array_agg(DISTINCT (platformgroup.groupid)) AS platforms, giantbombid
                        FROM platformbomb
                        INNER JOIN platformgroup
                          ON platformgroup.platformid = platformbomb.platformid
                        GROUP BY giantbombid
                   ) C
                   ON C.giantbombid = snapshot.giantbombid

                   WHERE stamp >= '2016-04-07'
                   ORDER BY stamp ASC
                   LIMIT 10
    '''

    cur.execute(query)
    fetch = cur.fetchall()
    conn.close()

    games, channels, genres, ratings, platforms, test_times, viewers = zip(*fetch)

    starscream = TestTransformer()
    starscream.fit(fetch)

    machine = starscream.predict(fetch)
    dill.dump(starscream, open('./models/test.dill', mode='wb+'))

    print machine
    return test_times, viewers, machine, 'test'


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

    data = dict(zip(['times', 'viewers'], zip(*fetch)))

    starscream = FFTTransformer()
    starscream.fit(data, data['viewers'])
    machine = starscream.predict(data)

    dill.dump(starscream, open('./models/' + CLEANNAME.sub('', name) + '.dill', mode='wb+'))

    return data['times'], data['viewers'], machine, name


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


if __name__ == '__main__':
    plotly.tools.set_credentials_file(username=app.config['PLOTLY_NAME'], api_key=app.config['PLOTLY_API'])
    plotly.tools.set_credentials_file(username='sherington', api_key='s5qlad4ga3')

    gamelist = get_game_list(20)

    pool = Pool()
    fetch = pool.map(get_time_data, gamelist)
    map(plot_predict, fetch)

    # fetch = streams_data()
    # plot_predict(fetch)
    # plot_fft()
