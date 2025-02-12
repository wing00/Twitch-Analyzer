from sklearn.linear_model import LinearRegression
from sklearn.preprocessing import OneHotEncoder, Normalizer
from sklearn.base import TransformerMixin, BaseEstimator
from sklearn.pipeline import FeatureUnion
from sklearn.grid_search import GridSearchCV
from sklearn.cross_validation import KFold
from sklearn.svm import SVR

import numpy


class FFTTransformer(TransformerMixin, BaseEstimator):
    def __init__(self):
        self.model = LinearRegression()
        self.y_mean = None
        self.normalize = Normalizer()

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
        X_test = self.model.predict(X_test) + self.y_mean
        return X_test.reshape(-1, 1)

    def transform(self, X, y=None):
        X_test = self.predict(X)
        X_test = self.normalize.fit_transform(X_test)
        return X_test.reshape(-1, 1)


class FillTransformer(TransformerMixin, BaseEstimator):
    def __init__(self, name):
        self.encoder = OneHotEncoder(handle_unknown='ignore')
        self.name = name
        self.length = None

    def fit(self, X, y=None):
        X = X[self.name]
        self.length = len(sorted(X, key=lambda x: 0 if x is None else len(x), reverse=True)[0])

        X = numpy.array([numpy.array(([0] if item is None else item) +
                                     [0] * (self.length - (1 if item is None else len(item))))
                         for item in X])

        self.encoder.fit_transform(X)
        return self

    def predict(self, X):
        X = X[self.name]
        X = numpy.array([numpy.array(([0] if item is None else item) +
                                     [0] * (self.length - (1 if item is None else len(item))))
                         for item in X])

        X_test = self.encoder.transform(X)
        return X_test

    def transform(self, X, y=None):
        return self.predict(X)


class EncodeTransformer(TransformerMixin, BaseEstimator):
    def __init__(self, name):
        self.name = name
        self.encoder = OneHotEncoder(handle_unknown='ignore')

    def fit(self, X, y=None):
        X = X[self.name]
        X = [[item if item else 0] for item in X]
        self.encoder.fit(X)

        return self

    def predict(self, X):
        X = X[self.name]
        X = [[item if item else 0] for item in X]
        X_test = self.encoder.transform(X)
        return X_test

    def transform(self, X, y=None):
        return self.predict(X)


class LineTransformer(TransformerMixin, BaseEstimator):
    def __init__(self, *args):
        self.args = args
        self.normalize = Normalizer()

    def fit(self, X, y=None):
        return self

    def predict(self, X):
        X_test = [numpy.array([numpy.array([1 if item else 0], dtype='float64') for item in X[arg]]) for arg in
                  self.args]
        X_test = numpy.concatenate(X_test, axis=1)
        X_test = self.normalize.transform(X_test)
        return X_test

    def transform(self, X, y=None):
        return self.predict(X)


class BinaryTransformer(TransformerMixin, BaseEstimator):
    def __init__(self, *args):
        self.args = args

    def fit(self, X, y=None):
        return self

    def predict(self, X):
        X_test = [numpy.array([numpy.array([1 if item else 0]) for item in X[arg]]) for arg in self.args]
        X_test = numpy.concatenate(X_test, axis=1)
        return X_test

    def transform(self, X, y=None):
        return self.predict(X)


class RollingKFold(KFold):
    def __iter__(self):
        ind = numpy.arange(self.n)
        n_folds = self.n_folds
        fold_sizes = (self.n // n_folds) * numpy.ones(n_folds, dtype=numpy.int)
        fold_sizes[:self.n % n_folds] += 1

        for index in xrange(1, len(fold_sizes)):
            i = sum(fold_sizes[:index])
            train_index = ind[:i]
            test_index = ind[i:]
            yield train_index, test_index


class SVRTransformer(TransformerMixin, BaseEstimator):
    def __init__(self):
        self.master = FeatureUnion([
            ('games', EncodeTransformer('games')),
            ('channelid', EncodeTransformer('channelid')),
            ('language', EncodeTransformer('language')),

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

            ('times', FFTTransformer())
        ],
            n_jobs=-1)

        self.model = None

    def process(self, X):
        data = dict(zip([
            'channelid',
            'language',
            'scheduled',
            'featured',
            'mature',
            'partner',
            'sponsored',
            'games',
            'genres',
            'ratings',
            'platforms',
            'followers',
            'videos',
            'teams',
            'times',
            'viewers'
            ],
            zip(*X)
        ))
        return data, data['viewers']

    def fit(self, X, y=None):
        X_train = self.master.fit_transform(X, y)
        self.model = GridSearchCV(
            estimator=SVR(),
            cv=RollingKFold(X_train.shape[0]),
            param_grid=dict(
                C=numpy.logspace(0.1, 10, base=2),
                kernel=['rbf', 'linear', 'poly'],
            ),
            n_jobs=-1,
        )
        self.model.fit(X_train, y)
        return self

    def predict(self, X, y=None):
        X_test = self.master.transform(X)
        return self.model.predict(X_test)

    def transform(self, X, y=None):
        return [[item] for item in self.predict(X)]

    def best_param(self):
        return self.model.best_params_

    def get_max(self, X_test, X, num=1):
        return sorted(zip(self.predict(X_test), X), key=lambda x: -x[0])[:num]

    def score(self, X, y):
        return self.predict(X) - numpy.array(y).T

