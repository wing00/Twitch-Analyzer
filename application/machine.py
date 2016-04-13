from sklearn.pipeline import Pipeline
from sklearn.linear_model import Ridge
from sklearn.grid_search import GridSearchCV
from sklearn.cross_validation import KFold
from scipy import fftpack
import numpy
import datetime
import db_connect

conn = db_connect.connect()
cur = conn.cursor()


query = '''SELECT snapshot.*, rating.rating, platform.platform, franchise.franchise, publisher.publisher, genre.genre, theme.theme FROM snapshot

              LEFT JOIN ratingbomb ON snapshot.giantbombid = ratingbomb.giantbombid
              LEFT JOIN rating ON rating.ratingid = ratingbomb.ratingid

              LEFT JOIN platformbomb ON snapshot.giantbombid = platformbomb.giantbombid
              LEFT JOIN platform ON platform.platformid = platformbomb.platformid

              LEFT JOIN franchisebomb ON snapshot.giantbombid = franchisebomb.giantbombid
              LEFT JOIN franchise ON franchise.franchiseid = franchisebomb.franchiseid

              LEFT JOIN publisherbomb ON snapshot.giantbombid = publisherbomb.giantbombid
              LEFT JOIN publisher ON publisher.publisherid = publisherbomb.publisherid

              LEFT JOIN genrebomb ON snapshot.giantbombid = genrebomb.giantbombid
              LEFT JOIN genre ON genre.genreid = genrebomb.genreid

              LEFT JOIN themebomb ON snapshot.giantbombid = themebomb.giantbombid
              LEFT JOIN theme ON theme.themeid = themebomb.themeid

          LIMIT 1
'''

query = '''SELECT stamp, viewers FROM snapshot
'''
cur.execute(query)
fetch = cur.fetchall()
times, viewers = zip(*fetch)
conn.close()

X_train = [time.total_seconds() for time in times]
viewers = numpy.array(viewers)
y_train = abs(fftpack.fft(viewers - viewers.mean()))

parameters = dict(alpha=range(0, 11))

cv = GridSearchCV(estimator=Ridge(),
                  param_grid=parameters,
                  cv=2,
                  n_jobs=1,
                  )

cv.fit(X_train, y_train)
print cv.best_params_
X_test

fftpack.ifft(cv.best_estimator_.predict(X_test))

