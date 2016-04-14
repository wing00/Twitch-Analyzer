from sklearn.pipeline import Pipeline
from sklearn.linear_model import Ridge, LinearRegression
from sklearn.grid_search import GridSearchCV
from sklearn.cross_validation import KFold
from scipy import fftpack
import numpy
import datetime
import math
import db_connect
from matplotlib import pyplot
import plotly

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
           WHERE name = 'League of Legends'
           AND stamp <= '2016-02-21'
'''
cur.execute(query)
fetch = cur.fetchall()
times, viewers = zip(*fetch)
conn.close()


time_scale = numpy.array([(time-times[0]).total_seconds() for time in times]).reshape(-1, 1)
viewers = numpy.array(viewers)
y_train = abs(fftpack.fft(viewers - viewers.mean())).reshape(-1, 1)



X_train = [[math.sin(2*math.pi*1.0/(60*24*60)*x), math.cos(2*math.pi*1.0/(60*24*60)*x)] for x in time_scale]

model = LinearRegression()

model.fit(X_train, viewers - viewers.mean())

y = model.predict(X_train)

pyplot.plot(times, y + viewers.mean())
pyplot.plot(times, viewers)
machine = y + viewers.mean()

data = [
        plotly.graph_objs.Scatter(
            x=times,
            y=viewers,
            name='Real Data'
            ),
        plotly.graph_objs.Scatter(
            x=times,
            y=machine,
            name='Machine'
            )
        ]

layout = plotly.graph_objs.Layout(
    title='Machine Fit Versus Data',
    titlefont=plotly.graph_objs.Font(
        size=48,
    ),
    showlegend=True
)
fig = dict(data=data, layout=layout)

div = plotly.offline.plot(fig, output_type='div')
with open('templates/plots/machine.html', mode='w+') as f:
    f.write(div)

plotly.plotly.image.save_as(fig, filename='static/img/plots/machine', format='png')


# fftpack.ifft(cv.best_estimator_.predict(X_test))

700000*0.5