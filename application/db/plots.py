from application import app
from application.machine.train import CLEANNAME
from multiprocessing import Pool
import server
import plotly
import time
import numpy
import dill
import datetime


def set_plotly_creds():
    """ plot.ly credentials for streaming API

    :return: None
    """
    plotly.tools.set_credentials_file(username=app.config['PLOTLY_NAME'], api_key=app.config['PLOTLY_API'])
    plotly.tools.set_credentials_file(app.config['STREAM_KEY'])
    return app.config['STREAM_KEY']


def create_plot(table_name, online=False):
    """ generating pie chart for table

    :param table_name: (str) name of table
    :param online: (boolean) flag for plot.ly online plots
    :return: None
    """

    conn = server.connect()
    cur = conn.cursor()

    param = dict(
        name=table_name,
        namebomb=table_name + 'bomb',
        nameid=table_name + 'id'
    )

    query = '''SELECT %(name)s.%(name)s, SUM(game_name.viewer_total) AS viewers
                  FROM %(namebomb)s
                  INNER JOIN game_name
                    ON game_name.giantbombid = %(namebomb)s.giantbombid
                  INNER JOIN %(name)s
                    ON %(namebomb)s.%(nameid)s = %(name)s.%(nameid)s''' % param

    if table_name == 'rating':
        query += '''
            WHERE rating.rating LIKE 'ESRB%'
            GROUP BY rating.rating
            ORDER BY rating.rating ASC;
        '''
    elif table_name =='platform':
        query = '''
            SELECT platformgroup.platformgroup, SUM(game_name.viewer_total) AS viewers
                FROM platformbomb
                INNER JOIN game_name
                  ON game_name.giantbombid = platformbomb.giantbombid
                INNER JOIN platform
                  ON platformbomb.platformid = platform.platformid
                INNER JOIN platformgroup
                  ON platformgroup.platformid = platform.platformid
                GROUP BY platformgroup.platformgroup
                ORDER BY viewers DESC;
        '''

    else:
        query += '''
          GROUP BY %(name)s.%(name)s
          ORDER BY viewers DESC;
    ''' % param

    cur.execute(query)
    rows = cur.fetchall()
    conn.close()

    if table_name == 'genre':
        total = sum(value for name, value in rows)
        rows = [(name, value) for name, value in rows if value > 0.01 * total] + [('Other', sum(value for name, value in rows if value <= 0.01 * total))]

    label, values = zip(*sorted(rows, key=lambda x: -x[1]))

    data = [dict(
            y=values,
            x=label,
            marker=dict(color=['#1F77B4', '#FF7F0E', '#2CA02C', '#D62728', '#103D5D', '#9467BD', '#17BECF', '#E377C2', '#FFB574', '#57A9E2']),
            type='bar',
            )]

    layout = dict(
        title=table_name.title() + ' By Viewers',
        titlefont=dict(
            size=48,
        ),
        showlegend=False
    )

    fig = dict(data=data, layout=layout)

    plotly.plotly.image.save_as(fig, filename='static/img/plots/' + table_name, format='png')

    div = plotly.offline.plot(fig, output_type='div')
    with open('templates/plots/' + table_name + '.html', mode='w+') as f:
        f.write(div)

    if online:
        url = plotly.plotly.plot(fig, filename=table_name)
        print(plotly.tools.get_embed(url))

    print 'created ' + table_name


def all_time(online=False):
    """ getting viewer distribution data and plotting

    :param online: (boolean) flag for plot.ly online plots
    :return: None
    """

    conn = server.connect()
    cur = conn.cursor()
    cur.execute('''SELECT name, viewer_total FROM game_name''')
    rows = cur.fetchall()
    conn.close()

    total = sum(value for name, value in rows)
    new_rows = sorted([(name[:17], value) for name, value in rows if value > 0.005 * total], key=lambda x: x[1])
               #+ [('Other', sum(value for name, value in rows if value <= 0.01 * total))]

    label, values = zip(*new_rows)
    data = [dict(
            y=label,
            x=values,
            type='bar',
            orientation='h',
            marker=dict(
                color='random'
                )
            )]

    layout = dict(
        title='Games By Viewers',
        titlefont=dict(
            size=48,
        ),

        margin=dict(
            l=100,
            pad=4
        ),

        yaxis=dict(
            tickangle=-45,
            tickfont=dict(
                size=8
            ),
        ),
        showlegend=False,
    )

    fig = dict(data=data, layout=layout)

    plotly.plotly.image.save_as(fig, filename='static/img/plots/alltime', format='png')
    div = plotly.offline.plot(fig, output_type='div')

    with open('templates/plots/alltime.html', mode='w+') as f:
        f.write(div)

    if online:
        url = plotly.plotly.plot(fig, filename='All Time')
        print(plotly.tools.get_embed(url))

    print 'created all time'


def time_series(online=False):
    """ getting time series data and plotting

    :param online: (boolean) flag for plot.ly online plots
    :return: None
    """

    conn = server.connect()
    cur = conn.cursor()
    cur.execute('''
                SELECT name, viewers, stamp FROM snapshot
                WHERE stamp >= '2016-04-02'
                ORDER BY name, stamp ASC
                ''')

    rows = cur.fetchall()
    conn.close()

    data = []
    labels = []
    viewers = []
    timestamps = []
    count = 0

    for row in rows:
        if not labels:
            labels = row[0]

        if labels == row[0]:
            viewers.append(row[1])
            timestamps.append(row[2])
        else:
            count += 1

            trace = plotly.graph_objs.Scatter(
                x=timestamps,
                y=viewers,
                name=labels,
            )

            data.append(trace)

            labels = row[0]
            viewers = [row[1]]
            timestamps = [row[2]]

    layout = dict(
        title='Games - Time Series',
        titlefont=dict(
            size=48,
        ),
        showlegend=False
        )
    fig = dict(data=data, layout=layout)

    div = plotly.offline.plot(fig, output_type='div')
    with open('templates/plots/timeseries.html', mode='w+') as f:
        f.write(div)

    plotly.plotly.image.save_as(fig, filename='static/img/plots/timeseries', format='png')
    if online:
        url = plotly.plotly.plot(fig, filename='Time Series')
        print(plotly.tools.get_embed(url))


def create_stream_model(stream_ids, name):
    """ creates plot and a list of stream objects with identifiers

    :param online: (boolean) flag for posting to plot.ly
    :param stream_ids: (list) list of stream id api keys
    :return: (list) list of dict of stream objects
    """

    starscream = dill.load(open('application/models/' + CLEANNAME.sub('', name) + '.dill', mode='rb'))

    conn = server.connect()
    cur = conn.cursor()

    query = '''SELECT stamp, viewers FROM snapshot
                  WHERE name = %(name)s
                  AND stamp >= '2016-04-07'
                  ORDER BY stamp ASC
    '''

    cur.execute(query, dict(name=name))
    fetch = cur.fetchall()
    times, actual = zip(*fetch)
    conn.close()

    predicted = starscream.predict(dict(times=times))

    data = [
        dict(
            x=times,
            y=predicted,
            type='scatter',
            name='Machine',
            stream=dict(token=stream_ids[0])
        ),

        dict(
            x=times,
            y=actual,
            type='scatter',
            mode='markers',
            name=name,
            stream=dict(token=stream_ids[1])
        ),

        dict(
            x=times,
            y=0 if actual == 0 else abs((predicted - actual)/actual)*100,
            type='scatter',
            name='% Error',
            stream=dict(token=stream_ids[2])
        ),
    ]

    stream = [
        dict(
            stream_obj=plotly.plotly.Stream(stream_ids[0]),
            name='Machine',
            stream_id=stream_ids[0]
           ),

        dict(
            stream_obj=plotly.plotly.Stream(stream_ids[1]),
            name=name,
            stream_id=stream_ids[1]
           ),

        dict(
            stream_obj=plotly.plotly.Stream(stream_ids[2]),
            name='% Error',
            stream_id=stream_ids[2]
            )
        ]

    layout = dict(title='Streaming')
    fig = dict(data=data, layout=layout)

    url = plotly.plotly.plot(fig, filename='model', auto_open=False)
    return plotly.tools.get_embed(url), stream


def stream_model_data(stream):
    """ Opens stream for data input and sends data

    :param stream: (list) list of dict of stream objects
    :return: None
    """
    name = stream[1]['name']
    starscream = dill.load(open('./models/' + CLEANNAME.sub('', name) + '.dill', mode='rb+'))

    map(lambda x: stream[x]['stream_obj'].open(), range(3))
    print('streams open')
    # feeding data to plot
    counter = 0

    while True:
        twitch = server.Twitch.run_live()
        actual = [item['viewers'] for item in twitch if name == item['name']][0]
        predicted = starscream.predict([datetime.datetime.now()])[0]

        x = datetime.datetime.now()
        y = predicted
        stream[0]['stream_obj'].write(dict(x=x, y=y))
        y = actual
        stream[1]['stream_obj'].write(dict(x=x, y=y))
        y = 0 if actual == 0 else abs((predicted - actual)/actual)*100
        stream[2]['stream_obj'].write(dict(x=x, y=y))

        time.sleep(0.5)
        counter += 1
        if counter % 10 == 0:
            print(counter)


def create_stream_plot(stream_ids, online):
    """ creates plot and a list of stream objects with identifiers

    :param online: (boolean) flag for posting to plot.ly
    :param stream_ids: (list) list of stream id api keys
    :return: (list) list of dict of stream objects
    """

    data = []
    stream = []
    twitch = server.Twitch.run_live()

    # creating plot
    for index, stream_id in enumerate(stream_ids):
        trace = dict(
            x=datetime.datetime.now(),
            y=twitch[index]['viewers'],
            type='scatter',
            name=twitch[index]['name'],
            stream=dict(token=stream_id)
        )

        data.append(trace)
        stream.append(dict(stream_obj=plotly.plotly.Stream(stream_id),
                           name=twitch[index]['name'],
                           stream_id=stream_id
                           )
                      )

    layout = dict(title='Streaming')
    fig = dict(data=data, layout=layout)

    if online:
        url = plotly.plotly.plot(fig, filename='live')
        with open('./templates/plots/live.html', mode='wb+') as f:
            f.write(plotly.tools.get_embed(url))

    return stream


def stream_data(stream):
    """ Opens stream for data input and sends data

    :param stream: (list) list of dict of stream objects
    :return: None
    """

    map(lambda x: stream[x]['stream_obj'].open(), range(len(stream)))

    print('streams open')
    # feeding data to plot
    counter = 0

    while True:
        twitch = server.Twitch.run_live()

        for data in stream:

            x = datetime.datetime.now()
            y = [item['viewers'] for item in twitch if data['name'] == item['name']][0]
            data['stream_obj'].write(dict(x=x, y=y))

        time.sleep(60)
        counter += 1
        if counter % 10 == 0:
            print(counter)


def run_plot():
    """run function to create all the plots
    """
    set_plotly_creds()
    table_names = ['rating', 'platform', 'genre', 'publisher', 'franchise', 'theme']

    pool = Pool()
    pool.map(create_plot, table_names)
    pool.close()

    all_time()
    time_series()


def run_stream(name):
    """creates stream plot on plotly and returns stream object for live updating

    :return div: iframe for stream
    :return stream: stream object
    """
    set_plotly_creds()
    stream_ids = set_plotly_creds()
    div, stream = create_stream_model(stream_ids, name)

    return div, stream


if __name__ == '__main__':
    pass
