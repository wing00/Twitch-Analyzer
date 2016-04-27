import datetime
import time
import plotly
import db_connect
import re
import dill
import numpy
from application import app

CLEANNAME = re.compile(r'[\s:\']')


def set_plotly_creds():
    """ plot.ly credentials for streaming API

    :return: None
    """
    plotly.tools.set_credentials_file(username=app.config['PLOTLY_NAME'], api_key=app.config['PLOTLY_API'])
    plotly.tools.set_credentials_file(app.config['STREAM_KEY'])
    return app.config['STREAM_KEY']


def create_stream_model(stream_ids, name):
    """ creates plot and a list of stream objects with identifiers

    :param online: (boolean) flag for posting to plot.ly
    :param stream_ids: (list) list of stream id api keys
    :return: (list) list of dict of stream objects
    """

    starscream = dill.load(open('./application/models/' + CLEANNAME.sub('', name) + '.dill', mode='rb'))

    conn = db_connect.connect()
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
            y=abs((predicted - actual)/actual)*100,
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
        twitch = db_connect.Twitch.run_live()
        actual = [item['viewers'] for item in twitch if name == item['name']][0]
        predicted = starscream.predict([datetime.datetime.now()])[0]

        x = datetime.datetime.now()
        y = predicted
        stream[0]['stream_obj'].write(dict(x=x, y=y))
        y = actual
        stream[1]['stream_obj'].write(dict(x=x, y=y))
        y = abs((predicted - actual)/actual) * 100
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
    twitch = db_connect.Twitch.run_live()

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

    :param stream_ids: (list) list of stream id api keys
    :param stream: (list) list of dict of stream objects
    :return: None
    """

    map(lambda x: stream[x]['stream_obj'].open(), range(len(stream)))

    print('streams open')
    # feeding data to plot
    counter = 0

    while True:
        twitch = db_connect.Twitch.run_live()

        for data in stream:

            x = datetime.datetime.now()
            y = [item['viewers'] for item in twitch if data['name'] == item['name']][0]
            data['stream_obj'].write(dict(x=x, y=y))

        time.sleep(60)
        counter += 1
        if counter % 10 == 0:
            print(counter)


if __name__ == '__main__':
    # stream = create_stream_plot(stream_ids, online=True)
    # stream_data(stream)
    name = 'League of Legends'
    stream_ids = set_plotly_creds()
    div, stream = create_stream_model(stream_ids, name)

